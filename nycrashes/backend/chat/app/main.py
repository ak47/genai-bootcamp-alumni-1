from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
from mcp import stdio_client, StdioServerParameters
from pydantic import BaseModel
from strands.agent.conversation_manager import SlidingWindowConversationManager
from strands import Agent
from strands.models import BedrockModel
from strands.session.s3_session_manager import S3SessionManager
import boto3
import json
import logging
import os
import uuid
import uvicorn
from strands.tools.mcp import MCPClient

AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-west-2")
BOTO_SESSION = boto3.Session()
CLUSTER_ARN = os.environ["CLUSTER_ARN"]
DATABASE_NAME = os.environ["DATABASE_NAME"]
MODEL_ID = os.environ.get("MODEL_ID", "global.anthropic.claude-sonnet-4-5-20250929-v1:0")
SECRET_ARN = os.environ["SECRET_ARN"]
STATE_BUCKET = os.environ.get("STATE_BUCKET", "")
logging.getLogger("strands").setLevel(logging.DEBUG)
logging.basicConfig(
    format="%(levelname)s | %(name)s | %(message)s", 
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You are a helpful assistant that answers questions about New York City motor vehicle crashes.
"""

BEDROCK_MODEL = BedrockModel(
    model_id=MODEL_ID,
    # Add Guardrails here if desired
)

current_agent = None

def build_mcp_environment() -> dict[str, str]:
    env = dict(os.environ)
    credentials = BOTO_SESSION.get_credentials()
    if credentials:
        frozen = credentials.get_frozen_credentials()
        env["AWS_ACCESS_KEY_ID"] = frozen.access_key
        env["AWS_SECRET_ACCESS_KEY"] = frozen.secret_key
        if frozen.token:
            env["AWS_SESSION_TOKEN"] = frozen.token
    return env

@asynccontextmanager
async def session(id: str):
    stdio_mcp_client = MCPClient(lambda: stdio_client(
        StdioServerParameters(
            command="uvx", 
            args=[
                "awslabs.postgres-mcp-server@latest",
                "--resource_arn", CLUSTER_ARN,
                "--secret_arn", SECRET_ARN,
                "--database", DATABASE_NAME,
                "--region", AWS_REGION,
                "--readonly", "True",
            ],
            env=build_mcp_environment(),
        )
    ))

    # Get/create conversation in S3
    session_manager = S3SessionManager(
        boto_session=BOTO_SESSION,
        bucket=STATE_BUCKET,
        session_id=id,
    )
    conversation_manager = SlidingWindowConversationManager(
        window_size=10,  # Maximum number of messages to keep
        should_truncate_results=True, # Enable truncating the tool result when a message is too large for the model's context window 
    )
    
    with stdio_mcp_client:
        tools = stdio_mcp_client.list_tools_sync()
        agent = Agent(
            conversation_manager=conversation_manager,
            model=BEDROCK_MODEL,
            session_manager=session_manager,
            tools=tools,
        )
        try:
            yield agent
        finally:
            pass

class ChatRequest(BaseModel):
    prompt: str

app = FastAPI()

@app.post('/api/chat')
async def chat(chat_request: ChatRequest, request: Request):
    session_id: str = request.cookies.get("session_id", str(uuid.uuid4()))
    response = StreamingResponse(
        generate(session_id, chat_request.prompt, request),
        media_type="text/event-stream"
    )
    response.set_cookie(key="session_id", value=session_id)
    return response

async def generate(session_id: str, prompt: str, request: Request):
    async with session(session_id) as agent:
        global current_agent
        current_agent = agent  # Store the current agent for use in tools
        try:
            async for event in agent.stream_async(prompt):
                if "complete" in event:
                    logger.info("Response generation complete")
                if "data" in event:
                    yield f"data: {json.dumps(event['data'])}\n\n"
        except Exception as e:
            error_message = json.dumps({"error": str(e)})
            yield f"event: error\ndata: {error_message}\n\n"
        finally:
            current_agent = None

@app.get('/api/chat')
async def chat_get(request: Request):
    session_id = request.cookies.get("session_id", str(uuid.uuid4()))
    async with session(session_id) as agent:
        # Filter messages to only include first text content
        filtered_messages = []
        for message in agent.messages:
            if (message.get("content") and 
                len(message["content"]) > 0 and 
                "text" in message["content"][0]):
                filtered_messages.append({
                    "role": message["role"],
                    "content": [{
                        "text": message["content"][0]["text"]
                    }]
                })
 
    response = Response(
        content=json.dumps({
            "messages": filtered_messages,
        }),
        media_type="application/json",
    )
    response.set_cookie(key="session_id", value=session_id)
    return response


# Called by the Lambda Adapter to check liveness
@app.get("/")
async def root():
    return Response(
        content=json.dumps({"message": "OK"}),
        media_type="application/json",
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
