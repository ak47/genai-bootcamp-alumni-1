from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
from mcp import stdio_client, StdioServerParameters
from pydantic import BaseModel
from strands.agent.conversation_manager import SlidingWindowConversationManager
from strands import Agent
from strands.models import BedrockModel
from strands.session.s3_session_manager import S3SessionManager
from strands.tools.mcp import MCPClient
import boto3
import logging
import os
import uuid
import uvicorn

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

SYSTEM_PROMPT = """
You are a helpful assistant that answers questions about New York City motor vehicle crashes.
"""

BEDROCK_MODEL = BedrockModel(
    model_id=MODEL_ID,
    # Add Guardrails here if desired
)

def session(id: str) -> Agent:
    # Add the Postgres MCP tool
    ## See https://awslabs.github.io/mcp/servers/postgres-mcp-server/ for details on where the args come from
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
        )
    ))
    tools = stdio_mcp_client.list_tools_sync()

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
    
    return Agent(
        conversation_manager=conversation_manager,
        model=BEDROCK_MODEL,
        session_manager=session_manager,
        tools=tools,
    )

class ChatRequest(BaseModel):
    prompt: str

app = FastAPI()

@app.post('/api/chat')
async def chat(chat_request: ChatRequest, request: Request):
    session_id: str = request.cookies.get("session_id", str(uuid.uuid4()))
    agent = session(session_id)
    global current_agent
    current_agent = agent  # Store the current agent for use in tools
    response = StreamingResponse(
        generate(agent, session_id, chat_request.prompt, request),
        media_type="text/event-stream"
    )
    response.set_cookie(key="session_id", value=session_id)
    return response

async def generate(agent: Agent, session_id: str, prompt: str, request: Request):
    try:
        async for event in agent.stream_async(prompt):
            if "complete" in event:
                logger.info("Response generation complete")
            if "data" in event:
                yield f"data: {json.dumps(event['data'])}\n\n"
    except Exception as e:
        error_message = json.dumps({"error": str(e)})
        yield f"event: error\ndata: {error_message}\n\n"

@app.get('/api/chat')
def chat_get(request: Request):
    session_id = request.cookies.get("session_id", str(uuid.uuid4()))
    agent = session(session_id)

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
