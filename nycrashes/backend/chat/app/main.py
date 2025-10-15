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
You are a helpful assistant that answers questions about New York City and California Highway Patrol motor vehicle crashes.
The crash data is in three table names `nyc_crashes, ca_crashes, ca_injuredwitnesspassengers, ca_injuredwitnesspassengers` which is created with the following schema:

```sql
        CREATE TABLE IF NOT EXISTS crashes (
            collision_id BIGINT PRIMARY KEY,
            crash_date TIMESTAMP WITHOUT TIME ZONE,
            crash_time TEXT,
            borough TEXT,
            zip_code TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            location geometry(Point, 4326),
            on_street_name TEXT,
            off_street_name TEXT,
            cross_street_name TEXT,
            number_of_persons_injured INTEGER,
            number_of_persons_killed INTEGER,
            number_of_pedestrians_injured INTEGER,
            number_of_pedestrians_killed INTEGER,
            number_of_cyclist_injured INTEGER,
            number_of_cyclist_killed INTEGER,
            number_of_motorist_injured INTEGER,
            number_of_motorist_killed INTEGER,
            contributing_factor_vehicle_1 TEXT,
            contributing_factor_vehicle_2 TEXT,
            contributing_factor_vehicle_3 TEXT,
            contributing_factor_vehicle_4 TEXT,
            contributing_factor_vehicle_5 TEXT,
            vehicle_type_code1 TEXT,
            vehicle_type_code2 TEXT,
            vehicle_type_code3 TEXT,
            vehicle_type_code4 TEXT,
            vehicle_type_code5 TEXT
        );

        CREATE TABLE IF NOT EXISTS ca_crashes (
            collision_id BIGINT PRIMARY KEY,
            report_number TEXT,
            report_version INTEGER,
            is_preliminary BOOLEAN,
            ncic_code TEXT,
            crash_date_time TIMESTAMP WITHOUT TIME ZONE,
            crash_time_description TEXT,
            beat TEXT,
            city_id INTEGER,
            city_code TEXT,
            city_name TEXT,
            county_code TEXT,
            city_is_active BOOLEAN,
            city_is_incorporated BOOLEAN,
            collision_type_code TEXT,
            collision_type_description TEXT,
            collision_type_other_desc TEXT,
            day_of_week TEXT,
            dispatch_notified TEXT,
            has_photographs BOOLEAN,
            hit_run TEXT,
            is_attachments_mailed BOOLEAN,
            is_deleted BOOLEAN,
            is_highway_related BOOLEAN,
            is_tow_away BOOLEAN,
            judicial_district TEXT,
            motor_vehicle_involved_with_code TEXT,
            motor_vehicle_involved_with_desc TEXT,
            motor_vehicle_involved_with_other_desc TEXT,
            number_injured INTEGER,
            number_killed INTEGER,
            weather_1 TEXT,
            weather_2 TEXT,
            road_condition_1 TEXT,
            road_condition_2 TEXT,
            special_condition TEXT,
            lighting_code TEXT,
            lighting_description TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            location geometry(Point, 4326),
            milepost_direction TEXT,
            milepost_distance TEXT,
            milepost_marker TEXT,
            milepost_unit_of_measure TEXT,
            pedestrian_action_code TEXT,
            pedestrian_action_desc TEXT,
            prepared_date TIMESTAMP WITHOUT TIME ZONE,
            primary_collision_factor_code TEXT,
            primary_collision_factor_violation TEXT,
            primary_collision_factor_is_cited BOOLEAN,
            primary_collision_party_number INTEGER,
            primary_road TEXT,
            reporting_district TEXT,
            reporting_district_code TEXT,
            reviewed_date TIMESTAMP WITHOUT TIME ZONE,
            roadway_surface_code TEXT,
            secondary_direction TEXT,
            secondary_distance TEXT,
            secondary_road TEXT,
            secondary_unit_of_measure TEXT,
            sketch_desc TEXT,
            traffic_control_device_code TEXT,
            created_date TIMESTAMP WITHOUT TIME ZONE,
            modified_date TIMESTAMP WITHOUT TIME ZONE,
            is_county_road BOOLEAN,
            is_freeway BOOLEAN,
            chp555_version TEXT,
            is_additional_object_struck BOOLEAN,
            notification_date TIMESTAMP WITHOUT TIME ZONE,
            notification_time_description TEXT,
            has_digital_media_files BOOLEAN,
            evidence_number TEXT,
            is_location_refer_to_narrative BOOLEAN,
            is_aoi_one_same_as_location BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS ca_injuredwitnesspassengers (
            injured_wit_pass_id BIGINT PRIMARY KEY,
            collision_id BIGINT,
            stated_age INTEGER,
            gender TEXT,
            gender_desc TEXT,
            race TEXT,
            race_desc TEXT,
            is_witness_only BOOLEAN,
            is_passenger_only BOOLEAN,
            extent_of_injury_code TEXT,
            injured_person_type TEXT,
            seat_position TEXT,
            seat_position_other TEXT,
            air_bag_code TEXT,
            air_bag_description TEXT,
            safety_equipment_code TEXT,
            safety_equipment_description TEXT,
            ejected TEXT,
            is_vovc_notified BOOLEAN,
            party_number INTEGER,
            seat_position_description TEXT,
            FOREIGN KEY (collision_id) REFERENCES ca_crashes(collision_id)
        );

        CREATE TABLE IF NOT EXISTS ca_parties (
            party_id BIGINT PRIMARY KEY,
            collision_id BIGINT,
            party_number INTEGER,
            party_type TEXT,
            is_at_fault BOOLEAN,
            is_on_duty_emergency_vehicle BOOLEAN,
            is_hit_and_run BOOLEAN,
            airbag_code TEXT,
            airbag_description TEXT,
            safety_equipment_code TEXT,
            safety_equipment_description TEXT,
            special_information TEXT,
            other_associate_factor TEXT,
            inattention TEXT,
            direction_of_travel TEXT,
            street_or_highway_name TEXT,
            speed_limit INTEGER,
            movement_prec_coll_code TEXT,
            movement_prec_coll_description TEXT,
            sobriety_drug_physical_code1 TEXT,
            sobriety_drug_physical_description1 TEXT,
            sobriety_drug_physical_code2 TEXT,
            sobriety_drug_physical_description2 TEXT,
            gender_code TEXT,
            gender_description TEXT,
            stated_age INTEGER,
            driver_license_class TEXT,
            driver_license_state_code TEXT,
            race_code TEXT,
            race_desc TEXT,
            vehicle1_type_id INTEGER,
            vehicle1_type_desc TEXT,
            vehicle1_year INTEGER,
            vehicle1_make TEXT,
            vehicle1_model TEXT,
            vehicle1_color TEXT,
            v1_is_vehicle_towed BOOLEAN,
            vehicle2_type_id INTEGER,
            vehicle2_type_desc TEXT,
            vehicle2_year INTEGER,
            vehicle2_make TEXT,
            vehicle2_model TEXT,
            vehicle2_color TEXT,
            v2_is_vehicle_towed BOOLEAN,
            lane TEXT,
            thru_lanes INTEGER,
            total_lanes INTEGER,
            is_dre_conducted BOOLEAN,
            FOREIGN KEY (collision_id) REFERENCES ca_crashes(collision_id)
        );
```
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
