"""Lambda entrypoint that prepares the Aurora database with NYC crash data."""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Iterable, Optional

import boto3
from botocore.exceptions import ClientError

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

S3_SOURCE_BUCKET = os.environ["SOURCE_DATA_BUCKET"]
S3_SOURCE_KEY = os.environ["SOURCE_DATA_KEY"]
DESTINATION_BUCKET = os.environ["DESTINATION_BUCKET"]
DESTINATION_KEY = os.environ.get("DESTINATION_KEY", S3_SOURCE_KEY)
CLUSTER_ARN = os.environ["CLUSTER_ARN"]
SECRET_ARN = os.environ["SECRET_ARN"]
DATABASE_NAME = os.environ["DATABASE_NAME"]
AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

STAGING_TABLE = "crashes_staging"

S3_CLIENT = boto3.client("s3")
RDS_DATA_CLIENT = boto3.client("rds-data")


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler."""
    LOGGER.info("Starting crash data load")
    copy_dataset_to_bucket()
    ensure_database_exists()
    enable_extensions()
    create_target_table()
    load_dataset()
    LOGGER.info("Crash data load complete")
    return {"status": "complete"}


def copy_dataset_to_bucket() -> None:
    """Copy the source dataset into the project S3 bucket."""
    if S3_SOURCE_BUCKET == DESTINATION_BUCKET and S3_SOURCE_KEY == DESTINATION_KEY:
        LOGGER.info("Source and destination objects are identical; skipping copy")
        return

    LOGGER.info(
        "Copying dataset from s3://%s/%s to s3://%s/%s",
        S3_SOURCE_BUCKET,
        S3_SOURCE_KEY,
        DESTINATION_BUCKET,
        DESTINATION_KEY,
    )

    try:
        S3_CLIENT.copy_object(
            CopySource={"Bucket": S3_SOURCE_BUCKET, "Key": S3_SOURCE_KEY},
            Bucket=DESTINATION_BUCKET,
            Key=DESTINATION_KEY,
        )
    except ClientError as error:
        LOGGER.error("Unable to copy dataset: %s", error, exc_info=True)
        raise


def ensure_database_exists() -> None:
    """Create the nycrashes database when it does not already exist."""
    try:
        execute_sql(
            f'CREATE DATABASE "{DATABASE_NAME}"',
            database="postgres",
        )
        LOGGER.info("Database %s created", DATABASE_NAME)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") == "BadRequestException" and "already exists" in error.response.get("Error", {}).get("Message", ""):
            LOGGER.info("Database %s already exists; continuing", DATABASE_NAME)
            return
        raise


def enable_extensions() -> None:
    """Enable required database extensions."""
    for extension in ("aws_s3", "postgis"):
        execute_sql(
            f"CREATE EXTENSION IF NOT EXISTS {extension};",
            database=DATABASE_NAME,
        )


def create_target_table() -> None:
    """Ensure the destination crashes table exists."""
    execute_sql(
        """
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
        """,
        database=DATABASE_NAME,
    )


def load_dataset() -> None:
    """Load crash data into the target table using the aws_s3 extension."""
    recreate_staging_table()
    try:
        import_into_staging()
        populate_target_table()
    finally:
        drop_staging_table()


def recreate_staging_table() -> None:
    execute_sql(f"DROP TABLE IF EXISTS {STAGING_TABLE};", database=DATABASE_NAME)
    execute_sql(
        f"""
        CREATE TABLE {STAGING_TABLE} (
            collision_id BIGINT PRIMARY KEY,
            crash_date TIMESTAMP WITHOUT TIME ZONE,
            crash_time TEXT,
            borough TEXT,
            zip_code TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            location TEXT,
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
        """,
        database=DATABASE_NAME,
    )


def import_into_staging() -> None:
    execute_sql(
        """
        SELECT aws_s3.table_import_from_s3(
            :table_name,
            '',
            '(format csv, header true)',
            :bucket_name,
            :object_key,
            :aws_region
        );
        """,
        database=DATABASE_NAME,
        parameters=[
            _string_param("table_name", STAGING_TABLE),
            _string_param("bucket_name", DESTINATION_BUCKET),
            _string_param("object_key", DESTINATION_KEY),
            _string_param("aws_region", AWS_REGION),
        ],
    )


def populate_target_table() -> None:
    execute_sql("TRUNCATE TABLE crashes;", database=DATABASE_NAME)
    execute_sql(
        """
        INSERT INTO crashes (
            collision_id,
            crash_date,
            crash_time,
            borough,
            zip_code,
            latitude,
            longitude,
            location,
            on_street_name,
            off_street_name,
            cross_street_name,
            number_of_persons_injured,
            number_of_persons_killed,
            number_of_pedestrians_injured,
            number_of_pedestrians_killed,
            number_of_cyclist_injured,
            number_of_cyclist_killed,
            number_of_motorist_injured,
            number_of_motorist_killed,
            contributing_factor_vehicle_1,
            contributing_factor_vehicle_2,
            contributing_factor_vehicle_3,
            contributing_factor_vehicle_4,
            contributing_factor_vehicle_5,
            vehicle_type_code1,
            vehicle_type_code2,
            vehicle_type_code3,
            vehicle_type_code4,
            vehicle_type_code5
        )
        SELECT
            collision_id,
            crash_date,
            crash_time,
            borough,
            zip_code,
            latitude,
            longitude,
            CASE
                WHEN location IS NULL OR TRIM(location) = '' THEN NULL
                ELSE ST_SetSRID(ST_GeomFromText(location), 4326)
            END AS location,
            on_street_name,
            off_street_name,
            cross_street_name,
            number_of_persons_injured,
            number_of_persons_killed,
            number_of_pedestrians_injured,
            number_of_pedestrians_killed,
            number_of_cyclist_injured,
            number_of_cyclist_killed,
            number_of_motorist_injured,
            number_of_motorist_killed,
            contributing_factor_vehicle_1,
            contributing_factor_vehicle_2,
            contributing_factor_vehicle_3,
            contributing_factor_vehicle_4,
            contributing_factor_vehicle_5,
            vehicle_type_code1,
            vehicle_type_code2,
            vehicle_type_code3,
            vehicle_type_code4,
            vehicle_type_code5
        FROM crashes_staging
        ON CONFLICT (collision_id) DO UPDATE SET
            crash_date = EXCLUDED.crash_date,
            crash_time = EXCLUDED.crash_time,
            borough = EXCLUDED.borough,
            zip_code = EXCLUDED.zip_code,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            location = EXCLUDED.location,
            on_street_name = EXCLUDED.on_street_name,
            off_street_name = EXCLUDED.off_street_name,
            cross_street_name = EXCLUDED.cross_street_name,
            number_of_persons_injured = EXCLUDED.number_of_persons_injured,
            number_of_persons_killed = EXCLUDED.number_of_persons_killed,
            number_of_pedestrians_injured = EXCLUDED.number_of_pedestrians_injured,
            number_of_pedestrians_killed = EXCLUDED.number_of_pedestrians_killed,
            number_of_cyclist_injured = EXCLUDED.number_of_cyclist_injured,
            number_of_cyclist_killed = EXCLUDED.number_of_cyclist_killed,
            number_of_motorist_injured = EXCLUDED.number_of_motorist_injured,
            number_of_motorist_killed = EXCLUDED.number_of_motorist_killed,
            contributing_factor_vehicle_1 = EXCLUDED.contributing_factor_vehicle_1,
            contributing_factor_vehicle_2 = EXCLUDED.contributing_factor_vehicle_2,
            contributing_factor_vehicle_3 = EXCLUDED.contributing_factor_vehicle_3,
            contributing_factor_vehicle_4 = EXCLUDED.contributing_factor_vehicle_4,
            contributing_factor_vehicle_5 = EXCLUDED.contributing_factor_vehicle_5,
            vehicle_type_code1 = EXCLUDED.vehicle_type_code1,
            vehicle_type_code2 = EXCLUDED.vehicle_type_code2,
            vehicle_type_code3 = EXCLUDED.vehicle_type_code3,
            vehicle_type_code4 = EXCLUDED.vehicle_type_code4,
            vehicle_type_code5 = EXCLUDED.vehicle_type_code5;
        """,
        database=DATABASE_NAME,
    )


def drop_staging_table() -> None:
    execute_sql(f"DROP TABLE IF EXISTS {STAGING_TABLE};", database=DATABASE_NAME)


def execute_sql(
    sql: str,
    *,
    database: str,
    parameters: Optional[Iterable[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Execute a SQL statement using the RDS Data API."""
    kwargs: Dict[str, Any] = {
        "resourceArn": CLUSTER_ARN,
        "secretArn": SECRET_ARN,
        "database": database,
        "sql": sql,
    }
    if parameters:
        kwargs["parameters"] = list(parameters)
    LOGGER.debug("Executing SQL against %s: %s", database, sql)
    return RDS_DATA_CLIENT.execute_statement(**kwargs)


def _string_param(name: str, value: str) -> Dict[str, Any]:
    return {"name": name, "value": {"stringValue": value}}


if __name__ == "__main__":
    handler({}, {})
