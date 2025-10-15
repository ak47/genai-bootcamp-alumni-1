"""Lambda entrypoint that prepares the Aurora database with NYC crash data."""

from __future__ import annotations

import logging
import os

import boto3
from botocore.exceptions import ClientError, WaiterError
from populator_types import (
    LambdaContext,
    LambdaEvent,
    LambdaResponse,
    SqlParameter,
    SqlParameters,
    SqlResult,
)

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

DATA_BUCKET = os.environ["DATA_BUCKET"]
NYC_DATA_KEY = os.environ["NYC_DATA_KEY"]
CA_DATA_KEYS = os.environ["CA_DATA_KEYS"].split(",")
CLUSTER_ARN = os.environ["CLUSTER_ARN"]
SECRET_ARN = os.environ["SECRET_ARN"]
DATABASE_NAME = os.environ["DATABASE_NAME"]
CLUSTER_IDENTIFIER = os.environ.get("CLUSTER_IDENTIFIER", "")
AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Table name mappings for California data files
CA_TABLE_MAPPING = {
    "2025crashes.csv": "ca_crashes",
    "2025injuredwitnesspassengers.csv": "ca_injuredwitnesspassengers",
    "2025parties.csv": "ca_parties",
}

S3_CLIENT = boto3.client("s3")
RDS_DATA_CLIENT = boto3.client("rds-data")
RDS_CLIENT = boto3.client("rds")


def handler(event: LambdaEvent, context: LambdaContext) -> LambdaResponse:
    """AWS Lambda handler."""
    request_type = event.get("RequestType") if isinstance(event, dict) else None
    physical_resource_id = (
        event.get("PhysicalResourceId")
        if isinstance(event, dict)
        else None
    ) or f"{CLUSTER_IDENTIFIER or 'crashes'}-populator"

    LOGGER.info("Handling request type: %s", request_type)
    if request_type == "Delete":
        LOGGER.info("Delete request received; starting cleanup.")
        cleanup_on_delete()
        return {"PhysicalResourceId": physical_resource_id, "status": "skipped"}

    wait_for_cluster_available()
    LOGGER.info("Starting crash data load")
    ensure_database_exists()
    enable_extensions()
    create_nyc_crashes_table()
    create_california_tables()
    load_nyc_dataset()
    load_california_datasets()
    LOGGER.info("Crash data load complete")
    return {"PhysicalResourceId": physical_resource_id, "status": "complete"}


def cleanup_on_delete() -> None:
    """Best-effort cleanup for stack deletion."""
    try:
        cleanup_database_for_delete()
    except Exception as error:  # noqa: BLE001 - log and continue during delete
        LOGGER.warning("Database cleanup during delete failed: %s", error, exc_info=True)


def cleanup_database_for_delete() -> None:
    """Drop the project database before the cluster is destroyed."""
    if not CLUSTER_IDENTIFIER:
        LOGGER.info("CLUSTER_IDENTIFIER not provided; skipping database cleanup.")
        return

    LOGGER.info("Ensuring cluster %s is available for cleanup", CLUSTER_IDENTIFIER)
    try:
        wait_for_cluster_available()
    except WaiterError as error:
        LOGGER.info(
            "Cluster %s is not available (likely deleting); skipping cleanup: %s",
            CLUSTER_IDENTIFIER,
            error,
        )
        return
    except ClientError as error:
        LOGGER.info(
            "Cluster %s could not be described; skipping cleanup: %s",
            CLUSTER_IDENTIFIER,
            error,
        )
        return

    LOGGER.info("Dropping database %s prior to cluster removal", DATABASE_NAME)
    drop_database_if_exists()
    LOGGER.info("Database cleanup complete")


def ensure_database_exists() -> None:
    """Drop and recreate the nycrashes database to ensure a clean state."""
    LOGGER.info("Recreating database %s", DATABASE_NAME)
    drop_database_if_exists()
    try:
        execute_sql(
            f'CREATE DATABASE "{DATABASE_NAME}"',
            database="postgres",
        )
    except ClientError as error:
        LOGGER.error("Unable to create database %s: %s", DATABASE_NAME, error, exc_info=True)
        raise
    LOGGER.info("Database %s created", DATABASE_NAME)


def drop_database_if_exists() -> None:
    """Drop the nycrashes database when it already exists."""
    LOGGER.info("Dropping database %s if it exists", DATABASE_NAME)
    terminate_database_connections()
    try:
        execute_sql(
            f'DROP DATABASE IF EXISTS "{DATABASE_NAME}"',
            database="postgres",
        )
    except ClientError as error:
        LOGGER.error("Unable to drop database %s: %s", DATABASE_NAME, error, exc_info=True)
        raise


def terminate_database_connections() -> None:
    """Terminate existing connections to the nycrashes database."""
    execute_sql(
        """
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = :database_name
          AND pid <> pg_backend_pid();
        """,
        database="postgres",
        parameters=[_string_param("database_name", DATABASE_NAME)],
    )


def enable_extensions() -> None:
    """Enable required database extensions."""
    for extension in ("aws_s3", "postgis"):
        execute_sql(
            f"CREATE EXTENSION IF NOT EXISTS {extension} CASCADE;",
            database=DATABASE_NAME,
        )


def create_nyc_crashes_table() -> None:
    """Ensure the NYC crashes table exists."""
    execute_sql(
        """
        CREATE TABLE IF NOT EXISTS nyc_crashes (
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


def create_california_tables() -> None:
    """Create California crash data tables."""
    LOGGER.info("Creating California crash data tables")
    
    # Main crashes table
    execute_sql(
        """
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
        """,
        database=DATABASE_NAME,
    )
    
    # Injured/witness/passengers table
    execute_sql(
        """
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
            seat_position_description TEXT
        );
        """,
        database=DATABASE_NAME,
    )
    
    # Parties table
    execute_sql(
        """
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
            is_dre_conducted BOOLEAN
        );
        """,
        database=DATABASE_NAME,
    )
    
    LOGGER.info("California tables created")


def load_nyc_dataset() -> None:
    """Load NYC crash data from CSV file into nyc_crashes table."""
    LOGGER.info("Loading NYC crash data from %s", NYC_DATA_KEY)
    staging_table = "nyc_crashes_staging"
    
    try:
        # Drop existing staging table
        execute_sql(f"DROP TABLE IF EXISTS {staging_table};", database=DATABASE_NAME)
        
        # Create staging table matching NYC CSV structure
        execute_sql(
            f"""
            CREATE TABLE {staging_table} (
                crash_date TEXT,
                crash_time TEXT,
                borough TEXT,
                zip_code TEXT,
                latitude TEXT,
                longitude TEXT,
                location TEXT,
                on_street_name TEXT,
                cross_street_name TEXT,
                off_street_name TEXT,
                number_of_persons_injured TEXT,
                number_of_persons_killed TEXT,
                number_of_pedestrians_injured TEXT,
                number_of_pedestrians_killed TEXT,
                number_of_cyclist_injured TEXT,
                number_of_cyclist_killed TEXT,
                number_of_motorist_injured TEXT,
                number_of_motorist_killed TEXT,
                contributing_factor_vehicle_1 TEXT,
                contributing_factor_vehicle_2 TEXT,
                contributing_factor_vehicle_3 TEXT,
                contributing_factor_vehicle_4 TEXT,
                contributing_factor_vehicle_5 TEXT,
                collision_id TEXT,
                vehicle_type_code1 TEXT,
                vehicle_type_code2 TEXT,
                vehicle_type_code3 TEXT,
                vehicle_type_code4 TEXT,
                vehicle_type_code5 TEXT
            );
            """,
            database=DATABASE_NAME,
        )
        
        # Import CSV data into staging table
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
                _string_param("table_name", staging_table),
                _string_param("bucket_name", DATA_BUCKET),
                _string_param("object_key", NYC_DATA_KEY),
                _string_param("aws_region", AWS_REGION),
            ],
        )
        
        # Move data from staging to target table with type conversions
        execute_sql(
            f"""
            INSERT INTO nyc_crashes (
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
                NULLIF(collision_id, '')::BIGINT,
                NULLIF(crash_date, '')::TIMESTAMP,
                NULLIF(crash_time, ''),
                NULLIF(borough, ''),
                NULLIF(zip_code, ''),
                NULLIF(latitude, '')::DOUBLE PRECISION,
                NULLIF(longitude, '')::DOUBLE PRECISION,
                CASE
                    WHEN NULLIF(latitude, '') IS NOT NULL AND NULLIF(longitude, '') IS NOT NULL
                        THEN ST_SetSRID(ST_MakePoint(NULLIF(longitude, '')::DOUBLE PRECISION, NULLIF(latitude, '')::DOUBLE PRECISION), 4326)
                    ELSE NULL
                END,
                NULLIF(on_street_name, ''),
                NULLIF(off_street_name, ''),
                NULLIF(cross_street_name, ''),
                NULLIF(number_of_persons_injured, '')::INTEGER,
                NULLIF(number_of_persons_killed, '')::INTEGER,
                NULLIF(number_of_pedestrians_injured, '')::INTEGER,
                NULLIF(number_of_pedestrians_killed, '')::INTEGER,
                NULLIF(number_of_cyclist_injured, '')::INTEGER,
                NULLIF(number_of_cyclist_killed, '')::INTEGER,
                NULLIF(number_of_motorist_injured, '')::INTEGER,
                NULLIF(number_of_motorist_killed, '')::INTEGER,
                NULLIF(contributing_factor_vehicle_1, ''),
                NULLIF(contributing_factor_vehicle_2, ''),
                NULLIF(contributing_factor_vehicle_3, ''),
                NULLIF(contributing_factor_vehicle_4, ''),
                NULLIF(contributing_factor_vehicle_5, ''),
                NULLIF(vehicle_type_code1, ''),
                NULLIF(vehicle_type_code2, ''),
                NULLIF(vehicle_type_code3, ''),
                NULLIF(vehicle_type_code4, ''),
                NULLIF(vehicle_type_code5, '')
            FROM {staging_table}
            WHERE NULLIF(collision_id, '') IS NOT NULL
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
        
    finally:
        # Clean up staging table
        execute_sql(f"DROP TABLE IF EXISTS {staging_table};", database=DATABASE_NAME)
    
    LOGGER.info("NYC crash data loaded successfully")


def load_california_datasets() -> None:
    """Load California crash data from CSV files into their respective tables."""
    LOGGER.info("Loading California crash data")
    
    for csv_file, table_name in CA_TABLE_MAPPING.items():
        if csv_file not in CA_DATA_KEYS:
            LOGGER.warning("Skipping %s - not in CA_DATA_KEYS", csv_file)
            continue
            
        LOGGER.info("Loading %s into %s", csv_file, table_name)
        staging_table = f"{table_name}_staging"
        
        try:
            # Drop existing staging table
            execute_sql(f"DROP TABLE IF EXISTS {staging_table};", database=DATABASE_NAME)
            
            # Create staging table based on target table structure
            create_staging_table_for_california(staging_table, table_name)
            
            # Import CSV data into staging table
            import_california_csv_to_staging(staging_table, csv_file)
            
            # Move data from staging to target table
            populate_california_target_table(staging_table, table_name)
            
        finally:
            # Clean up staging table
            execute_sql(f"DROP TABLE IF EXISTS {staging_table};", database=DATABASE_NAME)
    
    LOGGER.info("California crash data loaded successfully")


def create_staging_table_for_california(staging_table: str, target_table: str) -> None:
    """Create a staging table matching the target table structure."""
    if target_table == "ca_crashes":
        execute_sql(
            f"""
            CREATE TABLE {staging_table} (
                collision_id TEXT,
                report_number TEXT,
                report_version TEXT,
                is_preliminary TEXT,
                ncic_code TEXT,
                crash_date_time TEXT,
                crash_time_description TEXT,
                beat TEXT,
                city_id TEXT,
                city_code TEXT,
                city_name TEXT,
                county_code TEXT,
                city_is_active TEXT,
                city_is_incorporated TEXT,
                collision_type_code TEXT,
                collision_type_description TEXT,
                collision_type_other_desc TEXT,
                day_of_week TEXT,
                dispatch_notified TEXT,
                has_photographs TEXT,
                hit_run TEXT,
                is_attachments_mailed TEXT,
                is_deleted TEXT,
                is_highway_related TEXT,
                is_tow_away TEXT,
                judicial_district TEXT,
                motor_vehicle_involved_with_code TEXT,
                motor_vehicle_involved_with_desc TEXT,
                motor_vehicle_involved_with_other_desc TEXT,
                number_injured TEXT,
                number_killed TEXT,
                weather_1 TEXT,
                weather_2 TEXT,
                road_condition_1 TEXT,
                road_condition_2 TEXT,
                special_condition TEXT,
                lighting_code TEXT,
                lighting_description TEXT,
                latitude TEXT,
                longitude TEXT,
                milepost_direction TEXT,
                milepost_distance TEXT,
                milepost_marker TEXT,
                milepost_unit_of_measure TEXT,
                pedestrian_action_code TEXT,
                pedestrian_action_desc TEXT,
                prepared_date TEXT,
                primary_collision_factor_code TEXT,
                primary_collision_factor_violation TEXT,
                primary_collision_factor_is_cited TEXT,
                primary_collision_party_number TEXT,
                primary_road TEXT,
                reporting_district TEXT,
                reporting_district_code TEXT,
                reviewed_date TEXT,
                roadway_surface_code TEXT,
                secondary_direction TEXT,
                secondary_distance TEXT,
                secondary_road TEXT,
                secondary_unit_of_measure TEXT,
                sketch_desc TEXT,
                traffic_control_device_code TEXT,
                created_date TEXT,
                modified_date TEXT,
                is_county_road TEXT,
                is_freeway TEXT,
                chp555_version TEXT,
                is_additional_object_struck TEXT,
                notification_date TEXT,
                notification_time_description TEXT,
                has_digital_media_files TEXT,
                evidence_number TEXT,
                is_location_refer_to_narrative TEXT,
                is_aoi_one_same_as_location TEXT
            );
            """,
            database=DATABASE_NAME,
        )
    elif target_table == "ca_injuredwitnesspassengers":
        execute_sql(
            f"""
            CREATE TABLE {staging_table} (
                collision_id TEXT,
                injured_wit_pass_id TEXT,
                stated_age TEXT,
                gender TEXT,
                gender_desc TEXT,
                race TEXT,
                race_desc TEXT,
                is_witness_only TEXT,
                is_passenger_only TEXT,
                extent_of_injury_code TEXT,
                injured_person_type TEXT,
                seat_position TEXT,
                seat_position_other TEXT,
                air_bag_code TEXT,
                air_bag_description TEXT,
                safety_equipment_code TEXT,
                safety_equipment_description TEXT,
                ejected TEXT,
                is_vovc_notified TEXT,
                party_number TEXT,
                seat_position_description TEXT
            );
            """,
            database=DATABASE_NAME,
        )
    elif target_table == "ca_parties":
        execute_sql(
            f"""
            CREATE TABLE {staging_table} (
                party_id TEXT,
                collision_id TEXT,
                party_number TEXT,
                party_type TEXT,
                is_at_fault TEXT,
                is_on_duty_emergency_vehicle TEXT,
                is_hit_and_run TEXT,
                airbag_code TEXT,
                airbag_description TEXT,
                safety_equipment_code TEXT,
                safety_equipment_description TEXT,
                special_information TEXT,
                other_associate_factor TEXT,
                inattention TEXT,
                direction_of_travel TEXT,
                street_or_highway_name TEXT,
                speed_limit TEXT,
                movement_prec_coll_code TEXT,
                movement_prec_coll_description TEXT,
                sobriety_drug_physical_code1 TEXT,
                sobriety_drug_physical_description1 TEXT,
                sobriety_drug_physical_code2 TEXT,
                sobriety_drug_physical_description2 TEXT,
                gender_code TEXT,
                gender_description TEXT,
                stated_age TEXT,
                driver_license_class TEXT,
                driver_license_state_code TEXT,
                race_code TEXT,
                race_desc TEXT,
                vehicle1_type_id TEXT,
                vehicle1_type_desc TEXT,
                vehicle1_year TEXT,
                vehicle1_make TEXT,
                vehicle1_model TEXT,
                vehicle1_color TEXT,
                v1_is_vehicle_towed TEXT,
                vehicle2_type_id TEXT,
                vehicle2_type_desc TEXT,
                vehicle2_year TEXT,
                vehicle2_make TEXT,
                vehicle2_model TEXT,
                vehicle2_color TEXT,
                v2_is_vehicle_towed TEXT,
                lane TEXT,
                thru_lanes TEXT,
                total_lanes TEXT,
                is_dre_conducted TEXT
            );
            """,
            database=DATABASE_NAME,
        )


def import_california_csv_to_staging(staging_table: str, csv_file: str) -> None:
    """Import CSV data into staging table using aws_s3 extension."""
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
            _string_param("table_name", staging_table),
            _string_param("bucket_name", DATA_BUCKET),
            _string_param("object_key", csv_file),
            _string_param("aws_region", AWS_REGION),
        ],
    )


def populate_california_target_table(staging_table: str, target_table: str) -> None:
    """Populate target table from staging with type conversions."""
    if target_table == "ca_crashes":
        execute_sql(
            f"""
            INSERT INTO {target_table} (
                collision_id, report_number, report_version, is_preliminary, ncic_code,
                crash_date_time, crash_time_description, beat, city_id, city_code, city_name,
                county_code, city_is_active, city_is_incorporated, collision_type_code,
                collision_type_description, collision_type_other_desc, day_of_week,
                dispatch_notified, has_photographs, hit_run, is_attachments_mailed,
                is_deleted, is_highway_related, is_tow_away, judicial_district,
                motor_vehicle_involved_with_code, motor_vehicle_involved_with_desc,
                motor_vehicle_involved_with_other_desc, number_injured, number_killed,
                weather_1, weather_2, road_condition_1, road_condition_2, special_condition,
                lighting_code, lighting_description, latitude, longitude, location,
                milepost_direction, milepost_distance, milepost_marker,
                milepost_unit_of_measure, pedestrian_action_code, pedestrian_action_desc,
                prepared_date, primary_collision_factor_code, primary_collision_factor_violation,
                primary_collision_factor_is_cited, primary_collision_party_number,
                primary_road, reporting_district, reporting_district_code, reviewed_date,
                roadway_surface_code, secondary_direction, secondary_distance, secondary_road,
                secondary_unit_of_measure, sketch_desc, traffic_control_device_code,
                created_date, modified_date, is_county_road, is_freeway, chp555_version,
                is_additional_object_struck, notification_date, notification_time_description,
                has_digital_media_files, evidence_number, is_location_refer_to_narrative,
                is_aoi_one_same_as_location
            )
            SELECT
                NULLIF(collision_id, '')::BIGINT,
                NULLIF(report_number, ''),
                NULLIF(report_version, '')::INTEGER,
                NULLIF(is_preliminary, '') = 'True',
                NULLIF(ncic_code, ''),
                NULLIF(crash_date_time, '')::TIMESTAMP,
                NULLIF(crash_time_description, ''),
                NULLIF(beat, ''),
                NULLIF(city_id, '')::INTEGER,
                NULLIF(city_code, ''),
                NULLIF(city_name, ''),
                NULLIF(county_code, ''),
                NULLIF(city_is_active, '') = 'True',
                NULLIF(city_is_incorporated, '') = 'True',
                NULLIF(collision_type_code, ''),
                NULLIF(collision_type_description, ''),
                NULLIF(collision_type_other_desc, ''),
                NULLIF(day_of_week, ''),
                NULLIF(dispatch_notified, ''),
                NULLIF(has_photographs, '') = 'True',
                NULLIF(hit_run, ''),
                NULLIF(is_attachments_mailed, '') = 'True',
                NULLIF(is_deleted, '') = 'True',
                NULLIF(is_highway_related, '') = 'True',
                NULLIF(is_tow_away, '') = 'True',
                NULLIF(judicial_district, ''),
                NULLIF(motor_vehicle_involved_with_code, ''),
                NULLIF(motor_vehicle_involved_with_desc, ''),
                NULLIF(motor_vehicle_involved_with_other_desc, ''),
                NULLIF(number_injured, '')::INTEGER,
                NULLIF(number_killed, '')::INTEGER,
                NULLIF(weather_1, ''),
                NULLIF(weather_2, ''),
                NULLIF(road_condition_1, ''),
                NULLIF(road_condition_2, ''),
                NULLIF(special_condition, ''),
                NULLIF(lighting_code, ''),
                NULLIF(lighting_description, ''),
                NULLIF(latitude, '')::DOUBLE PRECISION,
                NULLIF(longitude, '')::DOUBLE PRECISION,
                CASE
                    WHEN NULLIF(latitude, '') IS NOT NULL AND NULLIF(longitude, '') IS NOT NULL
                        THEN ST_SetSRID(ST_MakePoint(NULLIF(longitude, '')::DOUBLE PRECISION, NULLIF(latitude, '')::DOUBLE PRECISION), 4326)
                    ELSE NULL
                END,
                NULLIF(milepost_direction, ''),
                NULLIF(milepost_distance, ''),
                NULLIF(milepost_marker, ''),
                NULLIF(milepost_unit_of_measure, ''),
                NULLIF(pedestrian_action_code, ''),
                NULLIF(pedestrian_action_desc, ''),
                NULLIF(prepared_date, '')::TIMESTAMP,
                NULLIF(primary_collision_factor_code, ''),
                NULLIF(primary_collision_factor_violation, ''),
                NULLIF(primary_collision_factor_is_cited, '') = 'True',
                NULLIF(primary_collision_party_number, '')::INTEGER,
                NULLIF(primary_road, ''),
                NULLIF(reporting_district, ''),
                NULLIF(reporting_district_code, ''),
                NULLIF(reviewed_date, '')::TIMESTAMP,
                NULLIF(roadway_surface_code, ''),
                NULLIF(secondary_direction, ''),
                NULLIF(secondary_distance, ''),
                NULLIF(secondary_road, ''),
                NULLIF(secondary_unit_of_measure, ''),
                NULLIF(sketch_desc, ''),
                NULLIF(traffic_control_device_code, ''),
                NULLIF(created_date, '')::TIMESTAMP,
                NULLIF(modified_date, '')::TIMESTAMP,
                NULLIF(is_county_road, '') = 'True',
                NULLIF(is_freeway, '') = 'True',
                NULLIF(chp555_version, ''),
                NULLIF(is_additional_object_struck, '') = 'True',
                NULLIF(notification_date, '')::TIMESTAMP,
                NULLIF(notification_time_description, ''),
                NULLIF(has_digital_media_files, '') = 'True',
                NULLIF(evidence_number, ''),
                NULLIF(is_location_refer_to_narrative, '') = 'True',
                NULLIF(is_aoi_one_same_as_location, '') = 'True'
            FROM {staging_table}
            ON CONFLICT (collision_id) DO NOTHING;
            """,
            database=DATABASE_NAME,
        )
    elif target_table == "ca_injuredwitnesspassengers":
        execute_sql(
            f"""
            INSERT INTO {target_table} (
                injured_wit_pass_id, collision_id, stated_age, gender, gender_desc,
                race, race_desc, is_witness_only, is_passenger_only, extent_of_injury_code,
                injured_person_type, seat_position, seat_position_other, air_bag_code,
                air_bag_description, safety_equipment_code, safety_equipment_description,
                ejected, is_vovc_notified, party_number, seat_position_description
            )
            SELECT
                NULLIF(injured_wit_pass_id, '')::BIGINT,
                NULLIF(collision_id, '')::BIGINT,
                NULLIF(stated_age, '')::INTEGER,
                NULLIF(gender, ''),
                NULLIF(gender_desc, ''),
                NULLIF(race, ''),
                NULLIF(race_desc, ''),
                NULLIF(is_witness_only, '') = 'True',
                NULLIF(is_passenger_only, '') = 'True',
                NULLIF(extent_of_injury_code, ''),
                NULLIF(injured_person_type, ''),
                NULLIF(seat_position, ''),
                NULLIF(seat_position_other, ''),
                NULLIF(air_bag_code, ''),
                NULLIF(air_bag_description, ''),
                NULLIF(safety_equipment_code, ''),
                NULLIF(safety_equipment_description, ''),
                NULLIF(ejected, ''),
                NULLIF(is_vovc_notified, '') = 'True',
                NULLIF(party_number, '')::INTEGER,
                NULLIF(seat_position_description, '')
            FROM {staging_table}
            WHERE NULLIF(injured_wit_pass_id, '') IS NOT NULL
            ON CONFLICT (injured_wit_pass_id) DO NOTHING;
            """,
            database=DATABASE_NAME,
        )
    elif target_table == "ca_parties":
        execute_sql(
            f"""
            INSERT INTO {target_table} (
                party_id, collision_id, party_number, party_type, is_at_fault,
                is_on_duty_emergency_vehicle, is_hit_and_run, airbag_code,
                airbag_description, safety_equipment_code, safety_equipment_description,
                special_information, other_associate_factor, inattention,
                direction_of_travel, street_or_highway_name, speed_limit,
                movement_prec_coll_code, movement_prec_coll_description,
                sobriety_drug_physical_code1, sobriety_drug_physical_description1,
                sobriety_drug_physical_code2, sobriety_drug_physical_description2,
                gender_code, gender_description, stated_age, driver_license_class,
                driver_license_state_code, race_code, race_desc, vehicle1_type_id,
                vehicle1_type_desc, vehicle1_year, vehicle1_make, vehicle1_model,
                vehicle1_color, v1_is_vehicle_towed, vehicle2_type_id, vehicle2_type_desc,
                vehicle2_year, vehicle2_make, vehicle2_model, vehicle2_color,
                v2_is_vehicle_towed, lane, thru_lanes, total_lanes, is_dre_conducted
            )
            SELECT
                NULLIF(party_id, '')::BIGINT,
                NULLIF(collision_id, '')::BIGINT,
                NULLIF(party_number, '')::INTEGER,
                NULLIF(party_type, ''),
                NULLIF(is_at_fault, '') = 'True',
                NULLIF(is_on_duty_emergency_vehicle, '') = 'True',
                NULLIF(is_hit_and_run, '') = 'True',
                NULLIF(airbag_code, ''),
                NULLIF(airbag_description, ''),
                NULLIF(safety_equipment_code, ''),
                NULLIF(safety_equipment_description, ''),
                NULLIF(special_information, ''),
                NULLIF(other_associate_factor, ''),
                NULLIF(inattention, ''),
                NULLIF(direction_of_travel, ''),
                NULLIF(street_or_highway_name, ''),
                NULLIF(speed_limit, '')::INTEGER,
                NULLIF(movement_prec_coll_code, ''),
                NULLIF(movement_prec_coll_description, ''),
                NULLIF(sobriety_drug_physical_code1, ''),
                NULLIF(sobriety_drug_physical_description1, ''),
                NULLIF(sobriety_drug_physical_code2, ''),
                NULLIF(sobriety_drug_physical_description2, ''),
                NULLIF(gender_code, ''),
                NULLIF(gender_description, ''),
                NULLIF(stated_age, '')::INTEGER,
                NULLIF(driver_license_class, ''),
                NULLIF(driver_license_state_code, ''),
                NULLIF(race_code, ''),
                NULLIF(race_desc, ''),
                NULLIF(vehicle1_type_id, '')::INTEGER,
                NULLIF(vehicle1_type_desc, ''),
                NULLIF(vehicle1_year, '')::INTEGER,
                NULLIF(vehicle1_make, ''),
                NULLIF(vehicle1_model, ''),
                NULLIF(vehicle1_color, ''),
                NULLIF(v1_is_vehicle_towed, '') = 'True',
                NULLIF(vehicle2_type_id, '')::INTEGER,
                NULLIF(vehicle2_type_desc, ''),
                NULLIF(vehicle2_year, '')::INTEGER,
                NULLIF(vehicle2_make, ''),
                NULLIF(vehicle2_model, ''),
                NULLIF(vehicle2_color, ''),
                NULLIF(v2_is_vehicle_towed, '') = 'True',
                NULLIF(lane, ''),
                NULLIF(thru_lanes, '')::INTEGER,
                NULLIF(total_lanes, '')::INTEGER,
                NULLIF(is_dre_conducted, '') = 'True'
            FROM {staging_table}
            WHERE NULLIF(party_id, '') IS NOT NULL
            ON CONFLICT (party_id) DO NOTHING;
            """,
            database=DATABASE_NAME,
        )


def execute_sql(
    sql: str,
    *,
    database: str,
    parameters: SqlParameters | None = None,
) -> SqlResult:
    """Execute a SQL statement using the RDS Data API."""
    kwargs: dict[str, object] = {
        "resourceArn": CLUSTER_ARN,
        "secretArn": SECRET_ARN,
        "database": database,
        "sql": sql,
    }
    if parameters:
        kwargs["parameters"] = list(parameters)
    LOGGER.debug("Executing SQL against %s: %s", database, sql)
    return RDS_DATA_CLIENT.execute_statement(**kwargs)


def _string_param(name: str, value: str) -> SqlParameter:
    return {"name": name, "value": {"stringValue": value}}


def wait_for_cluster_available() -> None:
    if not CLUSTER_IDENTIFIER:
        LOGGER.warning("CLUSTER_IDENTIFIER not provided; skipping availability wait.")
        return

    LOGGER.info("Waiting for cluster %s to become available", CLUSTER_IDENTIFIER)
    waiter = RDS_CLIENT.get_waiter("db_cluster_available")
    try:
        waiter.wait(DBClusterIdentifier=CLUSTER_IDENTIFIER)
    except ClientError as error:
        LOGGER.error(
            "Cluster %s failed to become available: %s",
            CLUSTER_IDENTIFIER,
            error,
            exc_info=True,
        )
        raise


if __name__ == "__main__":
    handler({}, {})
