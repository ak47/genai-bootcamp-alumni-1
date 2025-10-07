from constructs import Construct
import aws_cdk.ec2 as ec2
import aws_cdk.rds as rds


class Backend(Construct):


    def __init__(self, scope: "Construct", id: str, vpc: ec2.IVpc) -> None:
        super().__init__(scope, id)

        """
        TODO: Aurora database cluster

          - Use VPC private subnets
          - Postgres VER_16_8
          - Use Aurora serverless v2
          - Min capacity 0.5
          - Max capacity 4
          - Performance insights enabled, 7 days retention
          - No readers, only a single writer
          - Create a security group which allows access on port 5432 from the VPC's security group
        """

        """
        TODO: Source data S3 bucket

          - S3 bucket which the Aurora cluster can read from using the aws_s3 extensions
          - Ensure Aurora DB has read permissions on the bucket
        """

        """
        TODO: Database populator Lambda function

          - Create a Lambda function at backend/populator/main.py
            It should:
            - Copy the crashes CSV data from s3://nyc-crashdata/Motor_Vehicle_Collisions_-_Crashes_20251007.csv to the S3 bucket created above
            - Create a new database: nycrashes
            - Enable the aws_s3 extension
            - Enable the PostGIS extension
            - Create table crashes with the schema:
              - collision_id Number (PRIMARY KEY)
              -	crash_date Floating Timestamp
              -	crash_time Text
              -	borough Text
              - zip_code Text
              - latitude Number
              - longitude Number
              - location Location
              - on_street_name Text
              - off_street_name Text
              - cross_street_name Text
              - number_of_persons_injured Number
              - number_of_persons_killed Number
              - number_of_pedestrians_injured Number
              -	number_of_pedestrians_killed Number
              - number_of_cyclist_injured Number
              - number_of_cyclist_killed Number
              - number_of_motorist_injured Number
              - number_of_motorist_killed Number
              - contributing_factor_vehicle_1 Text
              - contributing_factor_vehicle_2 Text
              - contributing_factor_vehicle_3 Text
              - contributing_factor_vehicle_4 Text
              - contributing_factor_vehicle_5 Text
              - vehicle_type_code1 Text
              - vehicle_type_code2 Text
              - vehicle_type_code3 Text
              - vehicle_type_code4 Text
              - vehicle_type_code5 Text
            - Load the CSV data into the crashes table using the aws_s3 extension
          - Create a Lambda function with code bundled from backend/populator/main.py (note the use of uv) 
          - Ensure the Lambda function is in the same VPC as the Aurora cluster
        """
