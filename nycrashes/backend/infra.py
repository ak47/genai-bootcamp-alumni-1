from aws_cdk import (
    Aws,
    BundlingOptions,
    Duration,
    RemovalPolicy,
)
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_iam as iam
import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_logs as logs
import aws_cdk.aws_rds as rds
import aws_cdk.aws_s3 as s3
from constructs import Construct


class Backend(Construct):
    DATA_SOURCE_BUCKET = "nyc-crashdata"
    DATA_SOURCE_KEY = "Motor_Vehicle_Collisions_-_Crashes_20251007.csv"
    DATABASE_NAME = "nycrashes"

    def __init__(self, scope: Construct, construct_id: str, *, vpc: ec2.IVpc) -> None:
        super().__init__(scope, construct_id)

        self.database_security_group = ec2.SecurityGroup(
            self,
            "DatabaseSecurityGroup",
            vpc=vpc,
            description="Security group for Aurora cluster",
            allow_all_outbound=True,
        )
        self.database_security_group.add_ingress_rule(
            peer=ec2.Peer.security_group_id(vpc.vpc_default_security_group),
            connection=ec2.Port.tcp(5432),
            description="Allow Postgres access from VPC default security group",
        )

        self.source_bucket = s3.Bucket(
            self,
            "SourceDataBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            auto_delete_objects=False,
        )

        import_role = iam.Role(
            self,
            "AuroraS3ImportRole",
            assumed_by=iam.ServicePrincipal("rds.amazonaws.com"),
            description="Role used by Aurora to import data from S3",
        )
        self.source_bucket.grant_read(import_role)

        self.cluster = rds.DatabaseCluster(
            self,
            "AuroraCluster",
            engine=rds.DatabaseClusterEngine.aurora_postgres(
                version=rds.AuroraPostgresEngineVersion.VER_15_4,
            ),
            credentials=rds.Credentials.from_generated_secret("dbadmin"),
            writer=rds.ClusterInstance.serverless_v2(
                "Writer",
                enable_performance_insights=True,
                performance_insight_retention=rds.PerformanceInsightRetention.DEFAULT,
                publicly_accessible=False,
            ),
            enable_data_api=True,
            vpc=vpc,
            security_groups=[self.database_security_group],
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            serverless_v2_min_capacity=0.5,
            serverless_v2_max_capacity=4,
            s3_import_role=import_role,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.populator_function = _lambda.Function(
            self,
            "PopulatorFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="main.handler",
            code=_lambda.Code.from_asset(
                "backend/populator",
                bundling=BundlingOptions(
                    image=_lambda.Runtime.PYTHON_3_12.bundling_image,
                    command=[
                        "bash",
                        "-c",
                        "pip install uv && "
                        "uv export --frozen --no-dev --no-editable -o requirements.txt && "
                        "cp -r . /asset-output",
                    ],
                    user="root",
                    platform="linux/amd64",
                ),
            ),
            timeout=Duration.minutes(15),
            memory_size=1024,
            log_retention=logs.RetentionDays.ONE_MONTH,
            vpc=vpc,
            security_groups=[ec2.SecurityGroup.from_security_group_id(
                self,
                "LambdaSecurityGroup",
                security_group_id=vpc.vpc_default_security_group,
            )],
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            environment={
                "SOURCE_DATA_BUCKET": self.DATA_SOURCE_BUCKET,
                "SOURCE_DATA_KEY": self.DATA_SOURCE_KEY,
                "DESTINATION_BUCKET": self.source_bucket.bucket_name,
                "DESTINATION_KEY": self.DATA_SOURCE_KEY,
                "CLUSTER_ARN": self.cluster.cluster_arn,
                "SECRET_ARN": self.cluster.secret.secret_arn,
                "DATABASE_NAME": self.DATABASE_NAME,
            },
        )

        self.source_bucket.grant_read_write(self.populator_function)
        self.cluster.secret.grant_read(self.populator_function)
        self.cluster.grant_data_api_access(self.populator_function)
        self.cluster.connections.allow_default_port_from(self.populator_function)

        self.populator_function.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    f"arn:aws:s3:::{self.DATA_SOURCE_BUCKET}",
                    f"arn:aws:s3:::{self.DATA_SOURCE_BUCKET}/*",
                ],
            )
        )
