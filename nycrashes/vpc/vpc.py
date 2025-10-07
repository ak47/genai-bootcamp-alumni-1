from constructs import Construct
import aws_cdk.ec2 as ec2


class VPC(Construct):
    def __init__(self, scope: "Construct", id: str) -> None:
        super().__init__(scope, id)

        """
            TODO: Create a VPC with public and private subnets and a single NAT Gateway
        """
