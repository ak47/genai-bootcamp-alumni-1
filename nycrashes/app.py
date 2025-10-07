#!/usr/bin/env python3
import os

import aws_cdk as cdk

from backend.infra import Backend
from vpc.vpc import VPC


app = cdk.App()
env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

infrastructure_stack = cdk.Stack(app, "NycrashesBackendStack", env=env)

network = VPC(infrastructure_stack, "Vpc")
Backend(infrastructure_stack, "Backend", vpc=network.vpc)

app.synth()
