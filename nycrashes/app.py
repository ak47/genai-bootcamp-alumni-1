#!/usr/bin/env python3
import os

import aws_cdk as cdk

from backend.infra import Backend
from frontend.infra import Frontend
from vpc.vpc import VPC


app = cdk.App()
env = cdk.Environment(
    account=os.getenv("CDK_DEFAULT_ACCOUNT"),
    region=os.getenv("CDK_DEFAULT_REGION"),
)

infrastructure_stack = cdk.Stack(app, "NycrashesBackendStack", env=env)

network = VPC(infrastructure_stack, "Vpc")
backend = Backend(infrastructure_stack, "Backend", vpc=network.vpc)
Frontend(
    infrastructure_stack,
    "Frontend",
    backend_url=backend.chat_function_url.url,
)

app.synth()
