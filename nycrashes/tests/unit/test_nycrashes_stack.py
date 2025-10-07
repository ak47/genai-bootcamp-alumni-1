import aws_cdk as core
import aws_cdk.assertions as assertions

from nycrashes.nycrashes_stack import NycrashesStack

# example tests. To run these tests, uncomment this file along with the example
# resource in nycrashes/nycrashes_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = NycrashesStack(app, "nycrashes")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
