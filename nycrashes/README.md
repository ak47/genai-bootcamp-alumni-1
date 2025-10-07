# AWS CDK Project

[CDK Python Reference](https://docs.aws.amazon.com/cdk/api/v2/python/)

To deploy to your AWS account, run:

```bash
uv run cdk deploy
```

Ensure that you have [AWS CLI configured](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html).
You can test your AWS session by running

```bash
aws sts get-caller-identity
```

If this returns a JSON object with a `UserId`, an `Account` and an `Arn`, you're good to go.
