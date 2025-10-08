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

## Infrastructure Overview

```mermaid
graph TD
    User[End User] --> CF[CloudFront Distribution]
    CF -->|Static assets| FrontendBucket[(Frontend S3 Bucket)]
    CF -->|/api/*| ChatUrl[Lambda Function URL]
    ChatUrl --> ChatLambda[Chat Lambda (Docker)]
    ChatLambda --> SessionBucket[(Chat Session Bucket)]
    ChatLambda --> Secret[(Aurora Secret)]
    ChatLambda -->|RDS Data API| Aurora[(Aurora PostgreSQL Serverless v2)]
    subgraph VPC["VPC (2 AZs with public & private subnets)"]
        Aurora
        PopLambda[Populator Lambda]
        S3Endpoint[S3 Gateway Endpoint]
    end
    PopResource[PopulateDatabase Custom Resource] --> PopLambda
    PopLambda --> SourceBucket[(Source Data Bucket)]
    SourceBucket -.-> ExternalData[(nyc-crashdata dataset)]
    PopLambda -->|Imports CSV| Aurora
```

- `FrontendBucket` serves the built SvelteKit site through CloudFront, which also forwards `/api/*` calls to the publicly exposed Lambda Function URL.
- `ChatLambda` streams responses while persisting session state in `Chat Session Bucket`, reading credentials from Secrets Manager, and accessing Aurora via the Data API.
- `Populator Lambda` runs inside the VPC on deployment, staging the raw CSV in `Source Data Bucket` before importing it into Aurora; it reaches the public `nyc-crashdata` dataset over the internet and uses the S3 gateway endpoint for private subnet access.
