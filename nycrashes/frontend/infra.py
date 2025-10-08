from constructs import Construct

class Frontend(Construct):
    """
    TODO: S3 bucket to store frontend.

    Use s3deploy.BucketDeployment to deploy the frontend to the bucket.
    Build with node:22 image and use "npm ci && npm run build && cp -r build/* /asset-output/"
    """

    """
    TODO: CloudFront distribution

    Create a CloudFornt distribution to serve the frontend from the S3 bucket.
    Configure appropriate permissions to allow CloudFront to read from the S3 bucket.
    For now we will use the standard CloudFront provided hostname.
    """

    """
    TODO: Route to Backend

    Make the backend available at /api/
    """

