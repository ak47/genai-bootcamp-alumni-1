from __future__ import annotations

from urllib.parse import urlsplit

from aws_cdk import BundlingOptions, CfnOutput, DockerImage, RemovalPolicy, Token, Fn
import aws_cdk.aws_cloudfront as cloudfront
import aws_cdk.aws_cloudfront_origins as origins
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3deploy
from constructs import Construct


class Frontend(Construct):
    def __init__(self, scope: Construct, construct_id: str, *, backend_url: str) -> None:
        super().__init__(scope, construct_id)

        self.bucket = s3.Bucket(
            self,
            "FrontendBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            versioned=True,
            auto_delete_objects=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        origin_access_identity = cloudfront.OriginAccessIdentity(self, "FrontendOAI")
        self.bucket.grant_read(origin_access_identity.grant_principal)

        frontend_origin = origins.S3Origin(
            self.bucket,
            origin_access_identity=origin_access_identity,
        )

        backend_origin_kwargs: dict[str, object] = {
            "protocol_policy": cloudfront.OriginProtocolPolicy.HTTPS_ONLY,
        }

        if not Token.is_unresolved(backend_url):
            parsed_backend = urlsplit(backend_url)
            if parsed_backend.scheme != "https" or not parsed_backend.netloc:
                msg = "backend_url must be an absolute https:// URL"
                raise ValueError(msg)
            if parsed_backend.path and parsed_backend.path != "/":
                backend_origin_kwargs["origin_path"] = parsed_backend.path.rstrip("/")

        backend_origin = origins.HttpOrigin(Fn.parse_domain_name(backend_url), **backend_origin_kwargs)

        self.distribution = cloudfront.Distribution(
            self,
            "FrontendDistribution",
            default_root_object='index.html',
            default_behavior=cloudfront.BehaviorOptions(
                origin=frontend_origin,
                viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                allowed_methods=cloudfront.AllowedMethods.ALLOW_GET_HEAD_OPTIONS,
            ),
            additional_behaviors={
                "/api/*": cloudfront.BehaviorOptions(
                    origin=backend_origin,
                    allowed_methods=cloudfront.AllowedMethods.ALLOW_ALL,
                    cache_policy=cloudfront.CachePolicy.CACHING_DISABLED,
                    origin_request_policy=cloudfront.OriginRequestPolicy.ALL_VIEWER_EXCEPT_HOST_HEADER,
                    viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
                ),
            },
        )

        bundled_frontend = s3deploy.Source.asset(
            "frontend/src",
            bundling=BundlingOptions(
                image=DockerImage.from_registry("node:22"),
                command=[
                    "bash",
                    "-c",
                    "npm ci && npm run build && cp -r build/* /asset-output/",
                ],
                user="root",
            ),
        )

        s3deploy.BucketDeployment(
            self,
            "DeployFrontend",
            sources=[bundled_frontend],
            destination_bucket=self.bucket,
            distribution=self.distribution,
            distribution_paths=["/*"],
        )

        CfnOutput(
            self,
            "DistributionUrl",
            value=f"https://{self.distribution.distribution_domain_name}",
            description="URL of the CloudFront distribution serving the frontend.",
        )
