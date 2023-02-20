from aws_cdk import (
    Duration,
    aws_events as events,
    aws_ssm as ssm,
    aws_events_targets as events_targets,
    aws_lambda as lambda_,
)
from constructs import Construct


class MastodonPoster(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        event_bus: events.EventBus,
    ) -> None:
        super().__init__(scope, construct_id)

        lambda_layer = lambda_.LayerVersion(
            self,
            "Layer",
            code=lambda_.Code.from_asset("resources/layers/mastodon_poster/python.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
        )

        handler = lambda_.Function(
            self,
            "Function",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("resources/functions/mastodon_poster"),
            handler="index.event_handler",
            environment=dict(),
            layers=[lambda_layer],
            timeout=Duration.seconds(30),
            memory_size=256,
            tracing=lambda_.Tracing.ACTIVE,
        )
        mastodon_access_key_ssm_parameter = (
            ssm.StringParameter.from_secure_string_parameter_attributes(
                scope=self,
                id="SecureString",
                parameter_name="mastodon_awsblogs_access_token",
            )
        )
        mastodon_access_key_ssm_parameter.grant_read(handler)

        events.Rule(
            scope=self,
            id="MastodonRule",
            event_bus=event_bus,
            event_pattern=events.EventPattern(detail_type=["NewAWSBlogFound"]),
            targets=[events_targets.LambdaFunction(handler=handler)],
        )
