"""Excerpt Excerpt Service module."""

from constructs import Construct
from aws_cdk import (
    aws_lambda_event_sources as lambda_event_sources,
    aws_lambda as lambda_,
)


class ExcerptPosterService(Construct):
    """ExcerptPosterService class, responsible for posting excerpts in reply to tweets."""

    def __init__(self, scope: Construct, construct_id: str, resources: dict) -> None:
        """Construct a new ExcerptPosterService."""
        super().__init__(scope, construct_id)

        lambda_layer = lambda_.LayerVersion(
            self,
            "ExcerptPostLambdaLayer",
            code=lambda_.Code.from_asset("resources/layers/excerpt_poster/layer.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
        )

        handler = lambda_.Function(
            self,
            "ExcerptPostFunction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset("resources/functions/excerpt_poster"),
            handler="main.lambda_handler",
            environment=dict(
                BLOGS_TABLE=resources["table"].table_name,
                TWITTER_SECRET=resources["twitter_secret"].secret_name,
                TWITTER_THREAD_QUEUE=resources["twitter_thread_queue"].queue_url,
            ),
            layers=[lambda_layer],
            tracing=lambda_.Tracing.ACTIVE,
        )

        # SQS Event Source
        sqs_event_source = lambda_event_sources.SqsEventSource(
            queue=resources["twitter_thread_queue"],
            batch_size=1,
        )
        handler.add_event_source(sqs_event_source)

        resources["table"].grant_read_write_data(handler)
        resources["twitter_thread_queue"].grant_consume_messages(handler)
        resources["twitter_secret"].grant_read(handler)
