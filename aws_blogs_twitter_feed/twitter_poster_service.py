"""Twitter Poster Service module."""

from constructs import Construct
from aws_cdk import (
    aws_lambda_event_sources as lambda_event_sources,
    aws_lambda as lambda_,
)


class TwitterPosterService(Construct):
    """TwitterPosterService class, responsible for posting tweets."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        resources: dict,
    ) -> None:
        """Construct a new TwitterPosterService."""
        super().__init__(scope, construct_id)

        lambda_layer = lambda_.LayerVersion(
            self,
            "TwitterPostLambdaLayer",
            code=lambda_.Code.from_asset("resources/layers/twitter_poster/layer.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_9],
        )

        handler = lambda_.Function(
            self,
            "TwitterPostFunction",
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset("resources/functions/twitter_poster"),
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
            queue=resources["twitter_post_queue"],
            batch_size=1,
        )
        handler.add_event_source(sqs_event_source)

        resources["table"].grant_read_write_data(handler)
        resources["twitter_thread_queue"].grant_send_messages(handler)
        resources["twitter_post_queue"].grant_consume_messages(handler)
        resources["twitter_secret"].grant_read(handler)
