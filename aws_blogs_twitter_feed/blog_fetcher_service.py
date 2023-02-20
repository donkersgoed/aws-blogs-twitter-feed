"""Blog Fetcher Service module."""

from constructs import Construct
from aws_cdk import (
    Duration,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_events_targets as events_targets,
    aws_lambda as lambda_,
    aws_sqs as sqs,
)


class BlogFetcherService(Construct):
    """BlogFetcherService class, responsible for fetching blogs at AWS."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        table: dynamodb.Table,
        twitter_post_queue: sqs.Queue,
    ) -> None:
        """Construct a new BlogFetcherService."""
        super().__init__(scope, construct_id)

        lambda_layer = lambda_.LayerVersion(
            self,
            "BlogFetcherLambdaLayer",
            code=lambda_.Code.from_asset("resources/layers/blog_fetcher/python.zip"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
        )

        handler = lambda_.Function(
            self,
            "BlogFetcherFunction",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset("resources/functions/blog_fetcher"),
            handler="main.lambda_handler",
            environment=dict(
                BLOGS_TABLE=table.table_name,
                TWITTER_POST_QUEUE=twitter_post_queue.queue_url,  # deprecated
            ),
            layers=[lambda_layer],
            timeout=Duration.seconds(30),
            memory_size=256,
            tracing=lambda_.Tracing.ACTIVE,
        )

        lambda_schedule = events.Schedule.rate(Duration.minutes(1))
        event_lambda_target = events_targets.LambdaFunction(handler=handler)
        events.Rule(
            self,
            "BlogFetcherEvent",
            description="Scan for new blogs every minute",
            enabled=True,
            schedule=lambda_schedule,
            targets=[event_lambda_target],
        )

        table.grant_read_write_data(handler)
        twitter_post_queue.grant_send_messages(handler)
