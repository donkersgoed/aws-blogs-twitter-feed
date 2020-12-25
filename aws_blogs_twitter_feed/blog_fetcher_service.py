"""Blog Fetcher Service module."""

from aws_cdk import (
    core,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_events as events,
    aws_events_targets as events_targets,
)


class BlogFetcherService(core.Construct):
    """BlogFetcherService class, responsible for fetching blogs at AWS."""

    def __init__(
        self,
        scope: core.Construct,
        id: str,  # pylint:disable=redefined-builtin
        table: dynamodb.Table,
        queue: sqs.Queue,
    ) -> None:
        """Construct a new BlogFetcherService."""
        super().__init__(scope, id)

        lambda_layer = lambda_.LayerVersion(
            self,
            'BlogFetcherLambdaLayer',
            code=lambda_.Code.asset('resources/layers/blog_fetcher/layer.zip'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
        )

        handler = lambda_.Function(
            self,
            'BlogFetcherFunction',
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.asset('resources/functions/blog_fetcher'),
            handler='main.lambda_handler',
            environment=dict(
                BLOGS_TABLE=table.table_name,
                TWITTER_POST_QUEUE=queue.queue_url
            ),
            layers=[lambda_layer],
            timeout=core.Duration.seconds(10),
            tracing=lambda_.Tracing.ACTIVE
        )

        lambda_schedule = events.Schedule.rate(core.Duration.minutes(1))
        event_lambda_target = events_targets.LambdaFunction(handler=handler)
        events.Rule(
            self,
            'BlogFetcherEvent',
            description='Scan for new blogs every minute',
            enabled=True,
            schedule=lambda_schedule,
            targets=[event_lambda_target]
        )

        table.grant_read_write_data(handler)
        queue.grant_send_messages(handler)
