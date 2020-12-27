"""Excerpt Excerpt Service module."""
from aws_cdk import (
    core,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_secretsmanager as secretsmanager,
)


class ExcerptPosterService(core.Construct):
    """ExcerptPosterService class, responsible for posting excerpts in reply to tweets."""

    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        table: dynamodb.Table,
        twitter_thread_queue: sqs.Queue,
        twitter_secret: secretsmanager.Secret
    ) -> None:
        """Construct a new ExcerptPosterService."""
        super().__init__(scope, construct_id)

        lambda_layer = lambda_.LayerVersion(
            self,
            'ExcerptPostLambdaLayer',
            code=lambda_.Code.asset('resources/layers/excerpt_poster/layer.zip'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
        )

        handler = lambda_.Function(
            self,
            'ExcerptPostFunction',
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.asset('resources/functions/excerpt_poster'),
            handler='main.lambda_handler',
            environment=dict(
                BLOGS_TABLE=table.table_name,
                TWITTER_SECRET=twitter_secret.secret_name,
                TWITTER_THREAD_QUEUE=twitter_thread_queue.queue_url,
            ),
            layers=[lambda_layer],
            tracing=lambda_.Tracing.ACTIVE
        )

        # SQS Event Source
        sqs_event_source = lambda_event_sources.SqsEventSource(
            queue=twitter_thread_queue
        )
        handler.add_event_source(sqs_event_source)

        table.grant_read_write_data(handler)
        twitter_thread_queue.grant_consume_messages(handler)
        twitter_secret.grant_read(handler)
