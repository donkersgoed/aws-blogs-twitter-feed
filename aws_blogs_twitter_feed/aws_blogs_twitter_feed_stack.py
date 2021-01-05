"""AWS Blogs Twitter Feed stack module."""
from typing import Any

from aws_cdk import (
    core,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs,
    aws_secretsmanager as secretsmanager,
)

from . import blog_fetcher_service
from . import twitter_poster_service
from . import excerpt_poster_service


class AwsBlogsTwitterFeedStack(core.Stack):
    """Main AWS Blogs Twitter Feed CloudFormation stack."""

    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        **kwargs: Any
    ) -> None:
        """Constuct a new AwsBlogsTwitterFeedStack."""
        super().__init__(scope, construct_id, **kwargs)

        blogs_table = dynamodb.Table(
            self, 'BlogsTable',
            partition_key=dynamodb.Attribute(
                name='blog_url',
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        twitter_secret = secretsmanager.Secret(
            self,
            'TwitterSecret'
        )

        twitter_post_dlq = sqs.Queue(
            self, 'TwitterPostDLQ',
            fifo=True
        )

        twitter_post_queue = sqs.Queue(
            self, 'TwitterPostQueue',
            fifo=True,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=twitter_post_dlq
            )
        )

        twitter_thread_dlq = sqs.Queue(
            self, 'TwitterThreadDLQ',
            fifo=True
        )

        twitter_thread_queue = sqs.Queue(
            self, 'TwitterThreadQueue',
            fifo=True,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,
                queue=twitter_thread_dlq
            ),
            delivery_delay=core.Duration.seconds(15)
        )

        blog_fetcher_service.BlogFetcherService(
            self, 'BlogFetcher',
            table=blogs_table,
            twitter_post_queue=twitter_post_queue
        )

        twitter_poster_service.TwitterPosterService(
            self, 'TwitterPoster',
            resources={
                'table': blogs_table,
                'twitter_post_queue': twitter_post_queue,
                'twitter_thread_queue': twitter_thread_queue,
                'twitter_secret': twitter_secret,
            }
        )

        excerpt_poster_service.ExcerptPosterService(
            self, 'ExcerptPoster',
            resources={
                'table': blogs_table,
                'twitter_thread_queue': twitter_thread_queue,
                'twitter_secret': twitter_secret,
            }
        )
