"""AWS Blogs Twitter Feed stack module."""

from aws_cdk import (
    core,
    aws_dynamodb as dynamodb,
    aws_sqs as sqs
)

from . import blog_fetcher_service
from . import twitter_poster_service

class AwsBlogsTwitterFeedStack(core.Stack):
    """Main AWS Blogs Twitter Feed CloudFormation stack."""

    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        **kwargs
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

        blog_fetcher_service.BlogFetcherService(
            self, "BlogFetcher",
            table=blogs_table,
            queue=twitter_post_queue
        )

        twitter_poster_service.TwitterPosterService(
            self, "TwitterPoster",
            table=blogs_table,
            queue=twitter_post_queue
        )
