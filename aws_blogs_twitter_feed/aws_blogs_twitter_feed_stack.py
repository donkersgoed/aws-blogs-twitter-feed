"""AWS Blogs Twitter Feed stack module."""
from typing import Any

from aws_cdk import Stack
from constructs import Construct
from aws_cdk import (
    Duration,
    Fn,
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_sqs as sqs,
    aws_secretsmanager as secretsmanager,
)


from . import blog_fetcher_service
from . import twitter_poster_service
from . import excerpt_poster_service
from .app_constructs.ddb_stream_listener import DdbStreamListener
from .app_constructs.mastodon_poster import MastodonPoster


class AwsBlogsTwitterFeedStack(Stack):
    """Main AWS Blogs Twitter Feed CloudFormation stack."""

    def __init__(self, scope: Construct, construct_id: str, **kwargs: Any) -> None:
        """Constuct a new AwsBlogsTwitterFeedStack."""
        super().__init__(scope, construct_id, **kwargs)

        blogs_table = dynamodb.Table(
            self,
            "BlogsTableV2",
            partition_key=dynamodb.Attribute(
                name="PK", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            point_in_time_recovery=True,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
        )

        blogs_table.add_global_secondary_index(
            index_name="main_category_idx",
            partition_key=dynamodb.Attribute(
                name="main_category", type=dynamodb.AttributeType.STRING
            ),
        )

        event_bus = events.EventBus(
            scope=self, id="EventBus", event_bus_name="aws_blogs_event_bus"
        )
        events.Archive(
            scope=self,
            id="EventBusArchive",
            source_event_bus=event_bus,
            retention=Duration.days(30),
            event_pattern=events.EventPattern(account=[Fn.ref("AWS::AccountId")]),
        )

        DdbStreamListener(
            scope=self,
            construct_id="DdbStreamListener",
            event_bus=event_bus,
            table=blogs_table,
        )

        MastodonPoster(scope=self, construct_id="MastodonPoster", event_bus=event_bus)

        twitter_secret = secretsmanager.Secret(self, "TwitterSecret")

        twitter_post_dlq = sqs.Queue(self, "TwitterPostDLQ", fifo=True)

        twitter_post_queue = sqs.Queue(
            self,
            "TwitterPostQueue",
            fifo=True,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=twitter_post_dlq
            ),
        )

        twitter_thread_dlq = sqs.Queue(self, "TwitterThreadDLQ", fifo=True)

        twitter_thread_queue = sqs.Queue(
            self,
            "TwitterThreadQueue",
            fifo=True,
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3, queue=twitter_thread_dlq
            ),
            delivery_delay=Duration.seconds(15),
        )

        blog_fetcher_service.BlogFetcherService(
            self,
            "BlogFetcher",
            table=blogs_table,
            twitter_post_queue=twitter_post_queue,
        )

        twitter_poster_service.TwitterPosterService(
            self,
            "TwitterPoster",
            resources={
                "table": blogs_table,
                "twitter_post_queue": twitter_post_queue,
                "twitter_thread_queue": twitter_thread_queue,
                "twitter_secret": twitter_secret,
            },
        )

        excerpt_poster_service.ExcerptPosterService(
            self,
            "ExcerptPoster",
            resources={
                "table": blogs_table,
                "twitter_thread_queue": twitter_thread_queue,
                "twitter_secret": twitter_secret,
            },
        )
