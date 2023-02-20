import json

from aws_cdk import (
    aws_dynamodb as dynamodb,
    aws_events as events,
    aws_iam as iam,
    aws_pipes as pipes,
    aws_sqs as sqs,
)
from constructs import Construct


class DdbStreamListener(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        event_bus: events.EventBus,
        table: dynamodb.Table,
    ) -> None:
        super().__init__(scope, construct_id)

        pipe_dlq = sqs.Queue(scope=self, id="NewArticleFoundPipeDlq")

        pipe_role = iam.Role(
            scope=self,
            id="NewArticleFoundPipeRole",
            assumed_by=iam.ServicePrincipal(service="pipes.amazonaws.com"),
        )
        table.grant_stream_read(pipe_role)
        event_bus.grant_put_events_to(pipe_role)
        pipe_dlq.grant_send_messages(pipe_role)

        # EventBridge event design:
        ## Name: NewAWSBlogFound
        ## Schema: {
        ##     "$schema": "http://json-schema.org/draft-04/schema#",
        ##     "definitions": {
        ##         "uuid": {
        ##             "type": "string",
        ##             "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        ##         },
        ##         "date_time": {
        ##             "type": "string",
        ##             "pattern": "^(?:[1-9]\\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\\d|2[0-8])|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)|(?:[1-9]\\d(?:0[48]|[2468][048]|[13579][26])|(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d(?:\\.\\d{3}|\\.\\d{6})?(?:Z|[+-][01]\\d(?::?)[0-5]\\d)$",
        ##         },
        ##         "event_metadata": {
        ##             "type": "object",
        ##             "properties": {
        ##                 "event_id": {"$ref": "#/definitions/uuid"},
        ##                 "event_time": {"$ref": "#/definitions/date_time"},
        ##                 "event_version": {"type": "number", "minimum": 1, "maximum": 1},
        ##             },
        ##             "required": ["event_version", "event_time", "event_id"],
        ##         },
        ##         "event_data": {
        ##             "type": "object",
        ##             "properties": {
        ##                 "blog_url": {"type": "string"},
        ##                 "date_created": {"type": "string"},
        ##                 "title": {"type": "string"},
        ##                 "main_category": {"type": "string"},
        ##                 "categories": {"type": "array", "items": {"type": "string"}},
        ##                 "authors": {"type": "array", "items": {"type": "string"}},
        ##                 "date_updated": {"type": "string"},
        ##                 "post_excerpt": {"type": "string"},
        ##                 "featured_image_url": {"type": "string"},
        ##             },
        ##             "required": [
        ##                 "blog_url",
        ##                 "date_created",
        ##                 "title",
        ##                 "main_category",
        ##                 "categories",
        ##                 "authors",
        ##                 "date_updated",
        ##                 "post_excerpt",
        ##             ],
        ##         },
        ##     },
        ##     "type": "object",
        ##     "properties": {
        ##         "data": {"$ref": "#/definitions/event_data"},
        ##         "metadata": {"$ref": "#/definitions/event_metadata"},
        ##     },
        ##     "required": ["data", "metadata"],
        ## }

        pipes.CfnPipe(
            scope=self,
            id="NewArticleFoundPipe",
            role_arn=pipe_role.role_arn,
            source=table.table_stream_arn,
            target=event_bus.event_bus_arn,
            target_parameters=pipes.CfnPipe.PipeTargetParametersProperty(
                event_bridge_event_bus_parameters=pipes.CfnPipe.PipeTargetEventBridgeEventBusParametersProperty(
                    detail_type="NewAWSBlogFound"
                ),
                input_template=(
                    '{"metadata": {"event_id": <$.eventID>,'
                    '"event_time": "<aws.pipes.event.ingestion-time>",'
                    '"event_version": 1},"data": '
                    '{"blog_url": "<$.dynamodb.NewImage.blog_url.S>",'
                    '"date_created": "<$.dynamodb.NewImage.date_created.S>",'
                    '"date_updated": "<$.dynamodb.NewImage.date_updated.S>",'
                    '"title": "<$.dynamodb.NewImage.title.S>",'
                    '"post_excerpt": "<$.dynamodb.NewImage.post_excerpt.S>",'
                    '"main_category": "<$.dynamodb.NewImage.main_category.S>",'
                    '"categories": <$.dynamodb.NewImage.categories.SS>,'
                    '"authors": <$.dynamodb.NewImage.authors.SS>}}'
                ),
            ),
            source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
                dynamo_db_stream_parameters=pipes.CfnPipe.PipeSourceDynamoDBStreamParametersProperty(
                    starting_position="TRIM_HORIZON",
                    batch_size=1,
                    dead_letter_config=pipes.CfnPipe.DeadLetterConfigProperty(
                        arn=pipe_dlq.queue_arn
                    ),
                    maximum_retry_attempts=3,
                ),
                filter_criteria=pipes.CfnPipe.FilterCriteriaProperty(
                    filters=[
                        pipes.CfnPipe.FilterProperty(
                            pattern=json.dumps(
                                {
                                    "eventName": ["INSERT"],
                                    "dynamodb": {"Keys": {"PK": {"S": ["BlogPost"]}}},
                                }
                            )
                        )
                    ]
                ),
            ),
        )
