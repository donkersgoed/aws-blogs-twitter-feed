"""Blog fetcher Lambda module."""
import hashlib
import html
import json
import os
from typing import List
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

import boto3
import requests
import yaml

MAX_BLOG_PAGES = 6
HTTP_BLOG_URL = (
    'https://aws.amazon.com/api/dirs/items/search'
    '?item.directoryId=blog-posts&sort_by=item.additionalFields.createdDate'
    '&sort_order=desc&size=10&item.locale=en_US'
)

table_name = os.environ.get('BLOGS_TABLE')
queue_url = os.environ.get('TWITTER_POST_QUEUE')
ddb_client = boto3.client('dynamodb')
ssm_client = boto3.client('ssm')
sqs_client = boto3.client('sqs')


def lambda_handler(event, _context):
    """Run the Lambda function."""
    print(json.dumps(event))

    latest_blog_in_ddb = fetch_latest_item()
    aws_blogs = retrieve_blogs_from_aws(latest_blog_in_ddb)
    aws_blogs.reverse()
    store_blogs_in_ddb_and_sqs(aws_blogs)


def store_blogs_in_ddb_and_sqs(aws_blogs):
    """Store the blog entries in DynamoDB. When successful, send it to SQS."""
    print(f'Storing {len(aws_blogs)} items in DDB and SQS.')
    for blog in aws_blogs:
        try:
            date_created = blog['date_created']
            item_unique_id = hashlib.md5(blog['item_url'].encode()).hexdigest()
            sort_key = f'{date_created}#{item_unique_id}'
            store_blog_in_ddb(blog)
            send_sort_key_to_sqs(sort_key)
        except Exception as exc:  # pylint:disable=broad-except
            print(exc)  # Print the exception and continue to the next blog


def store_blog_in_ddb(blog: dict):
    """Take a dictionary and store it in DynamoDB."""
    item_url = blog.get('item_url')
    main_category = blog.get('main_category')
    date_created = blog.get('date_created')
    item_unique_id = hashlib.md5(item_url.encode()).hexdigest()
    authors = blog.get('authors')

    ddb_item = {
        'blog_url': {'S': item_url},
        'date_created': {'S': date_created},
        'title': {'S': blog.get('title')},
        'main_category': {'S': main_category},
        'categories': {'SS': blog.get('categories')},
        'authors': {'SS': authors},
        'date_updated': {'S': blog.get('date_updated')},
    }

    if blog.get('featured_image_url'):
        ddb_item['featured_image_url'] = {'S': blog.get('featured_image_url')}
    else:
        ddb_item['featured_image_url'] = {'NULL': True}

    if blog.get('post_excerpt'):
        ddb_item['post_excerpt'] = {'S': blog.get('post_excerpt')}
    else:
        ddb_item['post_excerpt'] = {'NULL': True}

    try:
        ddb_client.put_item(
            TableName=table_name,
            Item={
                'PK': {'S': 'BlogPost'},
                'SK': {'S': f'{date_created}#{item_unique_id}'},
                **ddb_item
            },
            ConditionExpression='attribute_not_exists(PK) AND attribute_not_exists(SK)'
        )
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'ConditionalCheckFailedException':
            print(f'Tried to insert an item that already exists in table v2: {item_url}')
        else:
            raise exc

    for author in authors:
        try:
            ddb_client.put_item(
                TableName=table_name,
                Item={
                    'PK': {'S': 'Author'},
                    'SK': {'S': author},
                },
                ConditionExpression='attribute_not_exists(PK) AND attribute_not_exists(SK)'
            )
        except ClientError as exc:
            if exc.response['Error']['Code'] == 'ConditionalCheckFailedException':
                pass
            else:
                print(f'Got ClientError while adding Author {author}: {exc}')


def send_sort_key_to_sqs(sort_key: str):
    """Send the Sort Key to SQS for further processing."""
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=sort_key,
        MessageGroupId=sort_key,
        MessageDeduplicationId=sort_key,
    )


def retrieve_blogs_from_aws(latest_blog_in_ddb, page=0):
    """
    Retrieve blogs from the AWS API.

    The function stops when the latest_blog_in_ddb is encountered or when
    the max number of pages has been reached.
    """
    if page >= MAX_BLOG_PAGES:
        return {}

    api_url = HTTP_BLOG_URL + f'&page={page}'
    response = requests.get(api_url)
    blog_data = response.json()

    parsed_items = []
    for item in blog_data['items']:
        blog_item = item['item']
        additional_fields = blog_item['additionalFields']

        categories = []
        for tag in item['tags']:
            if tag['tagNamespaceId'] == 'blog-posts#category':
                description = json.loads(tag['description'])
                if not description['name'].startswith('*'):
                    categories.append(html.unescape(description['name']))

        try:
            item_url = additional_fields['link']
            parsed_items.append({
                'item_url': item_url,
                'title': html.unescape(additional_fields['title']),
                'main_category': lookup_category(item_url, categories),
                'categories': categories,
                'post_excerpt': html.unescape(additional_fields.get('postExcerpt', '')),
                'featured_image_url': additional_fields.get('featuredImageUrl'),
                'authors': html.unescape(json.loads(blog_item['author'])),
                'date_created': blog_item['dateCreated'],
                'date_updated': blog_item['dateUpdated'],
            })
        except KeyError:
            continue

    if (
        not latest_blog_in_ddb or
        latest_blog_in_ddb not in [x['item_url'] for x in parsed_items]
    ):
        parsed_items += retrieve_blogs_from_aws(latest_blog_in_ddb, page+1)
    elif latest_blog_in_ddb in [x['item_url'] for x in parsed_items]:
        latest_blog_index = next((
            index for (index, d) in
            enumerate(parsed_items)
            if d['item_url'] == latest_blog_in_ddb
        ), None)
        return parsed_items[0:latest_blog_index]

    return parsed_items


def lookup_category(item_url: str, categories: List[str]):
    """Lookup the main category from the URL. If none is found, use the first category in tags."""
    with open('category_mapping.yaml') as category_mapping_file:
        category_mapping = yaml.load(category_mapping_file, Loader=yaml.FullLoader)

    url_path_components = item_url.split('/')
    blog_category_id = url_path_components[4]
    try:
        return category_mapping[blog_category_id]
    except KeyError:
        pass

    if categories:
        return categories[0]


def fetch_latest_item():
    """Fetch the last processed blog post from the SSM Parameter Store."""
    latest_item = None
    try:
        posts_table = boto3.resource('dynamodb').Table(table_name)
        response = posts_table.query(
            KeyConditionExpression=Key('PK').eq('BlogPost'),
            ConsistentRead=True,
            ScanIndexForward=False,
            Limit=1
        )
        latest_item = response['Items'][0]['blog_url']
        print(f'Got latest item from DDB: {latest_item}')
    except Exception as exc:  # pylint: disable=broad-except
        print(f'Failed to fetch latest item: {exc}')

    return latest_item
