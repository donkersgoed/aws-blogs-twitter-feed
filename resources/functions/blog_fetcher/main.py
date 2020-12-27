"""Blog fetcher Lambda module."""
import hashlib
import html
import json
import os
from typing import List

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
    last_stored_blog_url = None
    for blog in aws_blogs:
        try:
            blog_url = blog.get('item_url')
            store_blog_in_ddb(blog)
            send_url_to_sqs(blog_url)
            last_stored_blog_url = blog_url
        except Exception as exc:  # pylint:disable=broad-except
            print(exc)  # Print the exception and continue to the next blog

    set_last_blog_item(last_stored_blog_url)


def store_blog_in_ddb(blog: dict):
    """Take a dictionary and store it in DynamoDB."""
    ddb_item = {
        'blog_url': {'S': blog.get('item_url')},
        'date_created': {'S': blog.get('date_created')},
        'title': {'S': blog.get('title')},
        'main_category': {'S': blog.get('main_category')},
        'categories': {'SS': blog.get('categories')},
        'authors': {'SS': blog.get('authors')},
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

    ddb_client.put_item(
        TableName=table_name,
        Item=ddb_item,
        ConditionExpression='attribute_not_exists(blog_url)'
    )


def send_url_to_sqs(link_url: str):
    """Send the URL to SQS for further processing."""
    link_md5 = hashlib.md5(link_url.encode()).hexdigest()
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=link_url,
        MessageGroupId=link_md5,
        MessageDeduplicationId=link_md5,
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
                'post_excerpt': html.unescape(additional_fields.get('postExcerpt')),
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
    response = ssm_client.get_parameter(
        Name=os.environ.get('LAST_POST_PARAMETER')
    )

    return response['Parameter']['Value']


def set_last_blog_item(blog_url):
    """Store the last processed blog post in the SSM Parameter Store."""
    if blog_url:
        ssm_client.put_parameter(
            Name=os.environ.get('LAST_POST_PARAMETER'),
            Value=blog_url,
            Overwrite=True
        )
