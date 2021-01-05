"""Twitter Poster Lambda module."""
import hashlib
import json
import os
from typing import List

import boto3
import yaml
from TwitterAPI import TwitterAPI

table_name = os.environ.get('BLOGS_TABLE')
queue_url = os.environ.get('TWITTER_THREAD_QUEUE')
ddb_client = boto3.client('dynamodb')
sm_client = boto3.client('secretsmanager')
sqs_client = boto3.client('sqs')


def lambda_handler(event, _context):
    """Run the Lambda function."""
    print(json.dumps(event))

    twitter_api = get_twitter_api()
    for record in event['Records']:
        handle_blog_post(record['body'], twitter_api)


def get_twitter_api():
    """Retrieve the Twitter Consumer key & secret from AWS Secrets Manager."""
    get_secret_value_response = sm_client.get_secret_value(
        SecretId=os.environ.get('TWITTER_SECRET')
    )
    secret_dict = json.loads(get_secret_value_response['SecretString'])

    return TwitterAPI(
        secret_dict['consumer_key'],
        secret_dict['consumer_secret'],
        secret_dict['access_token_key'],
        secret_dict['access_token_secret']
    )


def send_url_to_tweet_thread_sqs(link_url: str):
    """Send the URL to SQS for further processing."""
    link_md5 = hashlib.md5(link_url.encode()).hexdigest()
    sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=link_url,
        MessageGroupId=link_md5,
        MessageDeduplicationId=link_md5,
        DelaySeconds=30  # Delay this message for 30 seconds so Twitter can catch up.
    )


def handle_blog_post(blog_url: str, twitter_api: TwitterAPI):
    """Fetch blog post data and post to Twitter."""
    ddb_item = get_ddb_item(blog_url)
    if 'tweet_id' in ddb_item:
        tweet_id = ddb_item['tweet_id']['S']
        blog_url = ddb_item['blog_url']['S']
        raise ValueError(f'A tweet with ID {tweet_id} was found for blog {blog_url}')

    twitter_text = prepare_twitter_text(ddb_item)
    tweet_response = send_tweet(twitter_text, twitter_api)
    update_ddb_item_with_tweet_id(blog_url, tweet_response)
    send_url_to_tweet_thread_sqs(blog_url)


def update_ddb_item_with_tweet_id(blog_url: str, tweet_response: dict) -> None:
    """Update the item in DDB with the tweet ID."""
    tweet_id = tweet_response['id_str']
    ddb_client.update_item(
        TableName=table_name,
        Key={'blog_url': {'S': blog_url}},
        AttributeUpdates={
            'tweet_id': {
                'Value': {'S': tweet_id}
            }
        }
    )


def send_tweet(twitter_text: str, twitter_api: TwitterAPI):
    """Use the Twitter API to send a tweet."""
    response = twitter_api.request('statuses/update', {'status': twitter_text})
    body = response.json()
    if response.status_code != 200:
        error_strs = [f'{x["code"]}: {x["message"]}' for x in body['errors']]
        errors = f'[{", ".join(error_strs)}]'
        raise Exception(
            f'Post Status failed with status code {response.status_code}. '
            f'Errors: {errors}'
        )
    return body


def prepare_twitter_text(ddb_item):
    """Prepare the text to send, based on content from DDB."""
    main_category = ddb_item['main_category']['S']
    title = ddb_item['title']['S']
    blog_url = ddb_item['blog_url']['S']
    authors = prepare_authors(ddb_item['authors']['SS'])

    base = f'New {main_category} post by {authors}:\n\n'
    max_len = 280  # max length of a tweet
    base_len = len(base)  # length of the base text
    url_len = 24  # length of the URL (newline + 23)
    rest_len_for_title = max_len - base_len - url_len  # space left for title
    title_len = len(title)  # actual length of title

    removed_chars = 0
    shortened_suffix = ' […]'
    while (
        (title_len - removed_chars) >
        rest_len_for_title + len(shortened_suffix)
    ):
        components = title.rsplit(' ', 1)
        title = components[0]
        title_len = len(title)

    if removed_chars > 0:
        title += shortened_suffix
        print(f'Shortened title: {title}')

    return f'{base}{title}\n{blog_url}'


def prepare_authors(authors: List[str]):
    """Take a list of authors, convert them to twitter handles, add commas."""
    with open('twitter_handles.yaml') as twitter_handles_file:
        twitter_handles = yaml.load(
            twitter_handles_file,
            Loader=yaml.FullLoader
        )

    mapped_authors = []
    for author in authors:
        if author in twitter_handles.keys():
            mapped_authors.append(twitter_handles[author])
        else:
            mapped_authors.append(author)

    authors_string = ''
    number_of_authors = len(authors)
    for index, author in enumerate(mapped_authors):
        authors_string += author
        if index < number_of_authors - 2:
            authors_string += ', '
        elif index == number_of_authors - 2:
            authors_string += ' and '
    return authors_string


def get_ddb_item(blog_url: str):
    """Get an item from DDB by primary key."""
    response = ddb_client.get_item(
        TableName=table_name,
        Key={'blog_url': {'S': blog_url}},
        ConsistentRead=True
    )
    return response['Item']
