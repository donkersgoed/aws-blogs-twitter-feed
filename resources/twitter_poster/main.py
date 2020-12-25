"""Twitter Poster Lambda module."""
import json
import os
from typing import List

import boto3
import yaml
from TwitterAPI import TwitterAPI

table_name = os.environ.get('BLOGS_TABLE')
ddb_client = boto3.client('dynamodb')
sm_client = boto3.client('secretsmanager')


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


def handle_blog_post(blog_url: str, twitter_api: TwitterAPI):
    """Fetch blog post data and post to Twitter."""
    ddb_item = get_ddb_item(blog_url)
    twitter_text = prepare_twitter_text(ddb_item)
    send_tweet(twitter_text, twitter_api)


def send_tweet(twitter_text: str, twitter_api: TwitterAPI):
    """Use the Twitter API to send a tweet."""
    twitter_api.request('statuses/update', {'status': twitter_text})


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
        Key={'blog_url': {'S': blog_url}}
    )
    return response['Item']
