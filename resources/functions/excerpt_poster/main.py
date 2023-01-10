"""Excerpt Poster Lambda module."""
import json
import os
from typing import List

import boto3
from TwitterAPI import TwitterAPI

table_name = os.environ.get("BLOGS_TABLE")
queue_url = os.environ.get("TWITTER_THREAD_QUEUE")
ddb_client = boto3.client("dynamodb")
sm_client = boto3.client("secretsmanager")


def lambda_handler(event, _context):
    """Run the Lambda function."""
    print(json.dumps(event))

    twitter_api = get_twitter_api()
    for record in event["Records"]:
        handle_blog_post(record["body"], twitter_api)


def get_twitter_api():
    """Retrieve the Twitter Consumer key & secret from AWS Secrets Manager."""
    get_secret_value_response = sm_client.get_secret_value(
        SecretId=os.environ.get("TWITTER_SECRET")
    )
    secret_dict = json.loads(get_secret_value_response["SecretString"])

    return TwitterAPI(
        secret_dict["consumer_key"],
        secret_dict["consumer_secret"],
        secret_dict["access_token_key"],
        secret_dict["access_token_secret"],
    )


def handle_blog_post(sort_key: str, twitter_api: TwitterAPI):
    """Fetch blog post data and post to Twitter."""
    ddb_item = get_ddb_item(sort_key)
    if "excerpt_id" in ddb_item:
        excerpt_id = ddb_item["excerpt_id"]["S"]
        blog_url = ddb_item["blog_url"]["S"]
        raise ValueError(
            f"A excerpt tweet with ID {excerpt_id} was found for blog {blog_url}"
        )

    if "tweet_id" not in ddb_item:
        raise ValueError(f"The item for blog {sort_key} does not have a tweet_id.")

    tweet_id = ddb_item["tweet_id"]["S"]

    twitter_texts = prepare_twitter_texts(ddb_item)
    if not twitter_texts:
        raise ValueError("Got no texts to post")

    tweet_response = send_tweets(twitter_texts, tweet_id, twitter_api)
    update_ddb_item_with_excerpt_tweet_id(sort_key, tweet_response)


def update_ddb_item_with_excerpt_tweet_id(sort_key: str, tweet_response: dict) -> None:
    """Update the item in DDB with the tweet ID."""
    tweet_id = tweet_response["id_str"]
    ddb_client.update_item(
        TableName=table_name,
        Key={"PK": {"S": "BlogPost"}, "SK": {"S": sort_key}},
        AttributeUpdates={"excerpt_id": {"Value": {"S": tweet_id}}},
    )


def send_tweets(twitter_texts: List[str], tweet_id: str, twitter_api: TwitterAPI):
    """Use the Twitter API to send a response to the orginal tweet."""
    first_body = None
    for text in twitter_texts:
        response = twitter_api.request(
            "statuses/update",
            {
                "status": text,
                "in_reply_to_status_id": tweet_id,
                "auto_populate_reply_metadata": True,
            },
        )

        body = response.json()
        print(json.dumps(body))
        if response.status_code != 200:
            error_strs = [f'{x["code"]}: {x["message"]}' for x in body["errors"]]
            errors = f'[{", ".join(error_strs)}]'
            raise Exception(
                f"Post Status failed with status code {response.status_code}. "
                f"Errors: {errors}"
            )
        tweet_id = body["id_str"]
        if not first_body:
            first_body = body

    return first_body


def prepare_twitter_texts(ddb_item):
    """Prepare the text to send, based on content from DDB."""
    excerpt = None
    try:
        excerpt = ddb_item["post_excerpt"]["S"]
    except Exception:  # pylint: disable=broad-except
        pass

    if not excerpt:
        return None

    text = f"Excerpt: {excerpt}"
    max_tweet_length = 280

    text_len = len(text)
    if text_len < max_tweet_length:
        return [text]

    texts = []
    components = text.split(" ")
    print(f"Got {len(components)} components")
    text = ""
    for component in components:
        tmp_text = f"{text} {component} "
        if len(tmp_text) > max_tweet_length - 8:  # 8 for ' [xx/xx]'
            texts.append(f"{text} [{len(texts) + 1}/xx]")
            text = f"… {component}"
        else:
            text = tmp_text.strip()

    texts.append(f"{text} [{len(texts) + 1}/xx]")

    number_of_texts = len(texts)
    print(f"Number of texts: {number_of_texts}")
    for index, text in enumerate(texts):
        print(f"Text {index} length: {len(text)}")
        texts[index] = text.replace("/xx]", f"/{number_of_texts}]")

    print(texts)
    return texts


def get_ddb_item(sort_key: str):
    """Get an item from DDB by PK and SK."""
    response = ddb_client.get_item(
        TableName=table_name,
        Key={"PK": {"S": "BlogPost"}, "SK": {"S": sort_key}},
        ConsistentRead=True,
    )
    return response["Item"]
