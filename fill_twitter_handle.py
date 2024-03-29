"""Tool to add twitter handles to known AWS Blog Authors."""
import argparse
import boto3
from boto3.dynamodb.conditions import Key, Attr

table_name = "aws-blogs-twitter-feed-BlogsTableV24493143C-18E0LOVY9EAQ4"
dynamodb = boto3.resource("dynamodb")
table_resource = dynamodb.Table(table_name)


def fetch_and_fill(author=None):
    """Fetch the first author with unknown Twitter handle."""
    author_no_details = get_last_author_without_details(author)
    if not author_no_details:
        raise RuntimeError("No authors to update")

    author_name = author_no_details["SK"]
    print(author_name)

    has_handle = None
    while has_handle is None:
        response = input(f"Does '{author_name}' have a twitter handle (y/n)? ")
        if response == "y":
            has_handle = True
        if response == "n":
            has_handle = False

    if has_handle:
        twitter_handle = input(f"What is the twitter handle for {author_name}?\n")
        if not twitter_handle.startswith("@"):
            twitter_handle = f"@{twitter_handle}"
        update_author({**author_no_details, "twitter_handle": twitter_handle})
    else:
        update_author({**author_no_details, "has_twitter": False})


def get_last_author_without_details(author=None):
    """Loop over DynamoDB until an author with an unknown Twitter handle is found."""
    found_items = None
    exclusive_start_key = None
    while not found_items:
        params = {
            "KeyConditionExpression": Key("PK").eq("Author"),
            "FilterExpression": Attr("twitter_handle").not_exists()
            & Attr("has_twitter").not_exists(),
        }
        if exclusive_start_key:
            params["ExclusiveStartKey"] = exclusive_start_key

        if author:
            params["KeyConditionExpression"] = Key("PK").eq("Author") & Key("SK").eq(
                author
            )

        # Execute the query
        items = table_resource.query(**params)
        found_items = items["Items"]
        if "LastEvaluatedKey" in items:
            exclusive_start_key = items["LastEvaluatedKey"]
        else:
            break

    if found_items:
        return found_items[0]
    return None


def update_author(author_dict):
    """Override the Author with an extended dictionary."""
    table_resource.put_item(Item=author_dict)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Argparser")
    parser.add_argument(
        "-a", "--author", help="Author argument", required=False, default=None
    )
    argument = parser.parse_args()

    try:
        while True:
            fetch_and_fill(author=argument.author)
    except RuntimeError:
        print("Done")
