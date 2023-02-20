import json
import boto3
from mastodon import Mastodon


# Set the name of the parameter to retrieve
SSM_PARAMETER_NAME = "mastodon_awsblogs_access_token"

# Create a new boto3 SSM client
SSM_CLIENT = boto3.client("ssm")

# Use the get_parameter() function to retrieve the parameter
parameter_response = SSM_CLIENT.get_parameter(
    Name=SSM_PARAMETER_NAME, WithDecryption=True
)

# Extract the value of the parameter from the response
access_token = parameter_response["Parameter"]["Value"]
mastodon = Mastodon(
    api_base_url="https://awscommunity.social/",
    access_token=access_token,
)

MAX_POST_LEN = 500  # max length of a Mastodon Post


def event_handler(event, _context):
    """
    Convert EventBridge "NewAWSBlogFound" event to Mastodon post.

    Example payload:
    {
        "version": "0",
        "id": "76da47ea-83f0-16d5-64b0-648c56b8e423",
        "detail-type": "NewAWSBlogFound",
        "source": "Pipe DdbStreamListenerNe-fMIwnJP7w5dI",
        "account": "942097401199",
        "time": "2023-02-20T14:51:14Z",
        "region": "eu-west-1",
        "resources": [],
        "detail": {
            "metadata": {
                "event_id": "ab4553d26a9f58a6291ee033f485d7a8",
                "event_time": "2023-02-20T14:51:14.013Z",
                "event_version": 1
            },
            "data": {
                "blog_url": "https://aws.amazon.com/blogs/publicsector/powering-smart-islands-islands-pioneer-scalable-green-energy-solutions/",
                "date_created": "2023-02-20T14:46:42+0000",
                "date_updated": "2023-02-20T14:47:45+0000",
                "title": "Powering smart islands: How islands can pioneer scalable green energy solutions",
                "post_excerpt": "A new geopolitical and energy market reality has accelerated momentum for the green transition. But there is one geography for which energy is a perennial challenge: islands. Islands have always sought to create a sustainable environment for their populations and have had to use ingenuity, collaboration, and civic will to make it happen—and often by using innovative technology. In this way, islands can be viewed as living laboratories of what could be scaled up for mainland communities. As the world responds to the energy crisis, islands can show a path for overcoming energy challenges.",
                "main_category": "Public Sector",
                "categories": [
                    "Industries",
                    "Public Sector",
                    "Regions"
                ],
                "authors": [
                    "Joe Dignan",
                    "Louisa Barker"
                ]
            }
        }
    }

    """
    print(json.dumps(event))

    post_text = prepare_mastodon_text(event)
    response = mastodon.toot(post_text)
    print(response)


def prepare_authors(authors):
    authors_string = ""
    number_of_authors = len(authors)
    for index, author in enumerate(authors):
        authors_string += author
        if index < number_of_authors - 2:
            authors_string += ", "
        elif index == number_of_authors - 2:
            authors_string += " and "
    return authors_string


def prepare_mastodon_text(event):
    """Prepare the text to send, based on the event."""
    main_category = event["detail"]["data"]["main_category"]
    title = event["detail"]["data"]["title"]
    blog_url = event["detail"]["data"]["blog_url"]
    post_excerpt = event["detail"]["data"]["post_excerpt"]
    authors = prepare_authors(event["detail"]["data"]["authors"])
    main_category = event["detail"]["data"]["main_category"]

    base = f"New {main_category} post by {authors}:\n\n{title}\n\n"
    base_len = len(base)  # length of the base text
    url_len = len(blog_url) + 2  # URL + 2 new lines before

    rest_len_for_excerpt = MAX_POST_LEN - base_len - url_len  # space left for post

    shortened_suffix = " […]"
    original_excerpt_len = len(post_excerpt)
    shortened_excerpt_len = original_excerpt_len

    # Loop over the text, removing tokens from right to left.
    # If the length of the text + suffix is _no longer_ longer than the available room,
    # break.
    while (shortened_excerpt_len + len(shortened_suffix)) > rest_len_for_excerpt:
        components = post_excerpt.rsplit(" ", 1)
        post_excerpt = components[0]
        shortened_excerpt_len = len(post_excerpt) + len(shortened_suffix)

    if shortened_excerpt_len != original_excerpt_len:
        post_excerpt += shortened_suffix

    full_text = f"{base}{post_excerpt}\n\n{blog_url}"
    return full_text
