import boto3
from boto3.dynamodb.conditions import Key, Attr

table_name = 'aws-blogs-twitter-feed-BlogsTableV24493143C-18E0LOVY9EAQ4'
dynamodb = boto3.resource('dynamodb')
table_resource = dynamodb.Table(table_name)

def fetch_and_fill():
    author_no_details = get_last_author_without_details()
    author_name = author_no_details['SK']
    print(author_name)
    
    has_handle = None
    while has_handle is None:
        response = input(f"Does '{author_name}' have a twitter handle (y/n)? ")
        if response == 'y':
            has_handle = True
        if response == 'n':
            has_handle = False

    if has_handle:
        twitter_handle = input(f'What is the twitter handle for {author_name}?\n')
        if not twitter_handle.startswith('@'):
            twitter_handle = f'@{twitter_handle}'
        update_author({
            **author_no_details,
            'twitter_handle': twitter_handle
        })
    else:
        update_author({
            **author_no_details,
            'has_twitter': False
        })

def get_last_author_without_details():
    found_items = None
    exclusive_start_key = None
    while not found_items:
        params = {
            'KeyConditionExpression': Key('PK').eq('Author'),
            'FilterExpression': Attr('twitter_handle').not_exists() & Attr('has_twitter').not_exists(),
        }
        if exclusive_start_key:
            params['ExclusiveStartKey'] = exclusive_start_key

        # Execute the query
        items = table_resource.query(**params)
        found_items = items['Items']
        if 'LastEvaluatedKey' in items:
            exclusive_start_key = items['LastEvaluatedKey']

    if found_items:
        return found_items[0]
    return None

def update_author(author_dict):
    table_resource.put_item(
        Item=author_dict
    )

if __name__ == '__main__':
    while True:
        fetch_and_fill()