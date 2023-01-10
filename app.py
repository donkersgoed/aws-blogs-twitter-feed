#!/usr/bin/env python3
"""Main CDK App."""

from aws_cdk import App
from aws_blogs_twitter_feed.aws_blogs_twitter_feed_stack import AwsBlogsTwitterFeedStack

app = App()
AwsBlogsTwitterFeedStack(app, "aws-blogs-twitter-feed")

app.synth()
