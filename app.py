#!/usr/bin/env python3
"""Main CDK App."""

from aws_cdk import core

from aws_blogs_twitter_feed.aws_blogs_twitter_feed_stack \
    import AwsBlogsTwitterFeedStack

app = core.App()
AwsBlogsTwitterFeedStack(app, 'aws-blogs-twitter-feed')

app.synth()
