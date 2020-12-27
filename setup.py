"""Setup for CDK project."""
import setuptools


with open('README.md') as fp:
    long_description = fp.read()


setuptools.setup(
    name='aws_blogs_twitter_feed',
    version='1.0.0',

    description='An empty CDK Python app',
    long_description=long_description,
    long_description_content_type='text/markdown',

    author='author',

    package_dir={'': 'aws_blogs_twitter_feed'},
    packages=setuptools.find_packages(where='aws_blogs_twitter_feed'),

    install_requires=[
        'aws-cdk.core==1.80.0',
        'aws-cdk.aws-events-targets==1.80.0',
        'aws-cdk.aws-lambda-event-sources==1.80.0',
        'aws-cdk.aws-secretsmanager==1.80.0',
        'aws-cdk.aws-events==1.80.0',
        'aws-cdk.aws-sqs==1.80.0',
        'aws-cdk.aws-ssm==1.80.0',
        'aws-cdk.aws-lambda==1.80.0',
        'aws-cdk.aws-dynamodb==1.80.0',
        'PyYAML==5.3',
        'boto3==1.16.9',
        'TwitterAPI==2.6.2.1'
    ],

    python_requires='>=3.7',

    classifiers=[
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',

        'License :: OSI Approved :: Apache Software License',

        'Programming Language :: JavaScript',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',

        'Topic :: Software Development :: Code Generators',
        'Topic :: Utilities',

        'Typing :: Typed',
    ],
)
