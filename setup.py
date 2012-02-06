import os
from distutils.core import setup

# also update version in __init__.py
version = '0.2.1'

setup(
    name="asyncdynamo",
    version=version,
    keywords=["dynamo", "dynamodb", "amazon", "async", "tornado"],
    long_description=open(os.path.join(os.path.dirname(__file__), "README.md"), "r").read(),
    description="async Amazon DynamoDB library for Tornado",
    author="Dan Frank",
    author_email="df@bit.ly",
    url="http://github.com/bitly/asyncdynamo",
    license="Apache Software License",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
    ],
    packages=['asyncdynamo'],
    install_requires=['tornado', 'boto'],
    requires=['tornado'],
    download_url="http://github.com/downloads/bitly/asyncdynamo/asyncdynamo-%s.tar.gz" % version,
)
