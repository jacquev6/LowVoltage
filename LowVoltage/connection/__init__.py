# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

# @todo Do we really need two Connection classes?
from .signing import SigningConnection
from .retrying import RetryingConnection
from .retry_policies import ExponentialBackoffRetryPolicy
from .credentials import StaticCredentials, EnvironmentCredentials


# @todo Consider using a builder pattern... as everywhere else in LowVoltage
def make_connection(
    region,
    credentials,
    endpoint=None,
    retry_policy=ExponentialBackoffRetryPolicy(1, 2, 5)
):
    """Create a connection using the provided retry policy"""
    # @todo Maybe allow injection of the Requests session to tweek low-level parameters (connection timeout, etc.)?

    if endpoint is None:
        endpoint = "https://dynamodb.{}.amazonaws.com/".format(region)
    connection = SigningConnection(region, credentials, endpoint)
    if retry_policy is not None:
        connection = RetryingConnection(connection, retry_policy)
    return connection
