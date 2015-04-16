# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage.policies as _pol
# @todo Do we need a Connection class to use as a base for all connection decorators? It couls ease documenting the return type of make_connection. It could allow isinstance in clients. It could be more intuitive for clients. But with duck-typing it's not striclty necessary.
from .signing import SigningConnection
from .retrying import RetryingConnection
from .completing import CompletingConnection
from .waiting import WaitingConnection


# @todo Consider using a builder pattern... as everywhere else in LowVoltage
def make_connection(
    region,
    credentials,
    endpoint=None,
    retry_policy=_pol.ExponentialBackoffRetryPolicy(1, 2, 5),
    complete_batches=True,
    wait_for_tables=True,
):
    """Create a connection, using all decorators (RetryingConnection, CompletingConnection, WaitingConnection on top of a SigningConnection)"""
    # @todo Maybe allow injection of the Requests session to tweek low-level parameters (connection timeout, etc.)?

    if endpoint is None:
        endpoint = "https://dynamodb.{}.amazonaws.com/".format(region)
    connection = SigningConnection(region, credentials, endpoint)
    if retry_policy is not None:
        connection = RetryingConnection(connection, retry_policy)
    if complete_batches:
        connection = CompletingConnection(connection)
    if wait_for_tables:
        connection = WaitingConnection(connection)
    return connection
