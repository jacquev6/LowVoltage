# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time

import LowVoltage as _lv
import LowVoltage.testing as _tst


def wait_for_table_activation(connection, table):
    """
    Make :class:`.DescribeTable` actions until the table's status (and all its GSI's statuses) is ```"ACTIVE"```.
    Useful after :class:`.CreateTable` and :class:`.UpdateTable` actions.

    .. testsetup::

        table = "LowVoltage.Tests.Doc.WaitForTableActivation.1"

    >>> connection(
    ...   CreateTable(table)
    ...     .hash_key("h", STRING)
    ...     .provisioned_throughput(1, 1)
    ... ).table_description.table_status
    u'CREATING'
    >>> wait_for_table_activation(connection, table)
    >>> connection(DescribeTable(table)).table.table_status
    u'ACTIVE'

    .. testcleanup::

        connection(DeleteTable(table))
        wait_for_table_deletion(connection, table)
    """

    desc = _describe(connection, table)
    while desc is None or not _table_is_fully_active(desc):
        time.sleep(3)
        desc = _describe(connection, table)
    # Unfortunately, DescribeTable seems to perform an eventually consistent read.
    # Without the next line, I've seen the doctest fail with a table status == "CREATING"
    # after the wait. So, let's wait a bit more :-/
    time.sleep(3)


def _describe(connection, table):
    try:
        return connection(_lv.DescribeTable(table)).table
    except _lv.ResourceNotFoundException:
        # DescribeTable seems to perform an eventually consistent read.
        # So from time to time you create a table (on server A), then ask for its description
        # (on server B) and server B doesn't know yet it exists.
        # Seen in https://travis-ci.org/jacquev6/LowVoltage/jobs/61754722#L962
        return None


def _table_is_fully_active(table):
    if table.global_secondary_indexes is not None:
        for gsi in table.global_secondary_indexes:
            if gsi.index_status != "ACTIVE":
                return False
    return table.table_status == "ACTIVE"


class WaitForTableActivationUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(WaitForTableActivationUnitTests, self).setUp()
        self.connection = self.mocks.create("connection")
        self.sleep = self.mocks.replace("time.sleep")

    def test_table_not_yet_created_on_first_describe(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("DescribeTable", {"TableName": "Table"})
        ).andRaise(
            _lv.ResourceNotFoundException
        )
        self.sleep.expect(3)
        self.connection.expect._call_.withArguments(
            self.ActionChecker("DescribeTable", {"TableName": "Table"})
        ).andReturn(
            _lv.DescribeTableResponse(Table={"TableStatus": "ACTIVE"})
        )
        self.sleep.expect(3)

        wait_for_table_activation(self.connection.object, "Table")

    def test_table_creating(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("DescribeTable", {"TableName": "Table"})
        ).andReturn(
            _lv.DescribeTableResponse(Table={"TableStatus": "CREATING"})
        )
        self.sleep.expect(3)
        self.connection.expect._call_.withArguments(
            self.ActionChecker("DescribeTable", {"TableName": "Table"})
        ).andReturn(
            _lv.DescribeTableResponse(Table={"TableStatus": "ACTIVE"})
        )
        self.sleep.expect(3)

        wait_for_table_activation(self.connection.object, "Table")

    def test_gsi_creating(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("DescribeTable", {"TableName": "Table"})
        ).andReturn(
            _lv.DescribeTableResponse(Table={"TableStatus": "ACTIVE", "GlobalSecondaryIndexes": [{"IndexStatus": "CREATING"}]})
        )
        self.sleep.expect(3)
        self.connection.expect._call_.withArguments(
            self.ActionChecker("DescribeTable", {"TableName": "Table"})
        ).andReturn(
            _lv.DescribeTableResponse(Table={"TableStatus": "ACTIVE", "GlobalSecondaryIndexes": [{"IndexStatus": "ACTIVE"}]})
        )
        self.sleep.expect(3)

        wait_for_table_activation(self.connection.object, "Table")
