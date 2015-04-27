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

    r = connection(_lv.DescribeTable(table))
    while not _table_is_fully_active(r.table):
        time.sleep(3)
        r = connection(_lv.DescribeTable(table))


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

        wait_for_table_activation(self.connection.object, "Table")
