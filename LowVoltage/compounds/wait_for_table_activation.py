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
    while r.table.table_status != "ACTIVE" or (r.table.global_secondary_indexes is not None and any(gsi.index_status != "ACTIVE" for gsi in r.table.global_secondary_indexes)):
        time.sleep(3)
        r = connection(_lv.DescribeTable(table))
