# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time

import LowVoltage as _lv
import LowVoltage.testing as _tst


# @todo Should we abort if the table_status is not "DELETING"? Better provide a DeleteTableAndWait compound
def wait_for_table_deletion(connection, table):
    """
    Make :class:`.DescribeTable` actions until a :exc:`.ResourceNotFoundException` is raised.
    Useful after :class:`.DeleteTable` action.

    .. testsetup::

        table = "LowVoltage.Tests.Doc.WaitForTableDeletion.1"
        connection(CreateTable(table).hash_key("h", STRING).provisioned_throughput(1, 1))
        wait_for_table_activation(connection, table)

    >>> connection(DeleteTable(table)).table_description.table_status
    u'DELETING'
    >>> wait_for_table_deletion(connection, table)
    >>> connection(DescribeTable(table))
    Traceback (most recent call last):
      ...
    LowVoltage.exceptions.ResourceNotFoundException: ...
    """

    while True:
        try:
            connection(_lv.DescribeTable(table))
            time.sleep(3)
        except _lv.ResourceNotFoundException:
            break
