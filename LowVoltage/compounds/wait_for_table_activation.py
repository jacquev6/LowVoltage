# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time

import LowVoltage as _lv
import LowVoltage.testing as _tst


def WaitForTableActivation(connection, table):
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
    >>> WaitForTableActivation(connection, table)
    >>> connection(DescribeTable(table)).table.table_status
    u'ACTIVE'

    .. testcleanup::

        connection(DeleteTable(table))
        WaitForTableDeletion(connection, table)
    """

    r = connection(_lv.DescribeTable(table))
    while r.table.table_status != "ACTIVE" or (r.table.global_secondary_indexes is not None and any(gsi.index_status != "ACTIVE" for gsi in r.table.global_secondary_indexes)):
        time.sleep(3)
        r = connection(_lv.DescribeTable(table))


class WaitForTableActivationLocalIntegTests(_tst.LocalIntegTests):
    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        super(WaitForTableActivationLocalIntegTests, self).tearDown()

    def test(self):
        self.connection(_lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, "Aaa")
        self.assertEqual(self.connection(_lv.DescribeTable("Aaa")).table.table_status, "ACTIVE")


class WaitForTableActivationConnectedIntegTests(_tst.ConnectedIntegTests):
    def setUp(self):
        super(WaitForTableActivationConnectedIntegTests, self).setUp()
        self.table = self.make_table_name()

    def tearDown(self):
        self.connection(_lv.DeleteTable(self.table))
        super(WaitForTableActivationConnectedIntegTests, self).tearDown()

    def test(self):
        self.connection(
            _lv.CreateTable(self.table).hash_key("tab_h", _lv.STRING).range_key("tab_r", _lv.NUMBER).provisioned_throughput(1, 1)
                .global_secondary_index("gsi").hash_key("gsi_h", _lv.STRING).range_key("gsi_r", _lv.NUMBER).project_all().provisioned_throughput(1, 1)
        )
        _lv.WaitForTableActivation(self.connection, self.table)
        r = self.connection(_lv.DescribeTable(self.table))
        self.assertEqual(r.table.table_status, "ACTIVE")
        self.assertEqual(r.table.global_secondary_indexes[0].index_status, "ACTIVE")
