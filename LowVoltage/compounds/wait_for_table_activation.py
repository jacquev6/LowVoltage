# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time

import LowVoltage as _lv
import LowVoltage.testing as _tst


def WaitForTableActivation(connection, table):
    """Make "DescribeTable" actions until the table's status is "ACTIVE"."""

    r = connection.request(_lv.DescribeTable(table))
    while r.table.table_status != "ACTIVE":  # pragma no branch (Covered by connected integ tests)
        # @todo Use a policy to choose polling interval? Same in WaitForTableDeletion.
        time.sleep(3)  # pragma no cover (Covered by connected integ tests)
        r = connection.request(_lv.DescribeTable(table))  # pragma no cover (Covered by connected integ tests)


class WaitForTableActivationLocalIntegTests(_tst.LocalIntegTests):
    def tearDown(self):
        self.connection.request(_lv.DeleteTable("Aaa"))

    def test(self):
        self.connection.request(_lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, "Aaa")
        self.assertEqual(self.connection.request(_lv.DescribeTable("Aaa")).table.table_status, "ACTIVE")


class WaitForTableActivationConnectedIntegTests(_tst.ConnectedIntegTests):  # pragma no cover (Connected integ test)
    def setUp(self):
        self.table = self.make_table_name()

    def tearDown(self):
        self.connection.request(_lv.DeleteTable(self.table))

    def test(self):
        self.connection.request(_lv.CreateTable(self.table).hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, self.table)
        self.assertEqual(self.connection.request(_lv.DescribeTable(self.table)).table.table_status, "ACTIVE")
