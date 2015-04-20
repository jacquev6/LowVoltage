# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time

import LowVoltage as _lv
import LowVoltage.testing as _tst


def WaitForTableActivation(connection, table):
    """Make "DescribeTable" actions until the table's status is "ACTIVE"."""

    r = connection.request(_lv.DescribeTable(table))
    while r.table.table_status != "ACTIVE":
        # @todo Use a policy to choose polling interval? Same in WaitForTableDeletion.
        time.sleep(3)
        r = connection.request(_lv.DescribeTable(table))


class WaitForTableActivationLocalIntegTests(_tst.LocalIntegTests):
    def tearDown(self):
        self.connection.request(_lv.DeleteTable("Aaa"))

    def test(self):
        self.connection.request(_lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, "Aaa")
        self.assertEqual(self.connection.request(_lv.DescribeTable("Aaa")).table.table_status, "ACTIVE")


class WaitForTableActivationConnectedIntegTests(_tst.ConnectedIntegTests):
    @classmethod
    def setUpClass(cls):
        _tst.ConnectedIntegTests.setUpClass()
        cls.table_name = cls.make_table_name()

    def tearDown(self):
        self.connection.request(_lv.DeleteTable(self.table_name))

    def test(self):
        self.connection.request(_lv.CreateTable(self.table_name).hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, self.table_name)
        self.assertEqual(self.connection.request(_lv.DescribeTable(self.table_name)).table.table_status, "ACTIVE")
