# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time

import LowVoltage as _lv
import LowVoltage.testing as _tst


def WaitForTableActivation(connection, table):
    r = connection.request(_lv.DescribeTable(table))
    while r.table.table_status != "ACTIVE":
        time.sleep(3)
        r = connection.request(_lv.DescribeTable(table))


class WaitForTableActivationConnectedIntegTests(_tst.ConnectedIntegTests):
    def setUp(self):
        self.table = self.make_table_name()

    def tearDown(self):
        self.connection.request(_lv.DeleteTable(self.table))

    def test(self):
        self.connection.request(_lv.CreateTable(self.table).hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, self.table)
        self.assertEqual(self.connection.request(_lv.DescribeTable(self.table)).table.table_status, "ACTIVE")
