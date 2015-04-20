# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time

import LowVoltage as _lv
import LowVoltage.testing as _tst


def WaitForTableActivation(connection, table):
    """Make "DescribeTable" actions until the table's status is "ACTIVE"."""

    r = connection.request(_lv.DescribeTable(table))
    while r.table.table_status != "ACTIVE" or (r.table.global_secondary_indexes is not None and any(gsi.index_status != "ACTIVE" for gsi in r.table.global_secondary_indexes)):
        # @todo Use a policy to choose polling interval? Same in WaitForTableDeletion.
        time.sleep(3)
        r = connection.request(_lv.DescribeTable(table))


class WaitForTableActivationLocalIntegTests(_tst.LocalIntegTests):
    def tearDown(self):
        self.connection.request(_lv.DeleteTable("Aaa"))
        super(WaitForTableActivationLocalIntegTests, self).tearDown()

    def test(self):
        self.connection.request(_lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, "Aaa")
        self.assertEqual(self.connection.request(_lv.DescribeTable("Aaa")).table.table_status, "ACTIVE")


class WaitForTableActivationConnectedIntegTests(_tst.ConnectedIntegTests):
    def setUp(self):
        super(WaitForTableActivationConnectedIntegTests, self).setUp()
        self.table = self.make_table_name()

    def tearDown(self):
        self.connection.request(_lv.DeleteTable(self.table))
        super(WaitForTableActivationConnectedIntegTests, self).tearDown()

    def test(self):
        self.connection.request(
            _lv.CreateTable(self.table).hash_key("tab_h", _lv.STRING).range_key("tab_r", _lv.NUMBER).provisioned_throughput(1, 1)
                .global_secondary_index("gsi").hash_key("gsi_h", _lv.STRING).range_key("gsi_r", _lv.NUMBER).project_all().provisioned_throughput(1, 1)
        )
        _lv.WaitForTableActivation(self.connection, self.table)
        r = self.connection.request(_lv.DescribeTable(self.table))
        self.assertEqual(r.table.table_status, "ACTIVE")
        self.assertEqual(r.table.global_secondary_indexes[0].index_status, "ACTIVE")
