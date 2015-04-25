# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time

import LowVoltage as _lv
import LowVoltage.testing as _tst


# @todo Should we abort if the table_status is not "DELETING"?
def WaitForTableDeletion(connection, table):
    """Make "DescribeTable" actions until a ResourceNotFoundException is raised."""

    while True:
        try:
            connection(_lv.DescribeTable(table))
            time.sleep(3)
        except _lv.ResourceNotFoundException:
            break


class WaitForTableDeletionLocalIntegTests(_tst.LocalIntegTests):
    def setUp(self):
        super(WaitForTableDeletionLocalIntegTests, self).setUp()
        self.connection(_lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 1))

    def test(self):
        self.connection(_lv.DeleteTable("Aaa"))
        _lv.WaitForTableDeletion(self.connection, "Aaa")
        with self.assertRaises(_lv.ResourceNotFoundException):
            self.connection(_lv.DescribeTable("Aaa"))


class WaitForTableDeletionConnectedIntegTests(_tst.ConnectedIntegTests):
    def setUp(self):
        super(WaitForTableDeletionConnectedIntegTests, self).setUp()
        self.table = self.make_table_name()
        self.connection(_lv.CreateTable(self.table).hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, self.table)

    def test(self):
        self.connection(_lv.DeleteTable(self.table))
        _lv.WaitForTableDeletion(self.connection, self.table)
        with self.assertRaises(_lv.ResourceNotFoundException):
            self.connection(_lv.DescribeTable(self.table))
