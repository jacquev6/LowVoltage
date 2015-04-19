# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time

import LowVoltage as _lv
import LowVoltage.testing as _tst


def WaitForTableDeletion(connection, table):
    """Make "DescribeTable" actions until a ResourceNotFoundException is raised."""

    while True:
        try:
            connection.request(_lv.DescribeTable(table))
            time.sleep(3)
        except _lv.ResourceNotFoundException:
            break


class WaitForTableDeletionConnectedIntegTests(_tst.ConnectedIntegTests):
    def setUp(self):
        self.table = self.make_table_name()
        self.connection.request(_lv.CreateTable(self.table).hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, self.table)
        
    def test(self):
        self.connection.request(_lv.DeleteTable(self.table))
        _lv.WaitForTableDeletion(self.connection, self.table)
        with self.assertRaises(_lv.ResourceNotFoundException):
            self.connection.request(_lv.DescribeTable(self.table))
