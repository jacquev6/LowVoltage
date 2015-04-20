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


class WaitForTableDeletionLocalIntegTests(_tst.LocalIntegTests):
    def setUp(self):
        self.connection.request(_lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 1))

    def test(self):
        self.connection.request(_lv.DeleteTable("Aaa"))
        _lv.WaitForTableDeletion(self.connection, "Aaa")
        with self.assertRaises(_lv.ResourceNotFoundException):
            self.connection.request(_lv.DescribeTable("Aaa"))


class WaitForTableDeletionConnectedIntegTests(_tst.ConnectedIntegTests):
    @classmethod
    def setUpClass(cls):
        _tst.ConnectedIntegTests.setUpClass()
        cls.table_name = cls.make_table_name()

    def setUp(self):
        self.connection.request(_lv.CreateTable(self.table_name).hash_key("h", _lv.STRING).provisioned_throughput(1, 1))
        _lv.WaitForTableActivation(self.connection, self.table_name)

    def test(self):
        self.connection.request(_lv.DeleteTable(self.table_name))
        _lv.WaitForTableDeletion(self.connection, self.table_name)
        with self.assertRaises(_lv.ResourceNotFoundException):
            self.connection.request(_lv.DescribeTable(self.table_name))
