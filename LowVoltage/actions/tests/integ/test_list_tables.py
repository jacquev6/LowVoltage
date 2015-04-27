# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class ListTablesLocalIntegTests(_tst.LocalIntegTests):
    def setUp(self):
        super(ListTablesLocalIntegTests, self).setUp()
        self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )
        self.connection(
            _lv.CreateTable("Bbb").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )
        self.connection(
            _lv.CreateTable("Ccc").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        self.connection(_lv.DeleteTable("Bbb"))
        self.connection(_lv.DeleteTable("Ccc"))
        super(ListTablesLocalIntegTests, self).tearDown()

    def test_all_arguments(self):
        r = self.connection(_lv.ListTables().exclusive_start_table_name("Aaa").limit(1))

        self.assertEqual(r.last_evaluated_table_name, "Bbb")
        self.assertEqual(r.table_names[0], "Bbb")

    def test_no_arguments(self):
        r = self.connection(_lv.ListTables())

        self.assertEqual(r.table_names[0], "Aaa")
        self.assertEqual(r.table_names[1], "Bbb")
        self.assertEqual(r.table_names[2], "Ccc")
