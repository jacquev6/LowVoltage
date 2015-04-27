# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class IterateListTablesLocalIntegTests(_tst.LocalIntegTests):
    table_names = ["Tab{:03}".format(i) for i in range(103)]

    def setUp(self):
        super(IterateListTablesLocalIntegTests, self).setUp()
        for t in self.table_names:
            self.connection(
                _lv.CreateTable(t).hash_key("h", _lv.STRING).provisioned_throughput(1, 1)
            )

    def tearDown(self):
        for t in self.table_names:
            self.connection(_lv.DeleteTable(t))
        super(IterateListTablesLocalIntegTests, self).tearDown()

    def test(self):
        self.assertEqual(
            list(_lv.iterate_list_tables(self.connection)),
            self.table_names
        )
