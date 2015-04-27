# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class DeleteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_simple_delete(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"simple", "a": "yyy"}))

        self.connection(_lv.DeleteItem("Aaa", {"h": u"simple"}))

        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"simple"})).item, None)

    def test_return_old_values(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"return", "a": "yyy"}))

        r = self.connection(_lv.DeleteItem("Aaa", {"h": u"return"}).return_values_all_old())

        self.assertEqual(r.attributes, {"h": "return", "a": "yyy"})


class DeleteItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(DeleteItemConnectedIntegTests, self).setUp()
        self.connection(_lv.PutItem(self.table, self.item))

    def test_return_consumed_capacity_indexes(self):
        r = self.connection(_lv.DeleteItem(self.table, self.tab_key).return_consumed_capacity_indexes())

        self.assertEqual(r.consumed_capacity.capacity_units, 3.0)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_item_collection_metrics_size(self):
        r = self.connection(_lv.DeleteItem(self.table, self.tab_key).return_item_collection_metrics_size())

        self.assertEqual(r.item_collection_metrics.item_collection_key, {"tab_h": u"0"})
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[0], 0.0)
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[1], 1.0)
