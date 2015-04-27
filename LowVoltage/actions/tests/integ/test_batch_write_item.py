# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class BatchWriteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_simple_batch_put(self):
        r = self.connection(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"1", "a": "xxx"},
            {"h": u"2", "a": "yyy"},
            {"h": u"3", "a": "zzz"},
        ))

        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"1"})).item, {"h": "1", "a": "xxx"})
        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"2"})).item, {"h": "2", "a": "yyy"})
        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"3"})).item, {"h": "3", "a": "zzz"})

    def test_simple_batch_delete(self):
        self.connection(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"1", "a": "xxx"},
            {"h": u"2", "a": "yyy"},
            {"h": u"3", "a": "zzz"},
        ))

        r = self.connection(_lv.BatchWriteItem().table("Aaa").delete(
            {"h": u"1"},
            {"h": u"2"},
            {"h": u"3"}
        ))

        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"1"})).item, None)
        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"2"})).item, None)
        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"3"})).item, None)


class BatchWriteItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(BatchWriteItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_indexes(self):
        r = self.connection(_lv.BatchWriteItem().table(self.table).put(self.item).return_consumed_capacity_indexes())

        self.assertEqual(r.consumed_capacity[0].capacity_units, 3.0)
        self.assertEqual(r.consumed_capacity[0].global_secondary_indexes["gsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity[0].local_secondary_indexes["lsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity[0].table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity[0].table_name, self.table)

    def test_return_item_collection_metrics_size(self):
        r = self.connection(_lv.BatchWriteItem().table(self.table).put(self.item).return_item_collection_metrics_size())

        self.assertEqual(r.item_collection_metrics[self.table][0].item_collection_key, {"tab_h": "0"})
        self.assertEqual(r.item_collection_metrics[self.table][0].size_estimate_range_gb[0], 0.0)
        self.assertEqual(r.item_collection_metrics[self.table][0].size_estimate_range_gb[1], 1.0)
