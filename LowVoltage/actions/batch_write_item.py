# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action, ActionProxy
from .conversion import _convert_dict_to_db
from .return_mixins import ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin
from .return_types import ConsumedCapacity_, ItemCollectionMetrics_, _is_dict, _is_list_of_dict


class BatchWriteItem(
    Action,
    ReturnConsumedCapacityMixin,
    ReturnItemCollectionMetricsMixin,
):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_RequestParameters"""

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_ResponseElements"""

        def __init__(
            self,
            ConsumedCapacity=None,
            ItemCollectionMetrics=None,
            UnprocessedItems=None,
            **dummy
        ):
            self.consumed_capacity = None
            if _is_list_of_dict(ConsumedCapacity):  # pragma no branch (Defensive code)
                self.consumed_capacity = [ConsumedCapacity_(**c) for c in ConsumedCapacity]

            self.item_collection_metrics = None
            if _is_dict(ItemCollectionMetrics):  # pragma no branch (Defensive code)
                self.item_collection_metrics = {n: [ItemCollectionMetrics_(**m) for m in v] for n, v in ItemCollectionMetrics.iteritems()}

            self.unprocessed_items = UnprocessedItems

    def __init__(self, previous_unprocessed_items=None):
        super(BatchWriteItem, self).__init__("BatchWriteItem")
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)
        self.__previous_unprocessed_items = previous_unprocessed_items
        self.__tables = {}

    def build(self):
        data = {}
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_return_item_collection_metrics())
        if self.__previous_unprocessed_items:
            data["RequestItems"] = self.__previous_unprocessed_items
        if self.__tables:
            data["RequestItems"] = {n: t._build() for n, t in self.__tables.iteritems()}
        return data

    class _Table(ActionProxy):
        def __init__(self, action, name):
            super(BatchWriteItem._Table, self).__init__(action)
            self.__delete = []
            self.__put = []

        def _build(self):
            items = []
            if self.__delete:
                items.extend({"DeleteRequest": {"Key": _convert_dict_to_db(k)}} for k in self.__delete)
            if self.__put:
                items.extend({"PutRequest": {"Item": _convert_dict_to_db(i)}} for i in self.__put)
            return items

        def delete(self, *keys):
            for key in keys:
                if isinstance(key, dict):
                    key = [key]
                self.__delete.extend(key)
            return self

        def put(self, *items):
            for item in items:
                if isinstance(item, dict):
                    item = [item]
                self.__put.extend(item)
            return self

    def table(self, name):
        if name not in self.__tables:
            self.__tables[name] = self._Table(self, name)
        return self.__tables[name]


class BatchWriteItemUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(BatchWriteItem().name, "BatchWriteItem")

    def testEmpty(self):
        self.assertEqual(
            BatchWriteItem().build(),
            {}
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_none().build(),
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnItemCollectionMetricsNone(self):
        self.assertEqual(
            BatchWriteItem().return_item_collection_metrics_none().build(),
            {
                "ReturnItemCollectionMetrics": "NONE",
            }
        )

    def testDelete(self):
        self.assertEqual(
            BatchWriteItem().table("Table").delete({"hash": u"h1"}).table("Table").delete([{"hash": u"h2"}]).build(),
            {
                "RequestItems": {
                    "Table": [
                        {"DeleteRequest": {"Key": {"hash": {"S": "h1"}}}},
                        {"DeleteRequest": {"Key": {"hash": {"S": "h2"}}}},
                    ]
                },
            }
        )

    def testPut(self):
        self.assertEqual(
            BatchWriteItem().table("Table").put({"hash": u"h1"}, [{"hash": u"h2"}]).build(),
            {
                "RequestItems": {
                    "Table": [
                        {"PutRequest": {"Item": {"hash": {"S": "h1"}}}},
                        {"PutRequest": {"Item": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )


class BatchWriteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def testSimpleBatchPut(self):
        r = self.connection.request(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"1", "a": "xxx"},
            {"h": u"2", "a": "yyy"},
            {"h": u"3", "a": "zzz"},
        ))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection.request(_lv.GetItem("Aaa", {"h": u"1"})).item,
            {"h": "1", "a": "xxx"}
        )

    def testSimpleBatchDelete(self):
        self.connection.request(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"1", "a": "xxx"},
            {"h": u"2", "a": "yyy"},
            {"h": u"3", "a": "zzz"},
        ))

        r = self.connection.request(_lv.BatchWriteItem().table("Aaa").delete(
            {"h": u"1"},
            {"h": u"2"},
            {"h": u"3"}
        ))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection.request(_lv.GetItem("Aaa", {"h": u"1"})).item,
            None
        )


class BatchWriteItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def tearDown(self):
        self.connection.request(_lv.DeleteItem(self.table, self.tab_key))
        super(BatchWriteItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_indexes(self):
        r = self.connection.request(_lv.BatchWriteItem().table(self.table).put(self.item).return_consumed_capacity_indexes())

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity[0].capacity_units, 3.0)
            self.assertEqual(r.consumed_capacity[0].global_secondary_indexes["gsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity[0].local_secondary_indexes["lsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity[0].table.capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity[0].table_name, self.table)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

    def test_return_item_collection_metrics_size(self):
        r = self.connection.request(_lv.BatchWriteItem().table(self.table).put(self.item).return_item_collection_metrics_size())

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics[self.table][0].item_collection_key, {"tab_h": "0"})
            self.assertEqual(r.item_collection_metrics[self.table][0].size_estimate_range_gb[0], 0.0)
            self.assertEqual(r.item_collection_metrics[self.table][0].size_estimate_range_gb[1], 1.0)
            self.assertEqual(r.unprocessed_items, {})
