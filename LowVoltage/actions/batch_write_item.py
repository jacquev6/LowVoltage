# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import MockMockMock

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import CompletableAction, ActionProxy
from .conversion import _convert_dict_to_db
from .return_mixins import ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin
from .return_types import ConsumedCapacity_, ItemCollectionMetrics_, _is_dict


class BatchWriteItem(
    CompletableAction,
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
            if _is_dict(ConsumedCapacity):  # pragma no branch (Defensive code)
                self.consumed_capacity = ConsumedCapacity_(**ConsumedCapacity)

            self.item_collection_metrics = None
            if _is_dict(ItemCollectionMetrics):  # pragma no branch (Defensive code)
                self.item_collection_metrics = ItemCollectionMetrics_(**ItemCollectionMetrics)

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

    def get_completion_action(self, r):
        if _is_dict(r.unprocessed_items) and len(r.unprocessed_items) != 0:
            return BatchWriteItem(r.unprocessed_items)
        else:
            return None

    def complete_response(self, r1, r2):
        r1.consumed_capacity = r2.consumed_capacity  # @todo Should we merge those? (Maybe add them?)
        r1.item_collection_metrics = r2.item_collection_metrics  # @todo Make sure that the newer one superceeds the older one.
        r1.unprocessed_items = r2.unprocessed_items


class BatchWriteItemUnitTests(unittest.TestCase):
    def setUp(self):
        self.mocks = MockMockMock.Engine()

    def tearDown(self):
        self.mocks.tearDown()

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

    def test_no_completion_action(self):
        self.assertIsNone(
            BatchWriteItem().get_completion_action(BatchWriteItem.Result(UnprocessedItems={}))
        )

    def test_completion_action(self):
        a = BatchWriteItem().get_completion_action(BatchWriteItem.Result(UnprocessedItems={"Foo": {}}))
        self.assertEqual(a.build(), {"RequestItems": {"Foo": {}}})

    def test_complete_response(self):
        r = BatchWriteItem.Result(UnprocessedItems={})
        r2 = self.mocks.create("r2")
        r2.expect.consumed_capacity.andReturn(1)
        r2.expect.item_collection_metrics.andReturn(2)
        r2.expect.unprocessed_items.andReturn(3)
        BatchWriteItem().complete_response(r, r2.object)
        self.assertEqual(r.consumed_capacity, 1)
        self.assertEqual(r.item_collection_metrics, 2)
        self.assertEqual(r.unprocessed_items, 3)


class BatchWriteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def testSimpleBatchPut(self):
        r = self.connection.request(
            _lv.BatchWriteItem().table("Aaa")
                .put({"h": u"1", "a": "xxx"}, {"h": u"2", "a": "yyy"}, {"h": u"3", "a": "zzz"})
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection.request(_lv.GetItem("Aaa", {"h": u"1"})).item,
            {"h": "1", "a": "xxx"}
        )

    def testSimpleBatchDelete(self):
        self.connection.request(_lv.PutItem("Aaa", {"h": u"1", "a": "xxx"}))
        self.connection.request(_lv.PutItem("Aaa", {"h": u"2", "a": "yyy"}))
        self.connection.request(_lv.PutItem("Aaa", {"h": u"3", "a": "zzz"}))

        r = self.connection.request(_lv.BatchWriteItem().table("Aaa").delete({"h": u"1"}, {"h": u"2"}, {"h": u"3"}))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection.request(_lv.GetItem("Aaa", {"h": u"1"})).item,
            None
        )

    def test_write_without_unprocessed_items(self):
        for i in range(25):
            self.connection.request(_lv.PutItem("Aaa", {"h": unicode(i)}))

        action = _lv.BatchWriteItem().table("Aaa").delete({"h": unicode(i)} for i in range(25))
        r = self.connection.request(action)
        self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(action.is_completable, True)
        self.assertIsNone(action.get_completion_action(r))

    # #todo I don't know if we can write a test_write_with_unprocessed_items because I don't know how to make DynamoDB return some UnprocessedItems on demand.
