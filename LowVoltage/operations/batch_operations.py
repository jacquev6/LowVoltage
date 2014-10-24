# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation as _Operation
from LowVoltage.operations.return_mixins import ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin
from LowVoltage.operations.conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value
import LowVoltage.tests.dynamodb_local
import LowVoltage.operations.admin_operations
import LowVoltage.operations.item_operations
import LowVoltage.return_types as _rtyp
import LowVoltage.attribute_types as _atyp
import LowVoltage.exceptions as _exn
from LowVoltage.tests.cover import cover


class BatchGetItem(_Operation, ReturnConsumedCapacityMixin):
    class Result(object):
        def __init__(
            self,
            Responses=None,
            UnprocessedKeys=None,
            **dummy
        ):
            self.responses = {t: [_convert_db_to_dict(v) for v in vs] for t, vs in Responses.iteritems()}
            self.unprocessed_keys = UnprocessedKeys # @todo Read doc and implement
            # @todo ConsumedCapacity

    def __init__(self):
        super(BatchGetItem, self).__init__("BatchGetItem")
        ReturnConsumedCapacityMixin.__init__(self)
        self.__request_items = {}
        self.__last_table = None

    def build(self):
        data = {
        }
        self._build_return_consumed_capacity(data)
        if self.__request_items:
            data["RequestItems"] = self.__request_items
        return data

    def table(self, table_name):
        if table_name not in self.__request_items:
            self.__request_items[table_name] = {}
        self.__last_table = self.__request_items[table_name]
        return self

    def keys(self, *keys):
        if "Keys" not in self.__last_table:
            self.__last_table["Keys"] = []
        for key in keys:
            if isinstance(key, dict):
                key = [key]
            self.__last_table["Keys"].extend(_convert_dict_to_db(k) for k in key)
        return self

    def consistent_read_true(self):
        self.__last_table["ConsistentRead"] = True
        return self

    def consistent_read_false(self):
        self.__last_table["ConsistentRead"] = False
        return self

    def attributes_to_get(self, *names):
        if "AttributesToGet" not in self.__last_table:
            self.__last_table["AttributesToGet"] = []
        for name in names:
            if isinstance(name, basestring):
                name = [name]
            self.__last_table["AttributesToGet"].extend(name)
        return self


class BatchGetItemUnitTests(unittest.TestCase):
    def testEmpty(self):
        self.assertEqual(
            BatchGetItem().build(),
            {}
        )

    def testReturnConsumedCapacityTotal(self):
        self.assertEqual(
            BatchGetItem().return_consumed_capacity_total().build(),
            {
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnConsumedCapacityIndexes(self):
        self.assertEqual(
            BatchGetItem().return_consumed_capacity_indexes().build(),
            {
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            BatchGetItem().return_consumed_capacity_none().build(),
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testKeys(self):
        self.assertEqual(
            BatchGetItem().table("Table2").keys({"hash": "h21"}).table("Table1").keys({"hash": "h11"}, {"hash": "h12"}).table("Table2").keys([{"hash": "h22"}, {"hash": "h23"}]).build(),
            {
                "RequestItems": {
                    "Table1": {
                        "Keys": [
                            {"hash": {"S": "h11"}},
                            {"hash": {"S": "h12"}},
                        ]
                    },
                    "Table2": {
                        "Keys": [
                            {"hash": {"S": "h21"}},
                            {"hash": {"S": "h22"}},
                            {"hash": {"S": "h23"}},
                        ]
                    },
                }
            }
        )

    def testConsistentRead(self):
        self.assertEqual(
            BatchGetItem().table("Table1").consistent_read_true().table("Table2").consistent_read_false().build(),
            {
                "RequestItems": {
                    "Table1": {
                        "ConsistentRead": True,
                    },
                    "Table2": {
                        "ConsistentRead": False,
                    },
                }
            }
        )

    def testAttributesToGet(self):
        self.assertEqual(
            BatchGetItem().table("Table2").attributes_to_get("a").table("Table1").attributes_to_get("b", "c").table("Table2").attributes_to_get(["d", "e"]).build(),
            {
                "RequestItems": {
                    "Table1": {
                        "AttributesToGet": ["b", "c"],
                    },
                    "Table2": {
                        "AttributesToGet": ["a", "d", "e"],
                    },
                }
            }
        )


class BatchGetItemIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.operations.admin_operations.CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(LowVoltage.operations.admin_operations.DeleteTable("Aaa"))

    def testSimpleBatchGet(self):
        self.connection.request(LowVoltage.operations.item_operations.PutItem("Aaa", {"h": "1", "a": "xxx"}))
        self.connection.request(LowVoltage.operations.item_operations.PutItem("Aaa", {"h": "2", "a": "yyy"}))
        self.connection.request(LowVoltage.operations.item_operations.PutItem("Aaa", {"h": "3", "a": "zzz"}))

        r = self.connection.request(BatchGetItem().table("Aaa").keys({"h": "1"}, {"h": "2"}, {"h": "3"}))

        with cover("r", r) as r:
            self.assertEqual(r.responses.keys(), ["Aaa"])
            self.assertEqual(
                sorted(r.responses["Aaa"], key=lambda i: i["h"]),
                [{"h": "1", "a": "xxx"}, {"h": "2", "a": "yyy"}, {"h": "3", "a": "zzz"}]
            )
            self.assertEqual(r.unprocessed_keys, {})


class BatchWriteItem(_Operation, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    class Result(object):
        def __init__(
            self,
            UnprocessedItems=None,
            **dummy
        ):
            self.unprocessed_items = UnprocessedItems  # @todo Read the doc and implement
            # @todo ConsumedCapacity and ItemCollectionMetrics

    def __init__(self):
        super(BatchWriteItem, self).__init__("BatchWriteItem")
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)
        self.__request_items = {}
        self.__last_table = None

    def build(self):
        data = {
        }
        self._build_return_consumed_capacity(data)
        self._build_return_item_collection_metrics(data)
        if self.__request_items:
            data["RequestItems"] = self.__request_items
        return data

    def table(self, table_name):
        if table_name not in self.__request_items:
            self.__request_items[table_name] = []
        self.__last_table = self.__request_items[table_name]
        return self

    def delete(self, *keys):
        for key in keys:
            if isinstance(key, dict):
                key = [key]
            self.__last_table.extend({"DeleteRequest": {"Key": _convert_dict_to_db(k)}} for k in key)
        return self

    def put(self, *items):
        for item in items:
            if isinstance(item, dict):
                item = [item]
            self.__last_table.extend({"PutRequest": {"Item": _convert_dict_to_db(i)}} for i in item)
        return self


class BatchWriteItemUnitTests(unittest.TestCase):
    def testEmpty(self):
        self.assertEqual(
            BatchWriteItem().build(),
            {}
        )

    def testReturnConsumedCapacityTotal(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_total().build(),
            {
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnConsumedCapacityIndexes(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_indexes().build(),
            {
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_none().build(),
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnSizeItemCollectionMetrics(self):
        self.assertEqual(
            BatchWriteItem().return_item_collection_metrics_size().build(),
            {
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def testReturnNoItemCollectionMetrics(self):
        self.assertEqual(
            BatchWriteItem().return_item_collection_metrics_none().build(),
            {
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


    def testDelete(self):
        self.assertEqual(
            BatchWriteItem().table("Table").delete({"hash": "h1"}).table("Table").delete([{"hash": "h2"}]).build(),
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
            BatchWriteItem().table("Table").put({"hash": "h1"}, [{"hash": "h2"}]).build(),
            {
                "RequestItems": {
                    "Table": [
                        {"PutRequest": {"Item": {"hash": {"S": "h1"}}}},
                        {"PutRequest": {"Item": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )


class BatchWriteItemIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.operations.admin_operations.CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(LowVoltage.operations.admin_operations.DeleteTable("Aaa"))

    def testSimpleBatchPut(self):
        r = self.connection.request(
            BatchWriteItem().table("Aaa")
                .put({"h": "1", "a": "xxx"}, {"h": "2", "a": "yyy"}, {"h": "3", "a": "zzz"})
        )

        with cover("r", r) as r:
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection.request(LowVoltage.operations.item_operations.GetItem("Aaa", {"h": "1"})).item,
            {"h": "1", "a": "xxx"}
        )

    def testSimpleBatchDelete(self):
        self.connection.request(LowVoltage.operations.item_operations.PutItem("Aaa", {"h": "1", "a": "xxx"}))
        self.connection.request(LowVoltage.operations.item_operations.PutItem("Aaa", {"h": "2", "a": "yyy"}))
        self.connection.request(LowVoltage.operations.item_operations.PutItem("Aaa", {"h": "3", "a": "zzz"}))

        r = self.connection.request(BatchWriteItem().table("Aaa").delete({"h": "1"}, {"h": "2"}, {"h": "3"}))

        with cover("r", r) as r:
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection.request(LowVoltage.operations.item_operations.GetItem("Aaa", {"h": "1"})).item,
            None
        )


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
