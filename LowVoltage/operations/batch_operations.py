# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin


class BatchGetItem(Operation, ReturnConsumedCapacityMixin):
    def __init__(self, connection):
        super(BatchGetItem, self).__init__("BatchGetItem", connection)
        ReturnConsumedCapacityMixin.__init__(self)
        self.__request_items = {}
        self.__last_table = None

    def _build(self):
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
            self.__last_table["Keys"].extend(self._convert_dict(k) for k in key)
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


class BatchGetItemTestCase(unittest.TestCase):
    def testEmpty(self):
        self.assertEqual(
            BatchGetItem(None)._build(),
            {}
        )

    def testReturnConsumedCapacityTotal(self):
        self.assertEqual(
            BatchGetItem(None).return_consumed_capacity_total()._build(),
            {
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnConsumedCapacityIndexes(self):
        self.assertEqual(
            BatchGetItem(None).return_consumed_capacity_indexes()._build(),
            {
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            BatchGetItem(None).return_consumed_capacity_none()._build(),
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testKeys(self):
        self.assertEqual(
            BatchGetItem(None).table("Table2").keys({"hash": "h21"}).table("Table1").keys({"hash": "h11"}, {"hash": "h12"}).table("Table2").keys([{"hash": "h22"}, {"hash": "h23"}])._build(),
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
            BatchGetItem(None).table("Table1").consistent_read_true().table("Table2").consistent_read_false()._build(),
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
            BatchGetItem(None).table("Table2").attributes_to_get("a").table("Table1").attributes_to_get("b", "c").table("Table2").attributes_to_get(["d", "e"])._build(),
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


class BatchWriteItem(Operation, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    def __init__(self, connection):
        super(BatchWriteItem, self).__init__("BatchWriteItem", connection)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)
        self.__request_items = {}
        self.__last_table = None

    def _build(self):
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
            self.__last_table.extend({"DeleteRequest": {"Key": self._convert_dict(k)}} for k in key)
        return self

    def put(self, *keys):
        for key in keys:
            if isinstance(key, dict):
                key = [key]
            self.__last_table.extend({"PutRequest": {"Item": self._convert_dict(k)}} for k in key)
        return self


class BatchWriteItemTestCase(unittest.TestCase):
    def testEmpty(self):
        self.assertEqual(
            BatchWriteItem(None)._build(),
            {}
        )

    def testReturnConsumedCapacityTotal(self):
        self.assertEqual(
            BatchWriteItem(None).return_consumed_capacity_total()._build(),
            {
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnConsumedCapacityIndexes(self):
        self.assertEqual(
            BatchWriteItem(None).return_consumed_capacity_indexes()._build(),
            {
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            BatchWriteItem(None).return_consumed_capacity_none()._build(),
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnSizeItemCollectionMetrics(self):
        self.assertEqual(
            BatchWriteItem(None).return_item_collection_metrics_size()._build(),
            {
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def testReturnNoItemCollectionMetrics(self):
        self.assertEqual(
            BatchWriteItem(None).return_item_collection_metrics_none()._build(),
            {
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


    def testDelete(self):
        self.assertEqual(
            BatchWriteItem(None).table("Table").delete({"hash": "h1"}).table("Table").delete([{"hash": "h2"}])._build(),
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
            BatchWriteItem(None).table("Table").put({"hash": "h1"}, [{"hash": "h2"}])._build(),
            {
                "RequestItems": {
                    "Table": [
                        {"PutRequest": {"Item": {"hash": {"S": "h1"}}}},
                        {"PutRequest": {"Item": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )


if __name__ == "__main__":  # pragma no branch (Test code)
    unittest.main()  # pragma no cover (Test code)
