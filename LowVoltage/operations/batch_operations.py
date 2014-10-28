# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation as _Operation, OperationProxy as _OperationProxy
from LowVoltage.operations.return_mixins import ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin
from LowVoltage.operations.expression_mixins import ExpressionAttributeNamesMixin, ProjectionExpressionMixin
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
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_ResponseElements
            # - ConsumedCapacity: @todo
            # - Responses: done
            # - UnprocessedKeys: @todo
            self.responses = {t: [_convert_db_to_dict(v) for v in vs] for t, vs in Responses.iteritems()}
            self.unprocessed_keys = UnprocessedKeys

    def __init__(self):
        super(BatchGetItem, self).__init__("BatchGetItem")
        ReturnConsumedCapacityMixin.__init__(self)
        self.__tables = {}

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_RequestParameters
        # - RequestItems:
        #   - Keys: done
        #   - AttributesToGet: deprecated
        #   - ConsistentRead: done
        #   - ExpressionAttributeNames: done
        #   - ProjectionExpression: done
        # - ReturnConsumedCapacity: done
        data = {}
        data.update(self._build_return_consumed_capacity())
        if self.__tables:
            data["RequestItems"] = {n: t._build() for n, t in self.__tables.iteritems()}
        return data

    class _Table(_OperationProxy, ExpressionAttributeNamesMixin, ProjectionExpressionMixin):
        def __init__(self, operation, name):
            super(BatchGetItem._Table, self).__init__(operation)
            ExpressionAttributeNamesMixin.__init__(self)
            ProjectionExpressionMixin.__init__(self)
            self.__consistent_read = None
            self.__keys = []

        def _build(self):
            data = {}
            data.update(self._build_expression_attribute_names())
            data.update(self._build_projection_expression())
            if self.__consistent_read is not None:
                data["ConsistentRead"] = self.__consistent_read
            if self.__keys:
                data["Keys"] = [_convert_dict_to_db(k) for k in self.__keys]
            return data

        def keys(self, *keys):
            for key in keys:
                if isinstance(key, dict):
                    key = [key]
                self.__keys.extend(key)
            return self

        def consistent_read_true(self):
            self.__consistent_read = True
            return self

        def consistent_read_false(self):
            self.__consistent_read = False
            return self

    def table(self, name):
        if name not in self.__tables:
            self.__tables[name] = self._Table(self, name)
        return self.__tables[name]


class BatchGetItemUnitTests(unittest.TestCase):
    def testEmpty(self):
        self.assertEqual(
            BatchGetItem().build(),
            {}
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

    def testProjection(self):
        self.assertEqual(
            BatchGetItem().table("Table1").project("a", ["b", "c"]).build(),
            {
                "RequestItems": {
                    "Table1": {
                        "ProjectionExpression": "a, b, c",
                    },
                }
            }
        )

    def testExpressionAttributeName(self):
        self.assertEqual(
            BatchGetItem().table("Table1").expression_attribute_name("a", "p").build(),
            {
                "RequestItems": {
                    "Table1": {
                        "ExpressionAttributeNames": {"#a": "p"},
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

    def testBatchGetWithProjections(self):
        self.connection.request(LowVoltage.operations.item_operations.PutItem("Aaa", {"h": "1", "a": "a1", "b": "b1", "c": "c1"}))
        self.connection.request(LowVoltage.operations.item_operations.PutItem("Aaa", {"h": "2", "a": "a2", "b": "b2", "c": "c2"}))
        self.connection.request(LowVoltage.operations.item_operations.PutItem("Aaa", {"h": "3", "a": "a3", "b": "b3", "c": "c3"}))

        r = self.connection.request(
            BatchGetItem().table("Aaa").keys({"h": "1"}, {"h": "2"}, {"h": "3"}).expression_attribute_name("p", "b").project("h").project("a", ["#p"])
        )

        with cover("r", r) as r:
            self.assertEqual(r.responses.keys(), ["Aaa"])
            self.assertEqual(
                sorted(r.responses["Aaa"], key=lambda i: i["h"]),
                [{"h": "1", "a": "a1", "b": "b1"}, {"h": "2", "a": "a2", "b": "b2"}, {"h": "3", "a": "a3", "b": "b3"}]
            )
            self.assertEqual(r.unprocessed_keys, {})


class BatchWriteItem(_Operation, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    class Result(object):
        def __init__(
            self,
            UnprocessedItems=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_ResponseElements
            # - ConsumedCapacity: @todo
            # - ItemCollectionMetrics: @todo
            # - UnprocessedItems: @todo
            self.unprocessed_items = UnprocessedItems

    def __init__(self):
        super(BatchWriteItem, self).__init__("BatchWriteItem")
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)
        self.__tables = {}

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_RequestParameters
        # - RequestItems
        #   - DeleteRequest.Key: done
        #   - PutRequest.Item: done
        # - ReturnConsumedCapacity: done
        # - ReturnItemCollectionMetrics: done
        data = {}
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_return_item_collection_metrics())
        if self.__tables:
            data["RequestItems"] = {n: t._build() for n, t in self.__tables.iteritems()}
        return data

    class _Table(_OperationProxy):
        def __init__(self, operation, name):
            super(BatchWriteItem._Table, self).__init__(operation)
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


# http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestParameters
# - KeyConditions: @todo
# - TableName: @todo
# - AttributesToGet: deprecated
# - ConditionalOperator: deprecated
# - ConsistentRead: @todo
# - ExclusiveStartKey: @todo
# - ExpressionAttributeNames: @todo
# - ExpressionAttributeValues: @todo
# - FilterExpression: @todo
# - IndexName: @todo
# - Limit: @todo
# - ProjectionExpression: @todo
# - QueryFilter: deprecated
# - ReturnConsumedCapacity: @todo
# - ScanIndexForward: @todo
# - Select: @todo

# http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_ResponseElements
# - ConsumedCapacity: @todo
# - Count: @todo
# - Items: @todo
# - LastEvaluatedKey: @todo
# - ScannedCount: @todo

# http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_RequestParameters
# - TableName: @todo
# - AttributesToGet: deprecated
# - ConditionalOperator: deprecated
# - ExclusiveStartKey: @todo
# - ExpressionAttributeNames: @todo
# - ExpressionAttributeValues: @todo
# - FilterExpression: @todo
# - Limit: @todo
# - ProjectionExpression: @todo
# - ReturnConsumedCapacity: @todo
# - ScanFilter: deprecated
# - Segment: @todo
# - Select: @todo
# - TotalSegments: @todo

# http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_ResponseElements
# - ConsumedCapacity: @todo
# - Count: @todo
# - Items: @todo
# - LastEvaluatedKey: @todo
# - ScannedCount: @todo


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
