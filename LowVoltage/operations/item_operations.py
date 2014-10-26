# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation as _Operation, OperationProxy as _OperationProxy
from LowVoltage.operations.return_mixins import ReturnValuesMixin, ReturnOldValuesMixin
from LowVoltage.operations.return_mixins import ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin
from LowVoltage.operations.conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value
import LowVoltage.tests.dynamodb_local
import LowVoltage.operations.admin_operations
import LowVoltage.return_types as _rtyp
import LowVoltage.attribute_types as _atyp
import LowVoltage.exceptions as _exn
from LowVoltage.tests.cover import cover


class DeleteItem(_Operation, ReturnOldValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    class Result(object):
        def __init__(
            self,
            Attributes=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html#API_DeleteItem_ResponseElements
            # - Attributes: done
            # - ConsumedCapacity: @todo
            # - ItemCollectionMetrics: @todo
            self.attributes = None if Attributes is None else _convert_db_to_dict(Attributes)

    def __init__(self, table_name, key):
        super(DeleteItem, self).__init__("DeleteItem")
        self.__table_name = table_name
        self.__key = key
        ReturnOldValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html#API_DeleteItem_RequestParameters
        # - Key: done
        # - TableName: done
        # - ConditionExpression: @todo
        # - ConditionalOperator: deprecated
        # - Expected: deprecated
        # - ExpressionAttributeNames: @todo
        # - ExpressionAttributeValues: @todo
        # - ReturnConsumedCapacity: done
        # - ReturnItemCollectionMetrics: done
        # - ReturnValues: done
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self._build_return_values())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_return_item_collection_metrics())
        return data


class DeleteItemUnitTests(unittest.TestCase):
    def testKey(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def testReturnValuesNone(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnItemCollectionMetricsNone(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_item_collection_metrics_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


class DeleteItemIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.operations.admin_operations.CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(LowVoltage.operations.admin_operations.DeleteTable("Aaa"))

    def testSimpleDelete(self):
        self.connection.request(PutItem("Aaa", {"h": "simple", "a": "yyy"}))

        r = self.connection.request(DeleteItem("Aaa", {"h": "simple"}))

        with cover("r", r) as r:
            self.assertEqual(r.attributes, None)

    def testReturnOldValues(self):
        self.connection.request(PutItem("Aaa", {"h": "get", "a": "yyy"}))

        r = self.connection.request(DeleteItem("Aaa", {"h": "get"}).return_values_all_old())

        with cover("r", r) as r:
            self.assertEqual(r.attributes, {"h": "get", "a": "yyy"})


class GetItem(_Operation, ReturnConsumedCapacityMixin):
    class Result(object):
        def __init__(
            self,
            Item=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_ResponseElements
            # - ConsumedCapacity: @todo
            # - Item: done
            self.item = None if Item is None else _convert_db_to_dict(Item)

    def __init__(self, table_name, key):
        super(GetItem, self).__init__("GetItem")
        self.__table_name = table_name
        self.__key = key
        ReturnConsumedCapacityMixin.__init__(self)
        self.__consistent_read = None
        self.__projections = []

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_RequestParameters
        # - Key: done
        # - TableName: done
        # - AttributesToGet: deprecated
        # - ConsistentRead: done
        # - ExpressionAttributeNames: @todo
        # - ProjectionExpression: @todo
        # - ReturnConsumedCapacity: done
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self._build_return_consumed_capacity())
        if self.__consistent_read is not None:
            data["ConsistentRead"] = self.__consistent_read
        if self.__projections:
            data["ProjectionExpression"] = ", ".join(self.__projections)
        return data

    def consistent_read_true(self):
        return self._set_consistent_read(True)

    def consistent_read_false(self):
        return self._set_consistent_read(False)

    def _set_consistent_read(self, value):
        self.__consistent_read = value
        return self

    def project(self, *names):
        for name in names:
            if isinstance(name, basestring):
                name = [name]
            self.__projections.extend(name)
        return self


class GetItemUnitTests(unittest.TestCase):
    def testKey(self):
        self.assertEqual(
            GetItem("Table", {"hash": 42}).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            GetItem("Table", {"hash": "h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testConsistentReadTrue(self):
        self.assertEqual(
            GetItem("Table", {"hash": "h"}).consistent_read_true().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": True,
            }
        )

    def testConsistentReadFalse(self):
        self.assertEqual(
            GetItem("Table", {"hash": "h"}).consistent_read_false().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": False,
            }
        )

    def testProjection(self):
        self.assertEqual(
            GetItem("Table", {"hash": "h"}).project("abc", ["def", "ghi"]).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ProjectionExpression": "abc, def, ghi",
            }
        )


class GetItemIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.operations.admin_operations.CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(LowVoltage.operations.admin_operations.DeleteTable("Aaa"))

    def testSimpleGet(self):
        self.connection.request(PutItem("Aaa", {"h": "get", "a": "yyy"}))

        r = self.connection.request(GetItem("Aaa", {"h": "get"}))

        with cover("r", r) as r:
            self.assertEqual(r.item, {"h": "get", "a": "yyy"})

    def testGetWithProjections(self):
        self.connection.request(PutItem("Aaa", {"h": "attrs", "a": "yyy", "b": {"c": ["d1", "d2", "d3"]}, "e": 42, "f": "nope"}))

        r = self.connection.request(GetItem("Aaa", {"h": "attrs"}).project("b.c[1]", "e"))

        with cover("r", r) as r:
            self.assertEqual(r.item, {"b": {"c": ["d2"]}, "e": 42})


class PutItem(_Operation, ReturnOldValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    class Result(object):
        def __init__(
            self,
            Attributes=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_ResponseElements
            # - Attributes: done
            # - ConsumedCapacity: @todo
            # - ItemCollectionMetrics: @todo
            self.attributes = None if Attributes is None else _convert_db_to_dict(Attributes)

    def __init__(self, table_name, item):
        super(PutItem, self).__init__("PutItem")
        self.__table_name = table_name
        self.__item = item
        ReturnOldValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_RequestParameters
        # - Item: done
        # - TableName: done
        # - ConditionExpression: @todo
        # - ConditionalOperator: deprecated
        # - Expected: deprecated
        # - ExpressionAttributeNames: @todo
        # - ExpressionAttributeValues: @todo
        # - ReturnConsumedCapacity: done
        # - ReturnItemCollectionMetrics: done
        # - ReturnValues: done
        data = {
            "TableName": self.__table_name,
            "Item": _convert_dict_to_db(self.__item),
        }
        data.update(self._build_return_values())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_return_item_collection_metrics())
        return data


class PutItemUnitTests(unittest.TestCase):
    def testItem(self):
        self.assertEqual(
            PutItem("Table", {"hash": "value"}).build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "value"}},
            }
        )

    def testReturnValuesNone(self):
        self.assertEqual(
            PutItem("Table", {"hash": "h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            PutItem("Table", {"hash": "h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnItemCollectionMetricsNone(self):
        self.assertEqual(
            PutItem("Table", {"hash": "h"}).return_item_collection_metrics_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


class PutItemIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.operations.admin_operations.CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(LowVoltage.operations.admin_operations.DeleteTable("Aaa"))

    def testSimplePut(self):
        r = self.connection.request(PutItem("Aaa", {"h": "simple"}))

        with cover("r", r) as r:
            self.assertEqual(r.attributes, None)

        self.assertEqual(self.connection.request(GetItem("Aaa", {"h": "simple"})).item, {"h": "simple"})

    def testPutAllTypes(self):
        # @todo B and BS
        self.connection.request(PutItem("Aaa", {
            "h": "all",
            "s": "foo",
            "b1": True,
            "b2": False,
            "n": 42,
            "null": None,
            "ns": set([42, 43]),
            "ss": set(["foo", "bar"]),
            "l": [True, 42],
            "m": {"a": True, "b": 42},
        }))

        self.assertEqual(
            self.connection.request(GetItem("Aaa", {"h": "all"})).item,
            {
                "h": "all",
                "s": "foo",
                "b1": True,
                "b2": False,
                "n": 42,
                "null": None,
                "ns": set([42, 43]),
                "ss": set(["foo", "bar"]),
                "l": [True, 42],
                "m": {"a": True, "b": 42},
            }
        )


    def testReturnOldValues(self):
        self.connection.request(PutItem("Aaa", {"h": "return", "a": "yyy"}))

        r = self.connection.request(
            PutItem("Aaa", {"h": "return", "b": "xxx"}).return_values_all_old()
        )

        with cover("r", r) as r:
            self.assertEqual(r.attributes, {"h": "return", "a": "yyy"})


class UpdateItem(_Operation, ReturnValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    class Result(object):
        def __init__(
            self,
            Attributes=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_ResponseElements
            # - Attributes: done
            # - ConsumedCapacity: @todo
            # - ItemCollectionMetrics: @todo
            self.attributes = None if Attributes is None else _convert_db_to_dict(Attributes)

    def __init__(self, table_name, key):
        super(UpdateItem, self).__init__("UpdateItem")
        self.__table_name = table_name
        self.__key = key
        self.__set = {}
        self.__values = {}
        ReturnValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_RequestParameters
        # - Key: done
        # - TableName: done
        # - AttributeUpdates: deprecated
        # - ConditionExpression: @todo
        # - ConditionalOperator: deprecated
        # - Expected: deprecated
        # - ExpressionAttributeNames: @todo
        # - ExpressionAttributeValues: @todo
        # - ReturnConsumedCapacity: done
        # - ReturnItemCollectionMetrics: done
        # - ReturnValues: done
        # - UpdateExpression: @todo (in progress)
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self._build_return_values())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_return_item_collection_metrics())
        update = []
        if self.__set:
            update.append("SET {}".format(", ".join("{}=:{}".format(n, v) for n, v in self.__set.iteritems())))
        if update:
            data["UpdateExpression"] = " ".join(update)
        if self.__values:
            data["ExpressionAttributeValues"] = {":" + n: _convert_value_to_db(v) for n, v in self.__values.iteritems()}
        return data

    def set(self, attribute_name, value_name):
        self.__set[attribute_name] = value_name
        return self

    def value(self, name, value):
        self.__values[name] = value
        return self


class UpdateItemUnitTests(unittest.TestCase):
    def testKey(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def testSet(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).set("a", "v").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "SET a=:v",
            }
        )

    def testSeveralSets(self):
        self.assertIn(
            UpdateItem("Table", {"hash": 42}).set("a", "v").set("b", "w").build(),
            [
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "SET a=:v, b=:w",
                },
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "SET b=:w, a=:v",
                }
            ]
        )

    def testValue(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).value("v", "value").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def testSeveralValues(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).value("v", "value").value("w", "walue").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}, ":w": {"S": "walue"}},
            }
        )

    def testReturnValuesNone(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnItemCollectionMetricsNone(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_item_collection_metrics_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


class UpdateItemIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.operations.admin_operations.CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(LowVoltage.operations.admin_operations.DeleteTable("Aaa"))

    def testSet(self):
        r = self.connection.request(
            UpdateItem("Aaa", {"h": "simple"}).set("a", "v").set("b", "w").value("v", "aaa").value("w", "bbb")
        )

        with cover("r", r) as r:
            self.assertEqual(r.attributes, None)

        self.assertEqual(
            self.connection.request(GetItem("Aaa", {"h": "simple"})).item,
            {"h": "simple", "a": "aaa", "b": "bbb"}
        )


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
