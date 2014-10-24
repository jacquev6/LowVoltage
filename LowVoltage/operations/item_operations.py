# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation, ExpectedMixin, ReturnOldValuesMixin, ReturnValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin
from LowVoltage.operations.operation import Operation as _Operation, OperationProxy as _OperationProxy
from LowVoltage.operations.conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value
import LowVoltage.tests.dynamodb_local
import LowVoltage.operations.admin_operations
import LowVoltage.return_types as _rtyp
import LowVoltage.attribute_types as _atyp
import LowVoltage.exceptions as _exn
from LowVoltage.tests.cover import cover


class DeleteItem(Operation, ExpectedMixin, ReturnOldValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    def __init__(self, table_name, key):
        super(DeleteItem, self).__init__("DeleteItem")
        self.__table_name = table_name
        self.__key = key
        ExpectedMixin.__init__(self)
        ReturnOldValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)

    def build(self):
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        self._build_expected(data)
        self._build_return_values(data)
        self._build_return_consumed_capacity(data)
        self._build_return_item_collection_metrics(data)
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

    def testConditionalOperatorAnd(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).conditional_operator_and().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConditionalOperator": "AND",
            }
        )

    def testConditionalOperatorOr(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).conditional_operator_or().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConditionalOperator": "OR",
            }
        )

    def testExpectEqual(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_eq("attr", 42).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotEqual(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_ne("attr", 42).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThanOrEqual(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_le("attr", 42).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThan(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_lt("attr", 42).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThanOrEqual(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_ge("attr", 42).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThan(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_gt("attr", 42).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotNull(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_not_null("attr").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_NULL"}},
            }
        )

    def testExpectNull(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_null("attr").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NULL"}},
            }
        )

    def testExpectContains(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_contains("attr", 42).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotContains(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_not_contains("attr", 42).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectBeginsWith(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_begins_with("attr", "prefix").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [{"S": "prefix"}]}},
            }
        )

    def testExpectIn(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_in("attr", [42, 43]).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "IN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )

    def testExpectBetween(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).expect_between("attr", 42, 43).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )

    def testReturnAllOldValues(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_values_all_old().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def testReturnNoValues(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnIndexesConsumedCapacity(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_consumed_capacity_indexes().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnTotalConsumedCapacity(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_consumed_capacity_total().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnNoConsumedCapacity(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnSizeItemCollectionMetrics(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_item_collection_metrics_size().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def testReturnNoItemCollectionMetrics(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": "h"}).return_item_collection_metrics_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


class GetItem(Operation, ReturnConsumedCapacityMixin):
    class Result(object):
        def __init__(
            self,
            Item=None,
            **dummy
        ):
            self.item = None if Item is None else _convert_db_to_dict(Item)
            # @todo ConsumedCapacity

    def __init__(self, table_name, key):
        super(GetItem, self).__init__("GetItem")
        self.__table_name = table_name
        self.__key = key
        ReturnConsumedCapacityMixin.__init__(self)
        self.__consistent_read = None
        self.__attributes_to_get = []

    def build(self):
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        self._build_return_consumed_capacity(data)
        if self.__consistent_read is not None:
            data["ConsistentRead"] = self.__consistent_read
        if self.__attributes_to_get:
            data["AttributesToGet"] = self.__attributes_to_get
        return data

    def consistent_read_true(self):
        return self._set_consistent_read(True)

    def consistent_read_false(self):
        return self._set_consistent_read(False)

    def _set_consistent_read(self, value):
        self.__consistent_read = value
        return self

    def attributes_to_get(self, *names):
        for name in names:
            if isinstance(name, basestring):
                name = [name]
            self.__attributes_to_get.extend(name)
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

    def testReturnIndexesConsumedCapacity(self):
        self.assertEqual(
            GetItem("Table", {"hash": "h"}).return_consumed_capacity_indexes().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnTotalConsumedCapacity(self):
        self.assertEqual(
            GetItem("Table", {"hash": "h"}).return_consumed_capacity_total().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnNoConsumedCapacity(self):
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

    def testOneAttributesToGet(self):
        self.assertEqual(
            GetItem("Table", {"hash": "h"}).attributes_to_get("a").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributesToGet": ["a"],
            }
        )

    def testSeveralAttributesToGet(self):
        self.assertEqual(
            GetItem("Table", {"hash": "h"}).attributes_to_get("a", "b").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributesToGet": ["a", "b"],
            }
        )

    def testListAttributesToGet(self):
        self.assertEqual(
            GetItem("Table", {"hash": "h"}).attributes_to_get(["a", "b"]).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributesToGet": ["a", "b"],
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
        self.connection.request(
            PutItem("Aaa", {"h": "get", "a": "yyy"})
        )

        r = self.connection.request(
            GetItem("Aaa", {"h": "get"})
        )

        with cover("r", r) as r:
            self.assertEqual(r.item, {"h": "get", "a": "yyy"})


class PutItem(_Operation, ReturnOldValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    class Result(object):
        def __init__(
            self,
            Attributes=None,
            **dummy
        ):
            self.attributes = None if Attributes is None else _convert_db_to_dict(Attributes)
            # @todo ConsumedCapacity and ItemCollectionMetrics

    def __init__(self, table_name, item):
        super(PutItem, self).__init__("PutItem")
        self.__table_name = table_name
        self.__item = item
        ReturnOldValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)

    def build(self):
        data = {
            "TableName": self.__table_name,
            "Item": _convert_dict_to_db(self.__item),
        }
        self._build_return_values(data)
        self._build_return_consumed_capacity(data)
        self._build_return_item_collection_metrics(data)
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

    def testReturnAllOldValues(self):
        self.assertEqual(
            PutItem("Table", {"hash": "h"}).return_values_all_old().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def testReturnNoValues(self):
        self.assertEqual(
            PutItem("Table", {"hash": "h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnIndexesConsumedCapacity(self):
        self.assertEqual(
            PutItem("Table", {"hash": "h"}).return_consumed_capacity_indexes().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnTotalConsumedCapacity(self):
        self.assertEqual(
            PutItem("Table", {"hash": "h"}).return_consumed_capacity_total().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnNoConsumedCapacity(self):
        self.assertEqual(
            PutItem("Table", {"hash": "h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnSizeItemCollectionMetrics(self):
        self.assertEqual(
            PutItem("Table", {"hash": "h"}).return_item_collection_metrics_size().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def testReturnNoItemCollectionMetrics(self):
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
            self.attributes = None if Attributes is None else _convert_db_to_dict(Attributes)
            # @todo ConsumedCapacity and ItemCollectionMetrics

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
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        self._build_return_values(data)
        self._build_return_consumed_capacity(data)
        self._build_return_item_collection_metrics(data)
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
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).set("a", "v").set("b", "w").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "SET a=:v, b=:w",
            }
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

    def testReturnAllNewValues(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_values_all_new().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_NEW",
            }
        )

    def testReturnUpdatedNewValues(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_values_updated_new().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_NEW",
            }
        )

    def testReturnAllOldValues(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_values_all_old().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def testReturnUpdatedOldValues(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_values_updated_old().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_OLD",
            }
        )

    def testReturnNoValues(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnIndexesConsumedCapacity(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_consumed_capacity_indexes().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnTotalConsumedCapacity(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_consumed_capacity_total().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnNoConsumedCapacity(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnSizeItemCollectionMetrics(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": "h"}).return_item_collection_metrics_size().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def testReturnNoItemCollectionMetrics(self):
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
    unittest.main()  # pragma no cover (Test code)
