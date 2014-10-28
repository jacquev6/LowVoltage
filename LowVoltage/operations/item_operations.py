# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation as _Operation
from LowVoltage.operations.return_mixins import (
    ReturnValuesMixin, ReturnOldValuesMixin,
    ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin,
)
from LowVoltage.operations.expression_mixins import (
    ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin,
    ConditionExpressionMixin,
)
from LowVoltage.operations.conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value
import LowVoltage.tests.dynamodb_local
import LowVoltage.operations.admin_operations
import LowVoltage.return_types as _rtyp
import LowVoltage.attribute_types as _atyp
import LowVoltage.exceptions as _exn
from LowVoltage.tests.cover import cover


class DeleteItem(_Operation,
    ReturnOldValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin,
    ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ConditionExpressionMixin,
):
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
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ConditionExpressionMixin.__init__(self)

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html#API_DeleteItem_RequestParameters
        # - Key: done
        # - TableName: done
        # - ConditionExpression: done
        # - ConditionalOperator: deprecated
        # - Expected: deprecated
        # - ExpressionAttributeNames: done
        # - ExpressionAttributeValues: done
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
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_condition_expression())
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

    def testExpressionAttributeValue(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).expression_attribute_value("v", "value").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def testExpressionAttributeName(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).expression_attribute_name("n", "path").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def testConditionExpression(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).condition_expression("a=b").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
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


class GetItem(_Operation,
    ReturnConsumedCapacityMixin, ExpressionAttributeNamesMixin,
):
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
        ExpressionAttributeNamesMixin.__init__(self)
        self.__consistent_read = None
        self.__projections = []

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_RequestParameters
        # - Key: done
        # - TableName: done
        # - AttributesToGet: deprecated
        # - ConsistentRead: done
        # - ExpressionAttributeNames: done
        # - ProjectionExpression: done
        # - ReturnConsumedCapacity: done
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_expression_attribute_names())
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

    def testExpressionAttributeName(self):
        self.assertEqual(
            GetItem("Table", {"hash": 42}).expression_attribute_name("n", "path").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
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


class PutItem(_Operation,
    ReturnOldValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin,
    ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ConditionExpressionMixin,
):
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
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ConditionExpressionMixin.__init__(self)

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_RequestParameters
        # - Item: done
        # - TableName: done
        # - ConditionExpression: done
        # - ConditionalOperator: deprecated
        # - Expected: deprecated
        # - ExpressionAttributeNames: done
        # - ExpressionAttributeValues: done
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
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_condition_expression())
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

    def testExpressionAttributeValue(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).expression_attribute_value("v", "value").build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def testExpressionAttributeName(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).expression_attribute_name("n", "path").build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def testConditionExpression(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).condition_expression("a=b").build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
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


class UpdateItem(_Operation,
    ReturnValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin,
    ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ConditionExpressionMixin,
):
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
        self.__remove = []
        self.__add = {}
        self.__delete = {}
        ReturnValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ConditionExpressionMixin.__init__(self)

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_RequestParameters
        # - Key: done
        # - TableName: done
        # - AttributeUpdates: deprecated
        # - ConditionExpression: done
        # - ConditionalOperator: deprecated
        # - Expected: deprecated
        # - ExpressionAttributeNames: done
        # - ExpressionAttributeValues: done
        # - ReturnConsumedCapacity: done
        # - ReturnItemCollectionMetrics: done
        # - ReturnValues: done
        # - UpdateExpression: done
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self._build_return_values())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_return_item_collection_metrics())
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_condition_expression())
        update = []
        if self.__set:
            # http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.SET
            update.append("SET {}".format(", ".join("{}=:{}".format(n, v) for n, v in self.__set.iteritems())))
        if self.__remove:
            # http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.REMOVE
            update.append("REMOVE {}".format(", ".join(self.__remove)))
        if self.__add:
            # http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.ADD
            update.append("ADD {}".format(", ".join("{} :{}".format(n, v) for n, v in self.__add.iteritems())))
        if self.__delete:
            # http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.DELETE
            update.append("DELETE {}".format(", ".join("{} :{}".format(n, v) for n, v in self.__delete.iteritems())))
        if update:
            data["UpdateExpression"] = " ".join(update)
        return data

    def set(self, attribute_name, value_name):
        self.__set[attribute_name] = value_name
        return self

    def remove(self, path):
        self.__remove.append(path)
        return self

    def add(self, attribute_name, value_name):
        self.__add[attribute_name] = value_name
        return self

    def delete(self, attribute_name, value_name):
        self.__delete[attribute_name] = value_name
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

    def testRemove(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).remove("a").remove("b").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "REMOVE a, b",
            }
        )

    def testAdd(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).add("a", "v").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "ADD a :v",
            }
        )

    def testSeveralAdds(self):
        self.assertIn(
            UpdateItem("Table", {"hash": 42}).add("a", "v").add("b", "w").build(),
            [
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "ADD a :v, b :w",
                },
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "ADD b :w, a :v",
                }
            ]
        )

    def testDelete(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).delete("a", "v").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "DELETE a :v",
            }
        )

    def testSeveralDeletes(self):
        self.assertIn(
            UpdateItem("Table", {"hash": 42}).delete("a", "v").delete("b", "w").build(),
            [
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "DELETE a :v, b :w",
                },
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "DELETE b :w, a :v",
                }
            ]
        )

    def testExpressionAttributeValue(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).expression_attribute_value("v", "value").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def testExpressionAttributeName(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).expression_attribute_name("n", "path").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def testConditionExpression(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).condition_expression("a=b").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
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
            UpdateItem("Aaa", {"h": "set"})
                .set("a", "v")
                .set("#p", "w")
                .expression_attribute_value("v", "aaa")
                .expression_attribute_value("w", "bbb")
                .expression_attribute_name("p", "b")
        )

        with cover("r", r) as r:
            self.assertEqual(r.attributes, None)

        self.assertEqual(
            self.connection.request(GetItem("Aaa", {"h": "set"})).item,
            {"h": "set", "a": "aaa", "b": "bbb"}
        )

    def testComplexUpdate(self):
        self.connection.request(
            PutItem(
                "Aaa",
                {
                    "h": "complex",
                    "a": "a",
                    "b": "b",
                    "c": "c",
                    "d": set([41, 43]),
                    "e": 42,
                    "f": set([41, 42, 43]),
                    "g": set([39, 40]),
                }
            )
        )

        r = self.connection.request(
            UpdateItem("Aaa", {"h": "complex"})
                .set("a", "s")
                .set("b", "i")
                .remove("c")
                .add("d", "s")
                .add("e", "i")
                .delete("f", "s")
                .delete("g", "s")
                .expression_attribute_value("s", set([42, 43]))
                .expression_attribute_value("i", 52)
                .return_values_all_new()
        )

        with cover("r", r) as r:
            self.assertEqual(
                r.attributes,
                {
                    "h": "complex",
                    "a": set([42, 43]),
                    "b": 52,
                    "d": set([41, 42, 43]),
                    "e": 94,
                    "f": set([41]),
                    "g": set([39, 40]),
                }
            )

    def testConditionExpression(self):
        self.connection.request(PutItem("Aaa", {"h": "expr", "a": 42, "b": 42}))

        r = self.connection.request(
            UpdateItem("Aaa", {"h": "expr"})
                .set("checked", "true")
                .expression_attribute_value("true", True)
                .condition_expression("a=b")
                .return_values_all_new()
        )

        with cover("r", r) as r:
            self.assertEqual(
                r.attributes,
                {"h": "expr", "a": 42, "b": 42, "checked": True}
            )


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
