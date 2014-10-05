# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import numbers
import unittest


class Operation(object):
    def __init__(self, operation, connection):
        self.__operation = operation
        self.__connection = connection

    def go(self):
        return self.__connection.request(self.__operation, self._build())

    def _convert_dict(self, attributes):
        return {
            key: self._convert_value(val)
            for key, val in attributes.iteritems()
        }

    def _convert_value(self, value):
        if isinstance(value, basestring):
            return {"S": value}
        elif isinstance(value, numbers.Integral):
            return {"N": str(value)}
        else:
            assert len(value) > 0
            if isinstance(value[0], basestring):
                return {"SS": value}
            elif isinstance(value[0], numbers.Integral):
                return {"NS": [str(n) for n in value]}
            else:
                assert False  # pragma no cover


class ExpectedMixin(object):
    def __init__(self):
        self.__conditional_operator = None
        self.__expected = {}

    def _build_expected(self, data):
        if self.__conditional_operator:
            data["ConditionalOperator"] = self.__conditional_operator
        if self.__expected:
            data["Expected"] = self.__expected

    def conditional_operator_and(self):
        return self._set_conditional_operator("AND")

    def conditional_operator_or(self):
        return self._set_conditional_operator("OR")

    def _set_conditional_operator(self, value):
        self.__conditional_operator = value
        return self

    def expect_eq(self, name, value):
        return self._add_expected(name, "EQ", [value])

    def expect_ne(self, name, value):
        return self._add_expected(name, "NE", [value])

    def expect_le(self, name, value):
        return self._add_expected(name, "LE", [value])

    def expect_lt(self, name, value):
        return self._add_expected(name, "LT", [value])

    def expect_ge(self, name, value):
        return self._add_expected(name, "GE", [value])

    def expect_gt(self, name, value):
        return self._add_expected(name, "GT", [value])

    def expect_not_null(self, name):
        return self._add_expected(name, "NOT_NULL")

    def expect_null(self, name):
        return self._add_expected(name, "NULL")

    def expect_contains(self, name, value):
        return self._add_expected(name, "CONTAINS", [value])

    def expect_not_contains(self, name, value):
        return self._add_expected(name, "NOT_CONTAINS", [value])

    def expect_begins_with(self, name, value):
        return self._add_expected(name, "BEGINS_WITH", [value])

    def expect_in(self, name, values):
        return self._add_expected(name, "IN", values)

    def expect_between(self, name, low, high):
        return self._add_expected(name, "BETWEEN", [low, high])

    def _add_expected(self, name, operator, value_list=None):
        data = {"ComparisonOperator": operator}
        if value_list:
            data["AttributeValueList"] = [self._convert_value(value) for value in value_list]
        self.__expected[name] = data
        return self


class ReturnOldValuesMixin(object):
    def __init__(self):
        self.__return_values = None

    def _build_return_values(self, data):
        if self.__return_values:
            data["ReturnValues"] = self.__return_values

    def return_values_all_old(self):
        return self._set_return_values("ALL_OLD")

    def return_values_none(self):
        return self._set_return_values("NONE")

    def _set_return_values(self, value):
        self.__return_values = value
        return self


class ReturnValuesMixin(ReturnOldValuesMixin):
    def return_values_all_new(self):
        return self._set_return_values("ALL_NEW")

    def return_values_updated_new(self):
        return self._set_return_values("UPDATED_NEW")

    def return_values_updated_old(self):
        return self._set_return_values("UPDATED_OLD")


class ReturnConsumedCapacityMixin(object):
    def __init__(self):
        self.__return_consumed_capacity = None

    def _build_return_consumed_capacity(self, data):
        if self.__return_consumed_capacity:
            data["ReturnConsumedCapacity"] = self.__return_consumed_capacity

    def return_consumed_capacity_total(self):
        return self._set_return_consumed_capacity("TOTAL")

    def return_consumed_capacity_indexes(self):
        return self._set_return_consumed_capacity("INDEXES")

    def return_consumed_capacity_none(self):
        return self._set_return_consumed_capacity("NONE")

    def _set_return_consumed_capacity(self, value):
        self.__return_consumed_capacity = value
        return self


class ReturnItemCollectionMetricsMixin(object):
    def __init__(self):
        self.__return_item_collection_metrics = None

    def _build_return_item_collection_metrics(self, data):
        if self.__return_item_collection_metrics:
            data["ReturnItemCollectionMetrics"] = self.__return_item_collection_metrics

    def return_item_collection_metrics_size(self):
        return self._set_return_item_collection_metrics("SIZE")

    def return_item_collection_metrics_none(self):
        return self._set_return_item_collection_metrics("NONE")

    def _set_return_item_collection_metrics(self, value):
        self.__return_item_collection_metrics = value
        return self


class DeleteItem(Operation, ExpectedMixin, ReturnOldValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    def __init__(self, connection, table_name, key):
        super(DeleteItem, self).__init__("DeleteItem", connection)
        self.__table_name = table_name
        self.__key = key
        ExpectedMixin.__init__(self)
        ReturnOldValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)

    def _build(self):
        data = {
            "TableName": self.__table_name,
            "Key": self._convert_dict(self.__key),
        }
        self._build_expected(data)
        self._build_return_values(data)
        self._build_return_consumed_capacity(data)
        self._build_return_item_collection_metrics(data)
        return data


class DeleteItemTestCase(unittest.TestCase):
    def testKey(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": 42})._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def testConditionalOperatorAnd(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).conditional_operator_and()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConditionalOperator": "AND",
            }
        )

    def testConditionalOperatorOr(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).conditional_operator_or()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConditionalOperator": "OR",
            }
        )

    def testExpectEqual(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_eq("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotEqual(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_ne("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThanOrEqual(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_le("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThan(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_lt("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThanOrEqual(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_ge("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThan(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_gt("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotNull(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_not_null("attr")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_NULL"}},
            }
        )

    def testExpectNull(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_null("attr")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NULL"}},
            }
        )

    def testExpectContains(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_contains("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotContains(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_not_contains("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectBeginsWith(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_begins_with("attr", "prefix")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [{"S": "prefix"}]}},
            }
        )

    def testExpectIn(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_in("attr", [42, 43])._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "IN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )

    def testExpectBetween(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).expect_between("attr", 42, 43)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )

    def testReturnAllOldValues(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).return_values_all_old()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def testReturnNoValues(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).return_values_none()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnIndexesConsumedCapacity(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).return_consumed_capacity_indexes()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnTotalConsumedCapacity(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).return_consumed_capacity_total()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnNoConsumedCapacity(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).return_consumed_capacity_none()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnSizeItemCollectionMetrics(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).return_item_collection_metrics_size()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def testReturnNoItemCollectionMetrics(self):
        self.assertEqual(
            DeleteItem(None, "Table", {"hash": "h"}).return_item_collection_metrics_none()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


class PutItem(Operation, ExpectedMixin, ReturnOldValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    def __init__(self, connection, table_name, item):
        super(PutItem, self).__init__("PutItem", connection)
        self.__table_name = table_name
        self.__item = item
        ExpectedMixin.__init__(self)
        ReturnOldValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)

    def _build(self):
        data = {
            "TableName": self.__table_name,
            "Item": self._convert_dict(self.__item),
        }
        self._build_expected(data)
        self._build_return_values(data)
        self._build_return_consumed_capacity(data)
        self._build_return_item_collection_metrics(data)
        return data


class PutItemTestCase(unittest.TestCase):
    def testItem(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "value"})._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "value"}},
            }
        )

    def testConditionalOperatorAnd(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).conditional_operator_and()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ConditionalOperator": "AND",
            }
        )

    def testConditionalOperatorOr(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).conditional_operator_or()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ConditionalOperator": "OR",
            }
        )

    def testExpectEqual(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_eq("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotEqual(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_ne("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThanOrEqual(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_le("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThan(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_lt("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThanOrEqual(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_ge("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThan(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_gt("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotNull(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_not_null("attr")._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_NULL"}},
            }
        )

    def testExpectNull(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_null("attr")._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NULL"}},
            }
        )

    def testExpectContains(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_contains("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotContains(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_not_contains("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectBeginsWith(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_begins_with("attr", "prefix")._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [{"S": "prefix"}]}},
            }
        )

    def testExpectIn(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_in("attr", [42, 43])._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "IN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )

    def testExpectBetween(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_between("attr", 42, 43)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )

    def testReturnAllOldValues(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).return_values_all_old()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def testReturnNoValues(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).return_values_none()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnIndexesConsumedCapacity(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).return_consumed_capacity_indexes()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnTotalConsumedCapacity(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).return_consumed_capacity_total()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnNoConsumedCapacity(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).return_consumed_capacity_none()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnSizeItemCollectionMetrics(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).return_item_collection_metrics_size()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def testReturnNoItemCollectionMetrics(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).return_item_collection_metrics_none()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


class UpdateItem(Operation, ExpectedMixin, ReturnValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin):
    def __init__(self, connection, table_name, key):
        super(UpdateItem, self).__init__("UpdateItem", connection)
        self.__table_name = table_name
        self.__key = key
        self.__attribute_updates = {}
        ExpectedMixin.__init__(self)
        ReturnValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)

    def _build(self):
        data = {
            "TableName": self.__table_name,
            "Key": self._convert_dict(self.__key),
        }
        if self.__attribute_updates:
            data["AttributeUpdates"] = self.__attribute_updates
        self._build_expected(data)
        self._build_return_values(data)
        self._build_return_consumed_capacity(data)
        self._build_return_item_collection_metrics(data)
        return data

    def put(self, name, value):
        return self._add_attribute_update(name, "PUT", value)

    def delete(self, name, value=None):
        return self._add_attribute_update(name, "DELETE", value)

    def add(self, name, value):
        return self._add_attribute_update(name, "ADD", value)

    def _add_attribute_update(self, name, action, value):
        data = {"Action": action}
        if value is not None:
            data["Value"] = self._convert_value(value)
        self.__attribute_updates[name] = data
        return self


class UpdateItemTestCase(unittest.TestCase):
    def testKey(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": 42})._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def testPutInt(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).put("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "PUT", "Value": {"N": "42"}}},
            }
        )

    def testDelete(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).delete("attr")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "DELETE"}},
            }
        )

    def testAddInt(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).add("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "ADD", "Value": {"N": "42"}}},
            }
        )

    def testDeleteSetOfInts(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).delete("attr", [42, 43])._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "DELETE", "Value": {"NS": ["42", "43"]}}},
            }
        )

    def testAddSetOfStrings(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).add("attr", ["42", "43"])._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "ADD", "Value": {"SS": ["42", "43"]}}},
            }
        )

    def testConditionalOperatorAnd(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).conditional_operator_and()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConditionalOperator": "AND",
            }
        )

    def testConditionalOperatorOr(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).conditional_operator_or()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConditionalOperator": "OR",
            }
        )

    def testExpectEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_eq("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_ne("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThanOrEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_le("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThan(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_lt("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThanOrEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_ge("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThan(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_gt("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotNull(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_not_null("attr")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_NULL"}},
            }
        )

    def testExpectNull(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_null("attr")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NULL"}},
            }
        )

    def testExpectContains(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_contains("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotContains(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_not_contains("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectBeginsWith(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_begins_with("attr", "prefix")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [{"S": "prefix"}]}},
            }
        )

    def testExpectIn(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_in("attr", [42, 43])._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "IN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )

    def testExpectBetween(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_between("attr", 42, 43)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )

    def testReturnAllNewValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_values_all_new()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_NEW",
            }
        )

    def testReturnUpdatedNewValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_values_updated_new()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_NEW",
            }
        )

    def testReturnAllOldValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_values_all_old()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def testReturnUpdatedOldValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_values_updated_old()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_OLD",
            }
        )

    def testReturnNoValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_values_none()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnIndexesConsumedCapacity(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_consumed_capacity_indexes()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnTotalConsumedCapacity(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_consumed_capacity_total()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnNoConsumedCapacity(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_consumed_capacity_none()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnSizeItemCollectionMetrics(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_item_collection_metrics_size()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def testReturnNoItemCollectionMetrics(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_item_collection_metrics_none()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


if __name__ == "__main__":  # pragma no branch (Test code)
    unittest.main()  # pragma no cover (Test code)
