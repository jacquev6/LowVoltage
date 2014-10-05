# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation, ExpectedMixin, ReturnOldValuesMixin, ReturnValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin


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


class GetItem(Operation, ReturnConsumedCapacityMixin):
    def __init__(self, connection, table_name, key):
        super(GetItem, self).__init__("GetItem", connection)
        self.__table_name = table_name
        self.__key = key
        ReturnConsumedCapacityMixin.__init__(self)
        self.__consistent_read = None
        self.__attributes_to_get = []

    def _build(self):
        data = {
            "TableName": self.__table_name,
            "Key": self._convert_dict(self.__key),
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
                self.__attributes_to_get.append(name)
            else:
                self.__attributes_to_get.extend(name)
        return self


class GetItemTestCase(unittest.TestCase):
    def testKey(self):
        self.assertEqual(
            GetItem(None, "Table", {"hash": 42})._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def testReturnIndexesConsumedCapacity(self):
        self.assertEqual(
            GetItem(None, "Table", {"hash": "h"}).return_consumed_capacity_indexes()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def testReturnTotalConsumedCapacity(self):
        self.assertEqual(
            GetItem(None, "Table", {"hash": "h"}).return_consumed_capacity_total()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def testReturnNoConsumedCapacity(self):
        self.assertEqual(
            GetItem(None, "Table", {"hash": "h"}).return_consumed_capacity_none()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testConsistentReadTrue(self):
        self.assertEqual(
            GetItem(None, "Table", {"hash": "h"}).consistent_read_true()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": True,
            }
        )

    def testConsistentReadFalse(self):
        self.assertEqual(
            GetItem(None, "Table", {"hash": "h"}).consistent_read_false()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": False,
            }
        )

    def testOneAttributesToGet(self):
        self.assertEqual(
            GetItem(None, "Table", {"hash": "h"}).attributes_to_get("a")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributesToGet": ["a"],
            }
        )

    def testSeveralAttributesToGet(self):
        self.assertEqual(
            GetItem(None, "Table", {"hash": "h"}).attributes_to_get("a", "b")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributesToGet": ["a", "b"],
            }
        )

    def testListAttributesToGet(self):
        self.assertEqual(
            GetItem(None, "Table", {"hash": "h"}).attributes_to_get(["a", "b"])._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributesToGet": ["a", "b"],
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
