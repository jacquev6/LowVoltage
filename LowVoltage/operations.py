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


class PutItem(Operation):
    def __init__(self, connection, table_name, item):
        super(PutItem, self).__init__("PutItem", connection)
        self.__table_name = table_name
        self.__item = item
        self.__conditional_operator = None
        self.__expected = {}
        self.__return_values = None

    def _build(self):
        data = {
            "TableName": self.__table_name,
            "Item": self._convert_dict(self.__item),
        }
        if self.__conditional_operator:
            data["ConditionalOperator"] = self.__conditional_operator
        if self.__expected:
            data["Expected"] = self.__expected
        if self.__return_values:
            data["ReturnValues"] = self.__return_values
        return data

    def conditional_operator_and(self):
        self.__conditional_operator = "AND"
        return self

    def conditional_operator_or(self):
        self.__conditional_operator = "OR"
        return self

    def expect_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "EQ", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_not_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "NE", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_less_than_or_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "LE", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_less_than(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "LT", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_greater_than_or_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "GE", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_greater_than(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "GT", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_not_null(self, name):
        self.__expected[name] = {"ComparisonOperator": "NOT_NULL"}
        return self

    def expect_null(self, name):
        self.__expected[name] = {"ComparisonOperator": "NULL"}
        return self

    def expect_contains(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "CONTAINS", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_not_contains(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "NOT_CONTAINS", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_begins_with(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_in(self, name, values):
        self.__expected[name] = {"ComparisonOperator": "IN", "AttributeValueList": [self._convert_value(value) for value in values]}
        return self

    def expect_between(self, name, low, high):
        self.__expected[name] = {"ComparisonOperator": "BETWEEN", "AttributeValueList": [self._convert_value(low), self._convert_value(high)]}
        return self

    def return_all_old_values(self):
        self.__return_values = "ALL_OLD"
        return self

    def return_no_values(self):
        self.__return_values = "NONE"
        return self


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
            PutItem(None, "Table", {"hash": "h"}).expect_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotEqual(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_not_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThanOrEqual(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_less_than_or_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThan(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_less_than("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThanOrEqual(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_greater_than_or_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThan(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).expect_greater_than("attr", 42)._build(),
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
            PutItem(None, "Table", {"hash": "h"}).return_all_old_values()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def testReturnNoValues(self):
        self.assertEqual(
            PutItem(None, "Table", {"hash": "h"}).return_no_values()._build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )


class UpdateItem(Operation):
    def __init__(self, connection, table_name, key):
        super(UpdateItem, self).__init__("UpdateItem", connection)
        self.__table_name = table_name
        self.__key = key
        self.__attribute_updates = {}
        self.__conditional_operator = None
        self.__expected = {}
        self.__return_values = None

    def _build(self):
        data = {
            "TableName": self.__table_name,
            "Key": self._convert_dict(self.__key),
        }
        if self.__attribute_updates:
            data["AttributeUpdates"] = self.__attribute_updates
        if self.__conditional_operator:
            data["ConditionalOperator"] = self.__conditional_operator
        if self.__expected:
            data["Expected"] = self.__expected
        if self.__return_values:
            data["ReturnValues"] = self.__return_values
        return data

    def put(self, name, value):
        self.__attribute_updates[name] = {"Action": "PUT", "Value": self._convert_value(value)}
        return self

    def delete(self, name, value=None):
        self.__attribute_updates[name] = {"Action": "DELETE"}
        if value is not None:
            self.__attribute_updates[name]["Value"] = self._convert_value(value)
        return self

    def add(self, name, value):
        self.__attribute_updates[name] = {"Action": "ADD", "Value": self._convert_value(value)}
        return self

    def conditional_operator_and(self):
        self.__conditional_operator = "AND"
        return self

    def conditional_operator_or(self):
        self.__conditional_operator = "OR"
        return self

    def expect_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "EQ", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_not_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "NE", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_less_than_or_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "LE", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_less_than(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "LT", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_greater_than_or_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "GE", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_greater_than(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "GT", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_not_null(self, name):
        self.__expected[name] = {"ComparisonOperator": "NOT_NULL"}
        return self

    def expect_null(self, name):
        self.__expected[name] = {"ComparisonOperator": "NULL"}
        return self

    def expect_contains(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "CONTAINS", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_not_contains(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "NOT_CONTAINS", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_begins_with(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_in(self, name, values):
        self.__expected[name] = {"ComparisonOperator": "IN", "AttributeValueList": [self._convert_value(value) for value in values]}
        return self

    def expect_between(self, name, low, high):
        self.__expected[name] = {"ComparisonOperator": "BETWEEN", "AttributeValueList": [self._convert_value(low), self._convert_value(high)]}
        return self

    def return_all_new_values(self):
        self.__return_values = "ALL_NEW"
        return self

    def return_updated_new_values(self):
        self.__return_values = "UPDATED_NEW"
        return self

    def return_all_old_values(self):
        self.__return_values = "ALL_OLD"
        return self

    def return_updated_old_values(self):
        self.__return_values = "UPDATED_OLD"
        return self

    def return_no_values(self):
        self.__return_values = "NONE"
        return self


class UpdateItemTestCase(unittest.TestCase):
    def testStringKey(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "value"})._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "value"}},
            }
        )

    def testIntKey(self):
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
            UpdateItem(None, "Table", {"hash": "h"}).expect_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_not_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThanOrEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_less_than_or_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThan(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_less_than("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThanOrEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_greater_than_or_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThan(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_greater_than("attr", 42)._build(),
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
            UpdateItem(None, "Table", {"hash": "h"}).return_all_new_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_NEW",
            }
        )

    def testReturnUpdatedNewValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_updated_new_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_NEW",
            }
        )

    def testReturnAllOldValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_all_old_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def testReturnUpdatedOldValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_updated_old_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_OLD",
            }
        )

    def testReturnNoValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_no_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )


if __name__ == "__main__":  # pragma no branch (Test code)
    unittest.main()  # pragma no cover (Test code)
