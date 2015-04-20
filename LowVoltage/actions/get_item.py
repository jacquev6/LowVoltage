# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value
from .expression_mixins import ExpressionAttributeNamesMixin, ProjectionExpressionMixin
from .return_mixins import ReturnConsumedCapacityMixin
from .return_types import ConsumedCapacity_, _is_dict


class GetItem(
    Action,
    ReturnConsumedCapacityMixin,
    ExpressionAttributeNamesMixin,
    ProjectionExpressionMixin,
):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_RequestParameters"""

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_ResponseElements"""

        def __init__(
            self,
            ConsumedCapacity=None,
            Item=None,
            **dummy
        ):
            self.consumed_capacity = None
            if _is_dict(ConsumedCapacity):  # pragma no branch (Defensive code)
                self.consumed_capacity = ConsumedCapacity_(**ConsumedCapacity)

            self.item = None
            if _is_dict(Item):  # pragma no branch (Defensive code)
                self.item = _convert_db_to_dict(Item)

    def __init__(self, table_name, key):
        super(GetItem, self).__init__("GetItem")
        self.__table_name = table_name
        self.__key = key
        ReturnConsumedCapacityMixin.__init__(self)
        ExpressionAttributeNamesMixin.__init__(self)
        ProjectionExpressionMixin.__init__(self)
        self.__consistent_read = None

    def build(self):
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_expression_attribute_names())
        data.update(self._build_projection_expression())
        if self.__consistent_read is not None:
            data["ConsistentRead"] = self.__consistent_read
        return data

    def consistent_read_true(self):
        return self._set_consistent_read(True)

    def consistent_read_false(self):
        return self._set_consistent_read(False)

    def _set_consistent_read(self, value):
        self.__consistent_read = value
        return self


class GetItemUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(GetItem("Table", {"hash": 42}).name, "GetItem")

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
            GetItem("Table", {"hash": u"h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testConsistentReadTrue(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).consistent_read_true().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": True,
            }
        )

    def testConsistentReadFalse(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).consistent_read_false().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": False,
            }
        )

    def testProject(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).project("abc").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ProjectionExpression": "abc",
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


class GetItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def testSimpleGet(self):
        self.connection.request(_lv.PutItem("Aaa", {"h": u"get", "a": "yyy"}))

        r = self.connection.request(_lv.GetItem("Aaa", {"h": u"get"}))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item, {"h": "get", "a": "yyy"})

    def testGetWithProjections(self):
        self.connection.request(_lv.PutItem("Aaa", {"h": u"attrs", "a": "yyy", "b": {"c": ["d1", "d2", "d3"]}, "e": 42, "f": "nope"}))

        r = self.connection.request(_lv.GetItem("Aaa", {"h": u"attrs"}).project("b.c[1]", "e"))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item, {"b": {"c": ["d2"]}, "e": 42})

    def test_unexisting_table(self):
        with self.assertRaises(_lv.ResourceNotFoundException):
            self.connection.request(_lv.GetItem("Bbb", {}))

    def test_bad_key_type(self):
        with self.assertRaises(_lv.ValidationException):
            self.connection.request(_lv.GetItem("Aaa", {"h": 42}))


class GetItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(GetItemConnectedIntegTests, self).setUp()
        self.connection.request(_lv.PutItem(self.table, self.item))

    def tearDown(self):
        self.connection.request(_lv.DeleteItem(self.table, self.tab_key))
        super(GetItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_total(self):
        r = self.connection.request(_lv.GetItem(self.table, self.tab_key).return_consumed_capacity_total())
        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity.capacity_units, 0.5)
            self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.table, None)
            self.assertEqual(r.consumed_capacity.table_name, self.table)
            self.assertEqual(r.item, self.item)
