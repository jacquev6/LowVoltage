# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value
from .expression_mixins import ExpressionAttributeNamesMixin, ProjectionExpressionMixin
from .next_gen_mixins import proxy, ReturnConsumedCapacity, ConsistentRead
from .return_types import ConsumedCapacity_, _is_dict


class GetItem(
    Action,
    ExpressionAttributeNamesMixin,
    ProjectionExpressionMixin,
):
    """
    The `GetItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_RequestParameters>`__

    >>> connection(GetItem(table, {"h": 0})).item
    {u'h': 0, u'gr': 0, u'gh': 0}

    Note that getting an unexisting item does not raise an exception:

    >>> print connection(GetItem(table, {"h": -1})).item
    None
    """

    class Result(object):
        """
        The `GetItem response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_ResponseElements>`__
        """

        def __init__(
            self,
            ConsumedCapacity=None,
            Item=None,
            **dummy
        ):
            self.consumed_capacity = None
            "@todo Document"
            if _is_dict(ConsumedCapacity):  # pragma no branch (Defensive code)
                self.consumed_capacity = ConsumedCapacity_(**ConsumedCapacity)

            self.item = None
            "@todo Document"
            if _is_dict(Item):  # pragma no branch (Defensive code)
                self.item = _convert_db_to_dict(Item)

    def __init__(self, table_name, key):
        super(GetItem, self).__init__("GetItem")
        self.__table_name = table_name
        self.__key = key
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        ExpressionAttributeNamesMixin.__init__(self)
        ProjectionExpressionMixin.__init__(self)
        self.__consistent_read = ConsistentRead(self)

    def build(self):
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self.__return_consumed_capacity.build())
        data.update(self._build_expression_attribute_names())
        data.update(self._build_projection_expression())
        data.update(self.__consistent_read.build())
        return data

    @proxy
    def consistent_read_true(self):
        """
        >>> connection(
        ...   GetItem(table, {"h": 0})
        ...     .consistent_read_true()
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        1.0
        """
        return self.__consistent_read.true()

    @proxy
    def consistent_read_false(self):
        """
        >>> connection(
        ...   GetItem(table, {"h": 0})
        ...     .consistent_read_false()
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        0.5
        """
        return self.__consistent_read.false()

    @proxy
    def return_consumed_capacity_total(self):
        """
        >>> connection(
        ...   GetItem(table, {"h": 0})
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        0.5
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_none(self):
        """
        >>> print connection(
        ...   GetItem(table, {"h": 0})
        ...     .return_consumed_capacity_none()
        ... ).consumed_capacity
        None
        """
        return self.__return_consumed_capacity.none()


class GetItemUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(GetItem("Table", {"hash": 42}).name, "GetItem")

    def test_key(self):
        self.assertEqual(
            GetItem("Table", {"hash": 42}).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def test_return_consumed_capacity_none(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def test_return_consumed_capacity_total(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).return_consumed_capacity_total().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def test_consistent_read_true(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).consistent_read_true().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": True,
            }
        )

    def test_consistent_read_false(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).consistent_read_false().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": False,
            }
        )

    def test_project(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).project("abc").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ProjectionExpression": "abc",
            }
        )

    def test_expression_attribute_name(self):
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
        self.connection(_lv.PutItem("Aaa", {"h": u"get", "a": "yyy"}))

        r = self.connection(_lv.GetItem("Aaa", {"h": u"get"}))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item, {"h": "get", "a": "yyy"})

    def testGetWithProjections(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"attrs", "a": "yyy", "b": {"c": ["d1", "d2", "d3"]}, "e": 42, "f": "nope"}))

        r = self.connection(_lv.GetItem("Aaa", {"h": u"attrs"}).project("b.c[1]", "e"))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item, {"b": {"c": ["d2"]}, "e": 42})

    def test_unexisting_table(self):
        with self.assertRaises(_lv.ResourceNotFoundException):
            self.connection(_lv.GetItem("Bbb", {}))

    def test_bad_key_type(self):
        with self.assertRaises(_lv.ValidationException):
            self.connection(_lv.GetItem("Aaa", {"h": 42}))


class GetItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(GetItemConnectedIntegTests, self).setUp()
        self.connection(_lv.PutItem(self.table, self.item))

    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(GetItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_total(self):
        r = self.connection(_lv.GetItem(self.table, self.tab_key).return_consumed_capacity_total())
        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity.capacity_units, 0.5)
            self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.table, None)
            self.assertEqual(r.consumed_capacity.table_name, self.table)
            self.assertEqual(r.item, self.item)
