# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`GetItem`, the connection will return a :class:`GetItemResponse`:

>>> connection(GetItem(table, {"h": 0}))
<LowVoltage.actions.get_item.GetItemResponse object at ...>

The item is accessed like this:

>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'gr': 0, u'gh': 0}

Note that getting an unexisting item does not raise an exception:

>>> print connection(GetItem(table, {"h": -1})).item
None
"""

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_db_to_dict
from .next_gen_mixins import proxy
from .next_gen_mixins import (
    ConsistentRead,
    ExpressionAttributeNames,
    ProjectionExpression,
    ReturnConsumedCapacity,
)
from .return_types import ConsumedCapacity, _is_dict


class GetItemResponse(object):
    """
    The `GetItem response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_ResponseElements>`__
    """

    def __init__(
        self,
        ConsumedCapacity=None,
        Item=None,
        **dummy
    ):
        self.__consumed_capacity = ConsumedCapacity
        self.__item = Item

    @property
    def consumed_capacity(self):
        """
        The capacity consumed by the request. If you used :meth:`~GetItem.return_consumed_capacity_total`.

        :type: ``None`` or :class:`.ConsumedCapacity`
        """
        if _is_dict(self.__consumed_capacity):  # pragma no branch (Defensive code)
            return ConsumedCapacity(**self.__consumed_capacity)

    @property
    def item(self):
        """
        The item you just got. None if the item is not in the table.

        :type: ``None`` or dict
        """
        if _is_dict(self.__item):  # pragma no branch (Defensive code)
            return _convert_db_to_dict(self.__item)


class GetItem(Action):
    """
    The `GetItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_RequestParameters>`__
    """

    def __init__(self, table_name, key):
        super(GetItem, self).__init__("GetItem", GetItemResponse)
        self.__table_name = table_name
        self.__key = key
        self.__consistent_read = ConsistentRead(self)
        self.__expression_attribute_names = ExpressionAttributeNames(self)
        self.__projection_expression = ProjectionExpression(self)
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)

    @property
    def payload(self):
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self.__consistent_read.payload)
        data.update(self.__expression_attribute_names.payload)
        data.update(self.__projection_expression.payload)
        data.update(self.__return_consumed_capacity.payload)
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
    def project(self, *names):
        """
        >>> connection(
        ...   GetItem(table, {"h": 0})
        ...     .project("gr")
        ... ).item
        {u'gr': 0}
        """
        return self.__projection_expression.add(*names)

    @proxy
    def expression_attribute_name(self, synonym, name):
        """
        >>> connection(
        ...   GetItem(table, {"h": 0})
        ...     .expression_attribute_name("syn", "gr")
        ...     .project("#syn")
        ... ).item
        {u'gr': 0}
        """
        return self.__expression_attribute_names.add(synonym, name)

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
            GetItem("Table", {"hash": 42}).payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def test_return_consumed_capacity_none(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).return_consumed_capacity_none().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def test_return_consumed_capacity_total(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).return_consumed_capacity_total().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def test_consistent_read_true(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).consistent_read_true().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": True,
            }
        )

    def test_consistent_read_false(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).consistent_read_false().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConsistentRead": False,
            }
        )

    def test_project(self):
        self.assertEqual(
            GetItem("Table", {"hash": u"h"}).project("abc").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ProjectionExpression": "abc",
            }
        )

    def test_expression_attribute_name(self):
        self.assertEqual(
            GetItem("Table", {"hash": 42}).expression_attribute_name("n", "path").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )


class GetItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_simple_get(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"get", "a": "yyy"}))

        r = self.connection(_lv.GetItem("Aaa", {"h": u"get"}))

        self.assertEqual(r.item, {"h": "get", "a": "yyy"})

    def test_get_with_projections(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"attrs", "a": "yyy", "b": {"c": ["d1", "d2", "d3"]}, "e": 42, "f": "nope"}))

        r = self.connection(_lv.GetItem("Aaa", {"h": u"attrs"}).project("b.c[1]", "e"))

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

        self.assertEqual(r.consumed_capacity.capacity_units, 0.5)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.table, None)
        self.assertEqual(r.consumed_capacity.table_name, self.table)
