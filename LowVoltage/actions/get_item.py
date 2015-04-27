# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`GetItem`, the connection will return a :class:`GetItemResponse`:

>>> connection(GetItem(table, {"h": 0}))
<LowVoltage.actions.get_item.GetItemResponse ...>

The item is accessed like this:

>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'gr': 10, u'gh': 0}

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
    Key,
    ProjectionExpression,
    ReturnConsumedCapacity,
    TableName,
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
        if _is_dict(self.__consumed_capacity):
            return ConsumedCapacity(**self.__consumed_capacity)

    @property
    def item(self):
        """
        The item you just got. None if the item is not in the table.

        :type: ``None`` or dict
        """
        if _is_dict(self.__item):
            return _convert_db_to_dict(self.__item)


class GetItem(Action):
    """
    The `GetItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_RequestParameters>`__
    """

    def __init__(self, table_name=None, key=None):
        """
        Passing ``table_name`` to the constructor is like calling :meth:`table_name` on the new instance.
        Passing ``key`` to the constructor is like calling :meth:`key` on the new instance.
        """
        super(GetItem, self).__init__("GetItem", GetItemResponse)
        self.__consistent_read = ConsistentRead(self)
        self.__expression_attribute_names = ExpressionAttributeNames(self)
        self.__key = Key(self, key)
        self.__projection_expression = ProjectionExpression(self)
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__table_name = TableName(self, table_name)

    @property
    def payload(self):
        data = {}
        data.update(self.__consistent_read.payload)
        data.update(self.__expression_attribute_names.payload)
        data.update(self.__key.payload)
        data.update(self.__projection_expression.payload)
        data.update(self.__return_consumed_capacity.payload)
        data.update(self.__table_name.payload)
        return data

    @proxy
    def table_name(self, table_name):
        """
        >>> connection(
        ...   GetItem(key={"h": 0})
        ...     .table_name(table)
        ... )
        <LowVoltage.actions.get_item.GetItemResponse ...>
        """
        return self.__table_name.set(table_name)

    @proxy
    def key(self, key):
        """
        >>> connection(
        ...   GetItem(table_name=table)
        ...     .key({"h": 0})
        ... )
        <LowVoltage.actions.get_item.GetItemResponse ...>
        """
        return self.__key.set(key)

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
    def expression_attribute_name(self, synonym, name):
        """
        >>> connection(
        ...   GetItem(table, {"h": 0})
        ...     .expression_attribute_name("syn", "gr")
        ...     .project("#syn")
        ... ).item
        {u'gr': 10}
        """
        return self.__expression_attribute_names.add(synonym, name)

    @proxy
    def project(self, *names):
        """
        >>> connection(
        ...   GetItem(table, {"h": 0})
        ...     .project("gr")
        ... ).item
        {u'gr': 10}
        """
        return self.__projection_expression.add(*names)

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

    def test_missing_table_name(self):
        with self.assertRaises(_lv.BuilderError):
            GetItem().key({"h": 42}).payload

    def test_bad_table_name(self):
        with self.assertRaises(TypeError):
            GetItem().table_name(42)

    def test_bad_key(self):
        with self.assertRaises(TypeError):
            GetItem().key(42)

    def test_missing_key(self):
        with self.assertRaises(_lv.BuilderError):
            GetItem().table_name("Foo").payload

    def test_table_name_and_key(self):
        self.assertEqual(
            GetItem().table_name("Table").key({"hash": 42}).payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def test_constructor(self):
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


class GetItemResponseUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = GetItemResponse()
        self.assertIsNone(r.consumed_capacity)
        self.assertIsNone(r.item)

    def test_all_set(self):
        unprocessed_keys = object()
        r = GetItemResponse(ConsumedCapacity={}, Item={"h": {"S": "a"}})
        self.assertIsInstance(r.consumed_capacity, ConsumedCapacity)
        self.assertEqual(r.item, {"h": u"a"})
