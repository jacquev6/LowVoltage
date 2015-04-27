# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`DescribeTable`, the connection will return a :class:`DescribeTableResponse`:

>>> r = connection(DescribeTable(table))
>>> r
<LowVoltage.actions.describe_table.DescribeTableResponse ...>
>>> r.table.table_status
u'ACTIVE'
>>> r.table.key_schema[0].attribute_name
u'h'
>>> r.table.key_schema[0].key_type
u'HASH'
"""

import datetime

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .return_types import TableDescription, _is_dict
from .next_gen_mixins import proxy
from .next_gen_mixins import (
    TableName,
)


class DescribeTableResponse(object):
    """
    The `DescribeTable response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html#API_DescribeTable_ResponseElements>`__.
    """

    def __init__(
        self,
        Table=None,
        **dummy
    ):
        self.__table = Table

    @property
    def table(self):
        """
        The description of the table.

        :type: ``None`` or :class:`.TableDescription`
        """
        if _is_dict(self.__table):
            return TableDescription(**self.__table)


class DescribeTable(Action):
    """
    The `DescribeTable request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html#API_DescribeTable_RequestParameters>`__.
    """

    def __init__(self, table_name=None):
        """
        Passing ``table_name`` to the constructor is like calling :meth:`table_name` on the new instance.
        """
        super(DescribeTable, self).__init__("DescribeTable", DescribeTableResponse)
        self.__table_name = TableName(self, table_name)

    @property
    def payload(self):
        data = {}
        data.update(self.__table_name.payload)
        return data

    @proxy
    def table_name(self, table_name):
        """
        >>> connection(DescribeTable().table_name(table))
        <LowVoltage.actions.describe_table.DescribeTableResponse ...>
        """
        return self.__table_name.set(table_name)


class DescribeTableUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(DescribeTable("Foo").name, "DescribeTable")

    def test_table_name(self):
        self.assertEqual(DescribeTable().table_name("Foo").payload, {"TableName": "Foo"})

    def test_constuctor(self):
        self.assertEqual(DescribeTable("Foo").payload, {"TableName": "Foo"})


class DescribeTableResponseUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = DescribeTableResponse()
        self.assertIsNone(r.table)

    def test_all_set(self):
        r = DescribeTableResponse(Table={})
        self.assertIsInstance(r.table, TableDescription)
