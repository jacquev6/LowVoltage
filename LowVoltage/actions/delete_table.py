# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`DeleteTable`, the connection will return a :class:`DeleteTableResponse`:

.. testsetup::

    table = "LowVoltage.Tests.Doc.DeleteTable.1"
    table2 = "LowVoltage.Tests.Doc.DeleteTable.2"
    connection(CreateTable(table).hash_key("h", STRING).provisioned_throughput(1, 1))
    connection(CreateTable(table2).hash_key("h", STRING).provisioned_throughput(1, 1))
    wait_for_table_activation(connection, table)
    wait_for_table_activation(connection, table2)

>>> r = connection(DeleteTable(table))
>>> r
<LowVoltage.actions.delete_table.DeleteTableResponse ...>
>>> r.table_description.table_status
u'DELETING'

Note that you can use the :func:`.wait_for_table_deletion` compound to poll the table status until it's deleted. See :ref:`actions-vs-compounds` in the user guide.

.. testcleanup::

    wait_for_table_deletion(connection, table)
    wait_for_table_deletion(connection, table2)
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


class DeleteTableResponse(object):
    """
    The `DeleteTable response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#API_DeleteTable_ResponseElements>`__.
    """

    def __init__(
        self,
        TableDescription=None,
        **dummy
    ):
        self.__table_description = TableDescription

    @property
    def table_description(self):
        """
        The description of the table you just deleted.

        :type: ``None`` or :class:`.TableDescription`
        """
        if _is_dict(self.__table_description):
            return TableDescription(**self.__table_description)


class DeleteTable(Action):
    """
    The `DeleteTable request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#API_DeleteTable_RequestParameters>`__.
    """

    def __init__(self, table_name=None):
        """
        Passing ``table_name`` to the constructor is like calling :meth:`table_name` on the new instance.
        """
        super(DeleteTable, self).__init__("DeleteTable", DeleteTableResponse)
        self.__table_name = TableName(self, table_name)

    @property
    def payload(self):
        data = {}
        data.update(self.__table_name.payload)
        return data

    @proxy
    def table_name(self, table_name):
        """
        >>> connection(DeleteTable().table_name(table2))
        <LowVoltage.actions.delete_table.DeleteTableResponse ...>
        """
        return self.__table_name.set(table_name)


class DeleteTableUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(DeleteTable("Foo").name, "DeleteTable")

    def test_table_name(self):
        self.assertEqual(DeleteTable().table_name("Foo").payload, {"TableName": "Foo"})

    def test_constructor(self):
        self.assertEqual(DeleteTable("Foo").payload, {"TableName": "Foo"})


class DeleteTableResponseUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = DeleteTableResponse()
        self.assertIsNone(r.table_description)

    def test_all_set(self):
        r = DeleteTableResponse(TableDescription={})
        self.assertIsInstance(r.table_description, TableDescription)
