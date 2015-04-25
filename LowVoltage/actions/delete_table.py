# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`DeleteTable`, the connection will return a :class:`DeleteTableResponse`:

.. testsetup::

    table = "LowVoltage.Tests.Doc.DeleteTable.1"
    connection(CreateTable(table).hash_key("h", STRING).provisioned_throughput(1, 1))
    WaitForTableActivation(connection, table)

>>> r = connection(DeleteTable(table))
>>> r
<LowVoltage.actions.delete_table.DeleteTableResponse object at ...>
>>> r.table_description.table_status
u'DELETING'

Note that you can use :func:`.WaitForTableDeletion` to poll the table status until it's deleted.

.. testcleanup::

    WaitForTableDeletion(connection, table)
"""

import datetime

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .return_types import TableDescription, _is_dict


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
        if _is_dict(self.__table_description):  # pragma no branch (Defensive code)
            return TableDescription(**self.__table_description)


class DeleteTable(Action):
    """
    The `DeleteTable request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#API_DeleteTable_RequestParameters>`__.
    """

    def __init__(self, table_name):
        super(DeleteTable, self).__init__("DeleteTable", DeleteTableResponse)
        self.__table_name = table_name

    @property
    def payload(self):
        return {"TableName": self.__table_name}


class DeleteTableUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(DeleteTable("Foo").name, "DeleteTable")

    def test_build(self):
        self.assertEqual(DeleteTable("Foo").payload, {"TableName": "Foo"})


class DeleteTableLocalIntegTests(_tst.LocalIntegTests):
    def setUp(self):
        super(DeleteTableLocalIntegTests, self).setUp()
        self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )

    def test(self):
        r = self.connection(_lv.DeleteTable("Aaa"))

        self.assertDateTimeIsReasonable(r.table_description.creation_date_time)
        self.assertEqual(r.table_description.attribute_definitions[0].attribute_name, "h")
        self.assertEqual(r.table_description.attribute_definitions[0].attribute_type, "S")
        self.assertEqual(r.table_description.global_secondary_indexes, None)
        self.assertEqual(r.table_description.item_count, 0)
        self.assertEqual(r.table_description.key_schema[0].attribute_name, "h")
        self.assertEqual(r.table_description.key_schema[0].key_type, "HASH")
        self.assertEqual(r.table_description.local_secondary_indexes, None)
        self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, datetime.datetime(1970, 1, 1))
        self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, datetime.datetime(1970, 1, 1))
        self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
        self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 1)
        self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 2)
        self.assertEqual(r.table_description.table_name, "Aaa")
        self.assertEqual(r.table_description.table_size_bytes, 0)
        self.assertEqual(r.table_description.table_status, "ACTIVE")
