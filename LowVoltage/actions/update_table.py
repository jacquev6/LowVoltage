# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`UpdateTable`, the connection will return a :class:`UpdateTableResponse`:

.. testsetup::

    table = "LowVoltage.Tests.Doc.UpdateTable.1"
    connection(CreateTable(table).hash_key("h", STRING).provisioned_throughput(1, 1))
    WaitForTableActivation(connection, table)

>>> r = connection(
...   UpdateTable(table)
...     .provisioned_throughput(2, 2)
... )
>>> r
<LowVoltage.actions.update_table.UpdateTableResponse object at ...>
>>> r.table_description.table_status
u'UPDATING'

Note that you can use :func:`.WaitForTableActivation` to poll the table status until it's updated.

.. testcleanup::

    WaitForTableActivation(connection, table)
    connection(DeleteTable(table))
    WaitForTableDeletion(connection, table)
"""

import datetime

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action, ActionProxy
from .return_types import TableDescription, _is_dict


class UpdateTableResponse(object):
    """
    The `UpdateTable response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html#API_UpdateTable_ResponseElements>`__.
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
        The description of the table you just updated.

        :type: None or :class:`.TableDescription`
        """
        if _is_dict(self.__table_description):  # pragma no branch (Defensive code)
            return TableDescription(**self.__table_description)


class UpdateTable(Action):
    """
    The `UpdateTable request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html#API_UpdateTable_RequestParameters>`__.
    """

    # @todo Create and delete secondary indexes!
    # @todo Remove the proxy. Document methods on UpdateTable level.

    def __init__(self, table_name):
        super(UpdateTable, self).__init__("UpdateTable", UpdateTableResponse)
        self.__table_name = table_name
        self.__read_capacity_units = None
        self.__write_capacity_units = None
        self.__gsis = {}

    @property
    def payload(self):
        data = {"TableName": self.__table_name}
        throughput = {}
        if self.__read_capacity_units:
            throughput["ReadCapacityUnits"] = self.__read_capacity_units
        if self.__write_capacity_units:
            throughput["WriteCapacityUnits"] = self.__write_capacity_units
        if throughput:
            data["ProvisionedThroughput"] = throughput
        if self.__gsis:
            data["GlobalSecondaryIndexUpdates"] = [{"Update": i._build()} for i in self.__gsis.itervalues()]
        return data

    class _IndexWithThroughput(ActionProxy):
        def __init__(self, action, name):
            super(UpdateTable._IndexWithThroughput, self).__init__(action)
            self.__name = name
            self.__read_capacity_units = None
            self.__write_capacity_units = None

        def table(self):
            return self._action

        def provisioned_throughput(self, read_capacity_units, write_capacity_units):
            self.__read_capacity_units = read_capacity_units
            self.__write_capacity_units = write_capacity_units
            return self

        def _build(self):
            data = {"IndexName": self.__name}
            throughput = {}
            if self.__read_capacity_units:
                throughput["ReadCapacityUnits"] = self.__read_capacity_units
            if self.__write_capacity_units:
                throughput["WriteCapacityUnits"] = self.__write_capacity_units
            if throughput:
                data["ProvisionedThroughput"] = throughput
            return data

    def provisioned_throughput(self, read_capacity_units, write_capacity_units):
        """
        @todo Document
        """
        self.__read_capacity_units = read_capacity_units
        self.__write_capacity_units = write_capacity_units
        return self

    def global_secondary_index(self, name):
        """
        @todo Document
        """
        if name not in self.__gsis:
            self.__gsis[name] = self._IndexWithThroughput(self, name)
        return self.__gsis[name]


class UpdateTableUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(UpdateTable("Foo").name, "UpdateTable")

    def test_no_arguments(self):
        self.assertEqual(UpdateTable("Foo").payload, {"TableName": "Foo"})

    def test_throughput(self):
        self.assertEqual(
            UpdateTable("Foo").provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def test_gsi(self):
        self.assertEqual(
            UpdateTable("Foo").global_secondary_index("the_gsi").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi"}},
                ],
            }
        )

    def test_gsi_provisioned_throughput(self):
        self.assertEqual(
            UpdateTable("Foo").global_secondary_index("the_gsi").provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
                ],
            }
        )

    def test_back_to_gsi_after_back_to_table(self):
        self.assertEqual(
            UpdateTable("Foo").global_secondary_index("the_gsi").table().provisioned_throughput(12, 13).global_secondary_index("the_gsi").provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 12, "WriteCapacityUnits": 13},
            }
        )


class UpdateTableLocalIntegTests(_tst.LocalIntegTests):
    def setUp(self):
        super(UpdateTableLocalIntegTests, self).setUp()
        self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _lv.STRING)
                .project_all()
                .provisioned_throughput(3, 4)
        )

    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        super(UpdateTableLocalIntegTests, self).tearDown()

    def test_provisioned_throughput(self):
        r = self.connection(
            _lv.UpdateTable("Aaa").provisioned_throughput(2, 4)
        )

        self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 2)
        self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 4)

    def test_gsi_provisioned_throughput(self):
        r = self.connection(
            _lv.UpdateTable("Aaa").global_secondary_index("the_gsi").provisioned_throughput(6, 8)
        )

        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 6)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 8)
