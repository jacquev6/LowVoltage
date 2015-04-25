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
from .action import Action
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

    def __init__(self, table_name):
        super(UpdateTable, self).__init__("UpdateTable", UpdateTableResponse)
        self.__table_name = table_name
        self.__attribute_definitions = {}
        self._read_capacity_units = None
        self._write_capacity_units = None
        self.__gsis = {}
        self.__active_index = self

    @property
    def payload(self):
        data = {"TableName": self.__table_name}
        if self.__attribute_definitions:
            data["AttributeDefinitions"] = [
                {"AttributeName": name, "AttributeType": typ}
                for name, typ in self.__attribute_definitions.iteritems()
            ]
        throughput = {}
        if self._read_capacity_units:
            throughput["ReadCapacityUnits"] = self._read_capacity_units
        if self._write_capacity_units:
            throughput["WriteCapacityUnits"] = self._write_capacity_units
        if throughput:
            data["ProvisionedThroughput"] = throughput
        if self.__gsis:
            data["GlobalSecondaryIndexUpdates"] = [i.payload for i in self.__gsis.itervalues()]
        return data

    class _Index(object):
        def __init__(self, verb, name):
            self.__verb = verb
            self.__name = name
            self._hash_key = None
            self._range_key = None
            self._projection = None
            self._read_capacity_units = None
            self._write_capacity_units = None

        @property
        def payload(self):
            data = {"IndexName": self.__name}
            schema = []
            if self._hash_key:
                schema.append({"AttributeName": self._hash_key, "KeyType": "HASH"})
            if self._range_key:
                schema.append({"AttributeName": self._range_key, "KeyType": "RANGE"})
            if schema:
                data["KeySchema"] = schema
            if isinstance(self._projection, basestring):
                data["Projection"] = {"ProjectionType": self._projection}
            elif self._projection:
                data["Projection"] = {"ProjectionType": "INCLUDE", "NonKeyAttributes": self._projection}
            throughput = {}
            if self._read_capacity_units:
                throughput["ReadCapacityUnits"] = self._read_capacity_units
            if self._write_capacity_units:
                throughput["WriteCapacityUnits"] = self._write_capacity_units
            if throughput:
                data["ProvisionedThroughput"] = throughput
            return {self.__verb: data}

    def hash_key(self, name, typ=None):
        """
        @todo Document
        """
        self.__active_index._hash_key = name
        if typ is not None:
            self.attribute_definition(name, typ)
        return self

    def range_key(self, name, typ=None):
        """
        @todo Document
        """
        self.__active_index._range_key = name
        if typ is not None:
            self.attribute_definition(name, typ)
        return self

    def attribute_definition(self, name, typ):
        """
        @todo Document
        """
        self.__attribute_definitions[name] = typ
        return self

    def provisioned_throughput(self, read_capacity_units, write_capacity_units):
        """
        @todo Document
        """
        self.__active_index._read_capacity_units = read_capacity_units
        self.__active_index._write_capacity_units = write_capacity_units
        return self

    def create_global_secondary_index(self, name):
        """
        @todo Document
        """
        if name not in self.__gsis:
            self.__gsis[name] = self._Index("Create", name)
        self.__active_index = self.__gsis[name]
        return self

    def update_global_secondary_index(self, name):
        """
        @todo Document
        """
        if name not in self.__gsis:
            self.__gsis[name] = self._Index("Update", name)
        self.__active_index = self.__gsis[name]
        return self

    def delete_global_secondary_index(self, name):
        """
        Mark a GSI for deletion.

        This method does not set the active index, because there is nothing to modify.

        @todo doctest
        """
        self.__gsis[name] = self._Index("Delete", name)
        return self

    def table(self):
        """
        @todo Document
        """
        self.__active_index = self
        return self

    def project_all(self):
        """
        @todo Document
        """
        self.__active_index._projection = "ALL"
        return self

    def project_keys_only(self):
        """
        @todo Document
        """
        self.__active_index._projection = "KEYS_ONLY"
        return self

    def project(self, *attrs):
        """
        @todo Document
        """
        if not isinstance(self.__active_index._projection, list):
            self.__active_index._projection = []
        for attr in attrs:
            if isinstance(attr, basestring):
                attr = [attr]
            self.__active_index._projection.extend(attr)
        return self


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

    def test_attribute_definition(self):
        self.assertEqual(
            UpdateTable("Foo").attribute_definition("a", "B").payload,
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "a", "AttributeType": "B"}],
            }
        )

    def test_create_gsi(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi"}},
                ],
            }
        )

    def test_create_gsi_provisioned_throughput(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").provisioned_throughput(1, 2).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 2}}},
                ],
            }
        )

    def test_create_gsi_hash_key(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").hash_key("h").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}]}},
                ],
            }
        )

    def test_create_gsi_range_key(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").range_key("r").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "KeySchema": [{"AttributeName": "r", "KeyType": "RANGE"}]}},
                ],
            }
        )

    def test_create_gsi_hash_key_with_type(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").hash_key("h", "S").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}]}},
                ],
                "AttributeDefinitions": [{"AttributeName": "h", "AttributeType": "S"}]
            }
        )

    def test_create_gsi_range_key_with_type(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").range_key("r", "N").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "KeySchema": [{"AttributeName": "r", "KeyType": "RANGE"}]}},
                ],
                "AttributeDefinitions": [{"AttributeName": "r", "AttributeType": "N"}]
            }
        )

    def test_create_gsi_project_all(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").project_all().payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "Projection": {"ProjectionType": "ALL"}}},
                ],
            }
        )

    def test_create_gsi_project_keys_only(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").project_keys_only().payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "Projection": {"ProjectionType": "KEYS_ONLY"}}},
                ],
            }
        )

    def test_create_gsi_project(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").project("a", ["b", "c"]).project("d").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["a", "b", "c", "d"]}}},
                ],
            }
        )

    def test_update_gsi(self):
        self.assertEqual(
            UpdateTable("Foo").update_global_secondary_index("the_gsi").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi"}},
                ],
            }
        )

    def test_update_gsi_provisioned_throughput(self):
        self.assertEqual(
            UpdateTable("Foo").update_global_secondary_index("the_gsi").provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
                ],
            }
        )

    def test_delete_gsi(self):
        self.assertEqual(
            UpdateTable("Foo").delete_global_secondary_index("the_gsi").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Delete": {"IndexName": "the_gsi"}},
                ],
            }
        )

    def test_back_to_update_gsi_after_back_to_table(self):
        self.assertEqual(
            UpdateTable("Foo").update_global_secondary_index("the_gsi").table().provisioned_throughput(12, 13)
                .update_global_secondary_index("the_gsi").provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 12, "WriteCapacityUnits": 13},
            }
        )

    def test_back_to_create_gsi_after_back_to_table(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").table().provisioned_throughput(12, 13)
                .create_global_secondary_index("the_gsi").provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 12, "WriteCapacityUnits": 13},
            }
        )

    def test_back_to_update_gsi_after_back_to_table_after_create_gsi(self):
        self.assertEqual(
            UpdateTable("Foo").create_global_secondary_index("the_gsi").table().provisioned_throughput(12, 13)
                .update_global_secondary_index("the_gsi").provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Create": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
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

    def test_update_gsi_provisioned_throughput(self):
        r = self.connection(
            _lv.UpdateTable("Aaa").update_global_secondary_index("the_gsi").provisioned_throughput(6, 8)
        )

        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 6)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 8)

    def test_delete_and_create_gsi(self):
        r = self.connection(_lv.UpdateTable("Aaa").delete_global_secondary_index("the_gsi"))
        self.assertEqual(r.table_description.global_secondary_indexes[0].index_status, "DELETING")

        _lv.WaitForTableActivation(self.connection, "Aaa")

        r = self.connection(_lv.DescribeTable("Aaa"))
        self.assertEqual(r.table.global_secondary_indexes, None)
        self.assertEqual(len(r.table.attribute_definitions), 1)  # The previous definition of attribute "hh" has disapeared.

        r = self.connection(
            _lv.UpdateTable("Aaa")
                .create_global_secondary_index("new_gsi")
                    .provisioned_throughput(1, 2)
                    .hash_key("nh", _lv.NUMBER)
                    .project_all()
        )

        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 1)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 2)
        self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].attribute_name, "nh")
        self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].key_type, "HASH")
        self.assertEqual(len(r.table_description.attribute_definitions), 2)
