# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`UpdateTable`, the connection will return a :class:`UpdateTableResponse`:

.. testsetup::

    table = "LowVoltage.Tests.Doc.UpdateTable.1"
    table2 = "LowVoltage.Tests.Doc.UpdateTable.2"
    table3 = "LowVoltage.Tests.Doc.UpdateTable.3"
    table4 = "LowVoltage.Tests.Doc.UpdateTable.4"
    connection(CreateTable(table).hash_key("h", STRING).provisioned_throughput(1, 1))
    connection(
        CreateTable(table2).hash_key("h", STRING).provisioned_throughput(1, 1)
            .global_secondary_index("gsi").hash_key("hh", STRING).range_key("rr", NUMBER).provisioned_throughput(1, 1).project_all()
    )
    connection(
        CreateTable(table3).hash_key("h", STRING).provisioned_throughput(1, 1)
            .global_secondary_index("gsi").hash_key("hh", STRING).range_key("rr", NUMBER).provisioned_throughput(1, 1).project_all()
    )
    connection(CreateTable(table4).hash_key("h", STRING).provisioned_throughput(1, 1))
    WaitForTableActivation(connection, table)
    WaitForTableActivation(connection, table2)
    WaitForTableActivation(connection, table3)
    WaitForTableActivation(connection, table4)

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
    WaitForTableActivation(connection, table2)
    WaitForTableActivation(connection, table3)
    WaitForTableActivation(connection, table4)
    connection(DeleteTable(table))
    connection(DeleteTable(table2))
    connection(DeleteTable(table3))
    connection(DeleteTable(table4))
    WaitForTableDeletion(connection, table)
    WaitForTableDeletion(connection, table2)
    WaitForTableDeletion(connection, table3)
    WaitForTableDeletion(connection, table4)
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

        :type: ``None`` or :class:`.TableDescription`
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
            self._verb = verb
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
            return {self._verb: data}

    def hash_key(self, name, typ=None):
        """
        Set the hash key in KeySchema for the active index.
        If you provide a second argument, :meth:`attribute_definition` will be called as well.

        :raise: :exc:`.BuilderError` if called when no index is active or if the active index is not being created.

        See :meth:`create_global_secondary_index` for an example.
        """
        self.__check_active_index()
        self.__active_index._hash_key = name
        if typ is not None:
            self.attribute_definition(name, typ)
        return self

    def range_key(self, name, typ=None):
        """
        Set the range key in KeySchema for the active index.
        If you provide a second argument, :meth:`attribute_definition` will be called as well.

        :raise: :exc:`.BuilderError` if called when no index is active or if the active index is not being created.

        See :meth:`create_global_secondary_index` for an example.
        """
        self.__check_active_index()
        self.__active_index._range_key = name
        if typ is not None:
            self.attribute_definition(name, typ)
        return self

    def attribute_definition(self, name, typ):
        """
        Set the type of an attribute in AttributeDefinitions.
        Key attribute must be typed. See :mod:`.attribute_types` for constants to be passed to this method.
        """
        self.__attribute_definitions[name] = typ
        return self

    def provisioned_throughput(self, read_capacity_units, write_capacity_units):
        """
        Set the new provisioned throughput for the table or the active index.

        See :meth:`create_global_secondary_index` for an example.
        """
        self.__active_index._read_capacity_units = read_capacity_units
        self.__active_index._write_capacity_units = write_capacity_units
        return self

    def create_global_secondary_index(self, name):
        """
        Create a new GSI.
        This method sets the active index: methods like :meth:`provisioned_throughput` will apply to the index.

        >>> connection(
        ...   UpdateTable(table4)
        ...     .create_global_secondary_index("gsi")
        ...       .hash_key("hh", STRING)
        ...       .range_key("rr", NUMBER)
        ...       .project_all()
        ...       .provisioned_throughput(2, 2)
        ... )
        <LowVoltage.actions.update_table.UpdateTableResponse ...>
        """
        if name not in self.__gsis:
            self.__gsis[name] = self._Index("Create", name)
        self.__active_index = self.__gsis[name]
        return self

    def update_global_secondary_index(self, name):
        """
        Update an existing GSI.
        This method sets the active index: methods like :meth:`provisioned_throughput` will apply to the index.

        >>> connection(
        ...   UpdateTable(table2)
        ...     .update_global_secondary_index("gsi")
        ...       .provisioned_throughput(2, 2)
        ... )
        <LowVoltage.actions.update_table.UpdateTableResponse ...>
        """
        if name not in self.__gsis:
            self.__gsis[name] = self._Index("Update", name)
        self.__active_index = self.__gsis[name]
        return self

    def delete_global_secondary_index(self, name):
        """
        Mark a GSI for deletion.

        This method does not set the active index, because there is nothing to modify.

        >>> connection(
        ...   UpdateTable(table3)
        ...     .delete_global_secondary_index("gsi")
        ... )
        <LowVoltage.actions.update_table.UpdateTableResponse ...>
        """
        self.__gsis[name] = self._Index("Delete", name)
        return self

    def table(self):
        """
        Reset the active index: methods like :meth:`provisioned_throughput` will apply to the table.
        """
        self.__active_index = self
        return self

    def project_all(self):
        """
        Set ProjectionType to ALL for the active index.

        :raise: :exc:`.BuilderError` if called when no index is active or if the active index is not being created.

        See :meth:`create_global_secondary_index` for an example.
        """
        self.__check_active_index()
        self.__active_index._projection = "ALL"
        return self

    def project_keys_only(self):
        """
        Set ProjectionType to KEYS_ONLY for the active index.

        :raise: :exc:`.BuilderError` if called when no index is active or if the active index is not being created.
        """
        self.__check_active_index()
        self.__active_index._projection = "KEYS_ONLY"
        return self

    def project(self, *attrs):
        """
        Set ProjectionType to INCLUDE for the active index and add names to NonKeyAttributes.

        :raise: :exc:`.BuilderError` if called when no index is active or if the active index is not being created.
        """
        self.__check_active_index()
        if not isinstance(self.__active_index._projection, list):
            self.__active_index._projection = []
        for attr in attrs:
            if isinstance(attr, basestring):
                attr = [attr]
            self.__active_index._projection.extend(attr)
        return self

    def __check_active_index(self):
        if self.__active_index is self or self.__active_index._verb != "Create":
            raise _lv.BuilderError("No active index or active index not being created.")


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

    def test_hash_key_without_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").hash_key("h")
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))

    def test_range_key_without_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").range_key("r")
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))

    def test_project_all_without_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").project_all()
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))

    def test_project_without_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").project("a")
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))

    def test_project_keys_only_without_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").project_keys_only()
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))

    def test_hash_key_with_updating_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").update_global_secondary_index("gsi").hash_key("h")
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))

    def test_range_key_with_updating_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").update_global_secondary_index("gsi").range_key("r")
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))

    def test_project_all_with_updating_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").update_global_secondary_index("gsi").project_all()
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))

    def test_project_with_updating_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").update_global_secondary_index("gsi").project("a")
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))

    def test_project_keys_only_with_updating_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            UpdateTable("Foo").update_global_secondary_index("gsi").project_keys_only()
        self.assertEqual(catcher.exception.args, ("No active index or active index not being created.",))


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
