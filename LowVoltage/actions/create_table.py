# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`CreateTable`, the connection will return a :class:`CreateTableResponse`:

.. testsetup::

    table = "LowVoltage.Tests.Doc.CreateTable.1"
    table2 = "LowVoltage.Tests.Doc.CreateTable.2"
    table3 = "LowVoltage.Tests.Doc.CreateTable.3"
    table4 = "LowVoltage.Tests.Doc.CreateTable.4"

>>> r = connection(
...   CreateTable(table)
...     .hash_key("h", STRING)
...     .provisioned_throughput(1, 1)
... )
>>> r
<LowVoltage.actions.create_table.CreateTableResponse ...>
>>> r.table_description.table_status
u'CREATING'

Note that you can use the :func:`.wait_for_table_activation` compound to poll the table status until it's usable. See :ref:`actions-vs-compounds` in the user guide.

.. testcleanup::

    wait_for_table_activation(connection, table)
    wait_for_table_activation(connection, table2)
    wait_for_table_activation(connection, table3)
    wait_for_table_activation(connection, table4)
    connection(DeleteTable(table))
    connection(DeleteTable(table2))
    connection(DeleteTable(table3))
    connection(DeleteTable(table4))
    wait_for_table_deletion(connection, table)
    wait_for_table_deletion(connection, table2)
    wait_for_table_deletion(connection, table3)
    wait_for_table_deletion(connection, table4)
"""

import datetime

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .next_gen_mixins import variadic, proxy
from .next_gen_mixins import (
    TableName,
)
from .return_types import TableDescription, _is_dict


class CreateTableResponse(object):
    """
    The `CreateTable response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_ResponseElements>`__.
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
        The description of the table you just created.

        :type: ``None`` or :class:`.TableDescription`
        """
        if _is_dict(self.__table_description):
            return TableDescription(**self.__table_description)


class CreateTable(Action):
    """
    The `CreateTable request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_RequestParameters>`__.
    """

    def __init__(self, table_name=None):
        """
        Passing ``table_name`` to the constructor is like calling :meth:`table_name` on the new instance.
        """
        super(CreateTable, self).__init__("CreateTable", CreateTableResponse)
        self.__table_name = TableName(self, table_name)
        self._hash_key = None
        self._range_key = None
        self.__attribute_definitions = {}
        self._read_capacity_units = None
        self._write_capacity_units = None
        self.__active_index = self
        self.__gsis = {}
        self.__lsis = {}

    @property
    def payload(self):
        # @todo Simplify, make more linear
        data = {}
        data.update(self.__table_name.payload)
        schema = []
        if self._hash_key:
            schema.append({"AttributeName": self._hash_key, "KeyType": "HASH"})
        if self._range_key:
            schema.append({"AttributeName": self._range_key, "KeyType": "RANGE"})
        if schema:
            data["KeySchema"] = schema
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
            data["GlobalSecondaryIndexes"] = [i.payload for i in self.__gsis.itervalues()]
        if self.__lsis:
            data["LocalSecondaryIndexes"] = [i.payload for i in self.__lsis.itervalues()]
        return data

    class _Index(object):
        def __init__(self, name):
            self.__name = name
            self._hash_key = None
            self._range_key = None
            self._projection = None

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
            return data

    class _IndexWithThroughput(_Index):
        def __init__(self, name):
            super(CreateTable._IndexWithThroughput, self).__init__(name)
            self._read_capacity_units = None
            self._write_capacity_units = None

        @property
        def payload(self):
            data = super(CreateTable._IndexWithThroughput, self).payload
            throughput = {}
            if self._read_capacity_units:
                throughput["ReadCapacityUnits"] = self._read_capacity_units
            if self._write_capacity_units:
                throughput["WriteCapacityUnits"] = self._write_capacity_units
            if throughput:
                data["ProvisionedThroughput"] = throughput
            return data

    @proxy
    def table_name(self, table_name):
        """
        See :meth:`range_key` an example.
        """
        return self.__table_name.set(table_name)

    def hash_key(self, name, typ=None):
        """
        Set the hash key in KeySchema for the table or the active index.
        If you provide a second argument, :meth:`attribute_definition` will be called as well.

        See :meth:`range_key` an example.
        """
        self.__active_index._hash_key = name
        if typ is not None:
            self.attribute_definition(name, typ)
        return self

    def range_key(self, name, typ=None):
        """
        Set the range key in KeySchema for the table or the active index.
        If you provide a second argument, :meth:`attribute_definition` will be called as well.

        >>> connection(
        ...   CreateTable()
        ...     .table_name(table2)
        ...     .hash_key("h", STRING)
        ...     .range_key("r")
        ...     .provisioned_throughput(1, 1)
        ...     .attribute_definition("r", NUMBER)
        ... )
        <LowVoltage.actions.create_table.CreateTableResponse ...>
        """
        self.__active_index._range_key = name
        if typ is not None:
            self.attribute_definition(name, typ)
        return self

    def attribute_definition(self, name, typ):
        """
        Set the type of an attribute in AttributeDefinitions.
        Key attribute must be typed. See :mod:`.attribute_types` for constants to be passed to this method.

        See :meth:`range_key` for an example.
        """
        self.__attribute_definitions[name] = typ
        return self

    def provisioned_throughput(self, read_capacity_units, write_capacity_units):
        """
        Set the read and write provisioned throughput for the table or the active index.

        See :meth:`range_key`, :meth:`global_secondary_index` or :meth:`local_secondary_index` for examples.
        """
        self.__active_index._read_capacity_units = read_capacity_units
        self.__active_index._write_capacity_units = write_capacity_units
        return self

    def global_secondary_index(self, name):
        """
        Add a GSI. This method sets the active index: methods like :meth:`hash_key` will apply to the index.

        >>> connection(
        ...   CreateTable(table3)
        ...     .hash_key("h", STRING)
        ...     .provisioned_throughput(1, 1)
        ...     .global_secondary_index("gsi")
        ...       .hash_key("a", BINARY)
        ...       .range_key("b", NUMBER)
        ...       .provisioned_throughput(1, 1)
        ...       .project_all()
        ... )
        <LowVoltage.actions.create_table.CreateTableResponse ...>
        """
        if name not in self.__gsis:
            self.__gsis[name] = self._IndexWithThroughput(name)
        self.__active_index = self.__gsis[name]
        return self

    def local_secondary_index(self, name):
        """
        Add a LSI. This method sets the active index: methods like :meth:`hash_key` will apply to the index.

        >>> connection(
        ...   CreateTable(table4)
        ...     .hash_key("h", STRING)
        ...     .range_key("r", NUMBER)
        ...     .provisioned_throughput(1, 1)
        ...     .local_secondary_index("lsi")
        ...       .hash_key("h")
        ...       .range_key("a", NUMBER)
        ...       .provisioned_throughput(1, 1)
        ...       .project("x", "y")
        ... )
        <LowVoltage.actions.create_table.CreateTableResponse ...>
        """
        if name not in self.__lsis:
            self.__lsis[name] = self._Index(name)
        self.__active_index = self.__lsis[name]
        return self

    def table(self):
        """
        Reset the active index: methods like :meth:`hash_key` will apply to the table.
        """
        self.__active_index = self
        return self

    def project_all(self):
        """
        Set ProjectionType to ALL for the active index.

        :raise: :exc:`.BuilderError` if called when no index is active.

        See :meth:`global_secondary_index` for an example.
        """
        self.__check_active_index()
        self.__active_index._projection = "ALL"
        return self

    def project_keys_only(self):
        """
        Set ProjectionType to KEYS_ONLY for the active index.

        :raise: :exc:`.BuilderError` if called when no index is active.
        """
        self.__check_active_index()
        self.__active_index._projection = "KEYS_ONLY"
        return self

    @variadic(basestring)
    def project(self, attrs):
        """
        Set ProjectionType to INCLUDE for the active index and add names to NonKeyAttributes.

        :raise: :exc:`.BuilderError` if called when no index is active.

        See :meth:`local_secondary_index` for an example.
        """
        self.__check_active_index()
        if not isinstance(self.__active_index._projection, list):
            self.__active_index._projection = []
        self.__active_index._projection.extend(attrs)
        return self

    def __check_active_index(self):
        if self.__active_index is self:
            raise _lv.BuilderError("No active index.")


class CreateTableUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(CreateTable("Foo").name, "CreateTable")

    def test_constructor(self):
        self.assertEqual(CreateTable("Foo").payload, {"TableName": "Foo"})

    def test_table_name(self):
        self.assertEqual(CreateTable().table_name("Foo").payload, {"TableName": "Foo"})

    def test_hash_key(self):
        self.assertEqual(
            CreateTable("Foo").hash_key("h").payload,
            {
                "TableName": "Foo",
                "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}],
            }
        )

    def test_hash_key_with_type(self):
        self.assertEqual(
            CreateTable("Foo").hash_key("h", _lv.STRING).payload,
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "h", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}],
            }
        )

    def test_attribute_definition(self):
        self.assertEqual(
            CreateTable("Foo").attribute_definition("h", _lv.STRING).payload,
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "h", "AttributeType": "S"}],
            }
        )

    def test_range_key(self):
        self.assertEqual(
            CreateTable("Foo").range_key("r").payload,
            {
                "TableName": "Foo",
                "KeySchema": [{"AttributeName": "r", "KeyType": "RANGE"}],
            }
        )

    def test_range_key_with_type(self):
        self.assertEqual(
            CreateTable("Foo").range_key("r", _lv.STRING).payload,
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "r", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "r", "KeyType": "RANGE"}],
            }
        )

    def test_throughput(self):
        self.assertEqual(
            CreateTable("Foo").provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def test_global_secondary_index(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [{"IndexName": "foo"}],
            }
        )

    def test_local_secondary_index(self):
        self.assertEqual(
            CreateTable("Foo").local_secondary_index("foo").payload,
            {
                "TableName": "Foo",
                "LocalSecondaryIndexes": [{"IndexName": "foo"}],
            }
        )

    def test_global_secondary_index_hash_key(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").hash_key("hh").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "KeySchema": [{"AttributeName": "hh", "KeyType": "HASH"}]},
                ],
            }
        )

    def test_global_secondary_index_range_key(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").range_key("rr").payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "KeySchema": [{"AttributeName": "rr", "KeyType": "RANGE"}]},
                ],
            }
        )

    def test_global_secondary_index_hash_key_with_type(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").hash_key("hh", _lv.STRING).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "KeySchema": [{"AttributeName": "hh", "KeyType": "HASH"}]},
                ],
                "AttributeDefinitions": [{"AttributeName": "hh", "AttributeType": "S"}],
            }
        )

    def test_global_secondary_index_range_key_with_type(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").range_key("rr", _lv.STRING).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "KeySchema": [{"AttributeName": "rr", "KeyType": "RANGE"}]},
                ],
                "AttributeDefinitions": [{"AttributeName": "rr", "AttributeType": "S"}],
            }
        )

    def test_global_secondary_index_throughput(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}},
                ],
            }
        )

    def test_global_secondary_index_project_all(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").project_all().payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "ALL"}},
                ],
            }
        )

    def test_global_secondary_index_project_keys_only(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").project_keys_only().payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "KEYS_ONLY"}},
                ],
            }
        )

    def test_global_secondary_index_project_include(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").project("toto", "titi").project(["tutu"]).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["toto", "titi", "tutu"]}},
                ],
            }
        )

    def test_back_to_table_after_gsi(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").table().provisioned_throughput(42, 43).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [{"IndexName": "foo"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def test_implicit_back_to_table_after_gsi(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").attribute_definition("bar", _lv.NUMBER).payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [{"IndexName": "foo"}],
                "AttributeDefinitions": [{"AttributeName": "bar", "AttributeType": "N"}],
            }
        )

    def test_back_to_gsi_after_back_to_table(self):
        self.assertEqual(
            CreateTable("Foo")
                .global_secondary_index("foo")
                .table().provisioned_throughput(42, 43)
                .global_secondary_index("foo").provisioned_throughput(12, 13)
                .payload,
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "ProvisionedThroughput": {"ReadCapacityUnits": 12, "WriteCapacityUnits": 13}}
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def test_back_to_lsi_after_back_to_table(self):
        self.assertEqual(
            CreateTable("Foo")
                .local_secondary_index("foo")
                .table().provisioned_throughput(42, 43)
                .local_secondary_index("foo").project_all()
                .payload,
            {
                "TableName": "Foo",
                "LocalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "ALL"}}
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def test_project_without_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            CreateTable("Foo").project("a")
        self.assertEqual(catcher.exception.args, ("No active index.",))

    def test_project_all_without_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            CreateTable("Foo").project_all()
        self.assertEqual(catcher.exception.args, ("No active index.",))

    def test_project_keys_only_without_active_index(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            CreateTable("Foo").project_keys_only()
        self.assertEqual(catcher.exception.args, ("No active index.",))


class CreateTableResponseUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = CreateTableResponse()
        self.assertIsNone(r.table_description)

    def test_all_set(self):
        r = CreateTableResponse(TableDescription={})
        self.assertIsInstance(r.table_description, TableDescription)
