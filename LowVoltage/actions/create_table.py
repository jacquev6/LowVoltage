# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action, ActionProxy
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
        :type: None or :class:`.TableDescription`
        """
        if _is_dict(self.__table_description):  # pragma no branch (Defensive code)
            return TableDescription(**self.__table_description)


class CreateTable(Action):
    """
    The `CreateTable request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_RequestParameters>`__.
    """

    # @todo Should we add ctor parameters and allow use to choose between ctor and builder syntaxes? Same for .global_secondary_index. Same everywhere.

    def __init__(self, table_name):
        super(CreateTable, self).__init__("CreateTable")
        self.__table_name = table_name
        self.__hash_key = None
        self.__range_key = None
        self.__attribute_definitions = {}
        self.__read_capacity_units = None
        self.__write_capacity_units = None
        self.__gsis = {}
        self.__lsis = {}

    def build(self):
        data = {"TableName": self.__table_name}
        schema = []
        if self.__hash_key:
            schema.append({"AttributeName": self.__hash_key, "KeyType": "HASH"})
        if self.__range_key:
            schema.append({"AttributeName": self.__range_key, "KeyType": "RANGE"})
        if schema:
            data["KeySchema"] = schema
        if self.__attribute_definitions:
            data["AttributeDefinitions"] = [
                {"AttributeName": name, "AttributeType": typ}
                for name, typ in self.__attribute_definitions.iteritems()
            ]
        throughput = {}
        if self.__read_capacity_units:
            throughput["ReadCapacityUnits"] = self.__read_capacity_units
        if self.__write_capacity_units:
            throughput["WriteCapacityUnits"] = self.__write_capacity_units
        if throughput:
            data["ProvisionedThroughput"] = throughput
        if self.__gsis:
            data["GlobalSecondaryIndexes"] = [i._build() for i in self.__gsis.itervalues()]
        if self.__lsis:
            data["LocalSecondaryIndexes"] = [i._build() for i in self.__lsis.itervalues()]
        return data

    @staticmethod
    def Result(**kwds):
        return CreateTableResponse(**kwds)

    class _Index(ActionProxy):
        def __init__(self, action, name):
            super(CreateTable._Index, self).__init__(action)
            self.__name = name
            self.__hash_key = None
            self.__range_key = None
            self.__projection = None

        def table(self):
            return self._action

        def hash_key(self, name, typ=None):
            self.__hash_key = name
            if typ is not None:
                self._action.attribute_definition(name, typ)
            return self

        def range_key(self, name, typ=None):
            self.__range_key = name
            if typ is not None:
                self._action.attribute_definition(name, typ)
            return self

        def project_all(self):
            self.__projection = "ALL"
            return self

        def project_keys_only(self):
            self.__projection = "KEYS_ONLY"
            return self

        def project(self, *attrs):
            if not isinstance(self.__projection, list):
                self.__projection = []
            for attr in attrs:
                if isinstance(attr, basestring):
                    attr = [attr]
                self.__projection.extend(attr)
            return self

        def _build(self):
            data = {"IndexName": self.__name}
            schema = []
            if self.__hash_key:
                schema.append({"AttributeName": self.__hash_key, "KeyType": "HASH"})
            if self.__range_key:
                schema.append({"AttributeName": self.__range_key, "KeyType": "RANGE"})
            if schema:
                data["KeySchema"] = schema
            if isinstance(self.__projection, basestring):
                data["Projection"] = {"ProjectionType": self.__projection}
            elif self.__projection:
                data["Projection"] = {"ProjectionType": "INCLUDE", "NonKeyAttributes": self.__projection}
            return data

    class _IndexWithThroughput(_Index):
        def __init__(self, table, name):
            super(CreateTable._IndexWithThroughput, self).__init__(table, name)
            self.__read_capacity_units = None
            self.__write_capacity_units = None

        def provisioned_throughput(self, read_capacity_units, write_capacity_units):
            self.__read_capacity_units = read_capacity_units
            self.__write_capacity_units = write_capacity_units
            return self

        def _build(self):
            data = super(CreateTable._IndexWithThroughput, self)._build()
            throughput = {}
            if self.__read_capacity_units:
                throughput["ReadCapacityUnits"] = self.__read_capacity_units
            if self.__write_capacity_units:
                throughput["WriteCapacityUnits"] = self.__write_capacity_units
            if throughput:
                data["ProvisionedThroughput"] = throughput
            return data

    def hash_key(self, name, typ=None):
        """
        @todo Document
        """
        self.__hash_key = name
        if typ is not None:
            self.attribute_definition(name, typ)
        return self

    def range_key(self, name, typ=None):
        """
        @todo Document
        """
        self.__range_key = name
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

    def local_secondary_index(self, name):
        """
        @todo Document
        """
        if name not in self.__lsis:
            self.__lsis[name] = self._Index(self, name)
        return self.__lsis[name]


class CreateTableUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(CreateTable("Foo").name, "CreateTable")

    def test_no_arguments(self):
        self.assertEqual(CreateTable("Foo").build(), {"TableName": "Foo"})

    def test_hash_key(self):
        self.assertEqual(
            CreateTable("Foo").hash_key("h").build(),
            {
                "TableName": "Foo",
                "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}],
            }
        )

    def test_hash_key_with_type(self):
        self.assertEqual(
            CreateTable("Foo").hash_key("h", _lv.STRING).build(),
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "h", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}],
            }
        )

    def test_attribute_definition(self):
        self.assertEqual(
            CreateTable("Foo").attribute_definition("h", _lv.STRING).build(),
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "h", "AttributeType": "S"}],
            }
        )

    def test_range_key(self):
        self.assertEqual(
            CreateTable("Foo").range_key("r").build(),
            {
                "TableName": "Foo",
                "KeySchema": [{"AttributeName": "r", "KeyType": "RANGE"}],
            }
        )

    def test_range_key_with_type(self):
        self.assertEqual(
            CreateTable("Foo").range_key("r", _lv.STRING).build(),
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "r", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "r", "KeyType": "RANGE"}],
            }
        )

    def test_throughput(self):
        self.assertEqual(
            CreateTable("Foo").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def test_global_secondary_index(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [{"IndexName": "foo"}],
            }
        )

    def test_local_secondary_index(self):
        self.assertEqual(
            CreateTable("Foo").local_secondary_index("foo").build(),
            {
                "TableName": "Foo",
                "LocalSecondaryIndexes": [{"IndexName": "foo"}],
            }
        )

    def test_global_secondary_index_hash_key(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").hash_key("hh").build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "KeySchema": [{"AttributeName": "hh", "KeyType": "HASH"}]},
                ],
            }
        )

    def test_global_secondary_index_range_key(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").range_key("rr").build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "KeySchema": [{"AttributeName": "rr", "KeyType": "RANGE"}]},
                ],
            }
        )

    def test_global_secondary_index_hash_key_with_type(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").hash_key("hh", _lv.STRING).build(),
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
            CreateTable("Foo").global_secondary_index("foo").range_key("rr", _lv.STRING).build(),
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
            CreateTable("Foo").global_secondary_index("foo").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}},
                ],
            }
        )

    def test_global_secondary_index_project_all(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").project_all().build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "ALL"}},
                ],
            }
        )

    def test_global_secondary_index_project_keys_only(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").project_keys_only().build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "KEYS_ONLY"}},
                ],
            }
        )

    def test_global_secondary_index_project_include(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").project("toto", "titi").project(["tutu"]).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["toto", "titi", "tutu"]}},
                ],
            }
        )

    def test_back_to_table_after_gsi(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").table().provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [{"IndexName": "foo"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def test_implicit_back_toTable_after_gsi(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").attribute_definition("bar", _lv.NUMBER).build(),
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
                .build(),
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
                .build(),
            {
                "TableName": "Foo",
                "LocalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "ALL"}}
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )


class CreateTableLocalIntegTests(_tst.LocalIntegTests):
    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        super(CreateTableLocalIntegTests, self).tearDown()

    def test_simplest_table(self):
        r = self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )

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

    def test_simple_global_secondary_index(self):
        r = self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _lv.STRING)
                .project_all()
                .provisioned_throughput(3, 4)
        )

        self.assertEqual(r.table_description.global_secondary_indexes[0].index_name, "the_gsi")
        self.assertEqual(r.table_description.global_secondary_indexes[0].index_size_bytes, 0)
        self.assertEqual(r.table_description.global_secondary_indexes[0].index_status, "ACTIVE")
        self.assertEqual(r.table_description.global_secondary_indexes[0].item_count, 0)
        self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].attribute_name, "hh")
        self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].key_type, "HASH")
        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.non_key_attributes, None)
        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.projection_type, "ALL")
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.last_decrease_date_time, None)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.last_increase_date_time, None)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.number_of_decreases_today, None)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 3)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 4)

    def test_simple_local_secondary_index(self):
        r = self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).range_key("r", _lv.STRING).provisioned_throughput(1, 2)
                .local_secondary_index("the_lsi").hash_key("h").range_key("rr", _lv.STRING).project_all()
        )

        self.assertEqual(r.table_description.local_secondary_indexes[0].index_name, "the_lsi")
        self.assertEqual(r.table_description.local_secondary_indexes[0].index_size_bytes, 0)
        self.assertEqual(r.table_description.local_secondary_indexes[0].item_count, 0)
        self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[0].attribute_name, "h")
        self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[0].key_type, "HASH")
        self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[1].attribute_name, "rr")
        self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[1].key_type, "RANGE")
        self.assertEqual(r.table_description.local_secondary_indexes[0].projection.non_key_attributes, None)
        self.assertEqual(r.table_description.local_secondary_indexes[0].projection.projection_type, "ALL")

    def test_global_secondary_index_with_projection(self):
        r = self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _lv.STRING)
                .project("toto", "titi")
                .provisioned_throughput(3, 4)
        )

        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.non_key_attributes[0], "toto")
        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.non_key_attributes[1], "titi")
        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.projection_type, "INCLUDE")


class CreateTableErrorLocalIntegTests(_tst.LocalIntegTests):
    def test_define_unused_attribute(self):
        with self.assertRaises(_lv.ValidationException) as catcher:
            self.connection(
                _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                    .attribute_definition("x", _lv.STRING)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "The number of attributes in key schema must match the number of attributesdefined in attribute definitions.",
            },)
        )

    def test_dont_define_key_attribute(self):
        with self.assertRaises(_lv.ValidationException) as catcher:
            self.connection(
                _lv.CreateTable("Aaa").hash_key("h").provisioned_throughput(1, 2)
                    .attribute_definition("x", _lv.STRING)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "Hash Key not specified in Attribute Definitions.  Type unknown.",
            },)
        )

    def test_dont_define_any_attribute(self):
        with self.assertRaises(_lv.ValidationException) as catcher:
            self.connection(
                _lv.CreateTable("Aaa").hash_key("h").provisioned_throughput(1, 2)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "No Attribute Schema Defined",
            },)
        )
