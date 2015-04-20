# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime
import unittest

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action, ActionProxy
from .return_types import TableDescription_, _is_dict


class CreateTable(Action):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_RequestParameters"""

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_ResponseElements"""

        def __init__(
            self,
            TableDescription=None,
            **dummy
        ):
            self.table_description = None
            if _is_dict(TableDescription):  # pragma no branch (Defensive code)
                self.table_description = TableDescription_(**TableDescription)

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
        self.__hash_key = name
        if typ is not None:
            self.attribute_definition(name, typ)
        return self

    def range_key(self, name, typ=None):
        self.__range_key = name
        if typ is not None:
            self.attribute_definition(name, typ)
        return self

    def attribute_definition(self, name, typ):
        self.__attribute_definitions[name] = typ
        return self

    def provisioned_throughput(self, read_capacity_units, write_capacity_units):
        self.__read_capacity_units = read_capacity_units
        self.__write_capacity_units = write_capacity_units
        return self

    def global_secondary_index(self, name):
        if name not in self.__gsis:
            self.__gsis[name] = self._IndexWithThroughput(self, name)
        return self.__gsis[name]

    def local_secondary_index(self, name):
        if name not in self.__lsis:
            self.__lsis[name] = self._Index(self, name)
        return self.__lsis[name]


class CreateTableUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(CreateTable("Foo").name, "CreateTable")

    def testNoArguments(self):
        self.assertEqual(CreateTable("Foo").build(), {"TableName": "Foo"})

    def testHashKey(self):
        self.assertEqual(
            CreateTable("Foo").hash_key("h").build(),
            {
                "TableName": "Foo",
                "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}],
            }
        )

    def testHashKeyWithType(self):
        self.assertEqual(
            CreateTable("Foo").hash_key("h", _lv.STRING).build(),
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "h", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}],
            }
        )

    def testAttributeDefinition(self):
        self.assertEqual(
            CreateTable("Foo").attribute_definition("h", _lv.STRING).build(),
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "h", "AttributeType": "S"}],
            }
        )

    def testRangeKey(self):
        self.assertEqual(
            CreateTable("Foo").range_key("r").build(),
            {
                "TableName": "Foo",
                "KeySchema": [{"AttributeName": "r", "KeyType": "RANGE"}],
            }
        )

    def testRangeKeyWithType(self):
        self.assertEqual(
            CreateTable("Foo").range_key("r", _lv.STRING).build(),
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "r", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "r", "KeyType": "RANGE"}],
            }
        )

    def testThroughput(self):
        self.assertEqual(
            CreateTable("Foo").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def testGlobalSecondaryIndex(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [{"IndexName": "foo"}],
            }
        )

    def testLocalSecondaryIndex(self):
        self.assertEqual(
            CreateTable("Foo").local_secondary_index("foo").build(),
            {
                "TableName": "Foo",
                "LocalSecondaryIndexes": [{"IndexName": "foo"}],
            }
        )

    def testGlobalSecondaryIndexHashKey(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").hash_key("hh").build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "KeySchema": [{"AttributeName": "hh", "KeyType": "HASH"}]},
                ],
            }
        )

    def testGlobalSecondaryIndexRangeKey(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").range_key("rr").build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "KeySchema": [{"AttributeName": "rr", "KeyType": "RANGE"}]},
                ],
            }
        )

    def testGlobalSecondaryIndexHashKeyWithType(self):
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

    def testGlobalSecondaryIndexRangeKeyWithType(self):
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

    def testGlobalSecondaryIndexThroughput(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}},
                ],
            }
        )

    def testGlobalSecondaryIndexProjectAll(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").project_all().build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "ALL"}},
                ],
            }
        )

    def testGlobalSecondaryIndexProjectKeysOnly(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").project_keys_only().build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "KEYS_ONLY"}},
                ],
            }
        )

    def testGlobalSecondaryIndexProjectInclude(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").project("toto", "titi").project(["tutu"]).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["toto", "titi", "tutu"]}},
                ],
            }
        )

    def testBackToTableAfterGsi(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").table().provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [{"IndexName": "foo"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def testImplicitBackToTableAfterGsi(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").attribute_definition("bar", _lv.NUMBER).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [{"IndexName": "foo"}],
                "AttributeDefinitions": [{"AttributeName": "bar", "AttributeType": "N"}],
            }
        )

    def testBackToGsiAfterBackToTable(self):
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

    def testBackToLsiAfterBackToTable(self):
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
        self.connection.request(_lv.DeleteTable("Aaa"))
        super(CreateTableLocalIntegTests, self).tearDown()

    def testSimplestTable(self):
        r = self.connection.request(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )

        with _tst.cover("r", r) as r:
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

    def testSimpleGlobalSecondaryIndex(self):
        r = self.connection.request(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _lv.STRING)
                .project_all()
                .provisioned_throughput(3, 4)
        )

        _tst.fix_table_description(r.table_description)

        with _tst.cover("r", r) as r:
            self.assertDateTimeIsReasonable(r.table_description.creation_date_time)
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_name, "h")
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_type, "S")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_name, "hh")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_type, "S")
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

    def testSimpleLocalSecondaryIndex(self):
        r = self.connection.request(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).range_key("r", _lv.STRING).provisioned_throughput(1, 2)
                .local_secondary_index("the_lsi").hash_key("h").range_key("rr", _lv.STRING).project_all()
        )

        _tst.fix_table_description(r.table_description)

        with _tst.cover("r", r) as r:
            self.assertDateTimeIsReasonable(r.table_description.creation_date_time)
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_name, "h")
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_type, "S")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_name, "r")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_type, "S")
            self.assertEqual(r.table_description.attribute_definitions[2].attribute_name, "rr")
            self.assertEqual(r.table_description.attribute_definitions[2].attribute_type, "S")
            self.assertEqual(r.table_description.global_secondary_indexes, None)
            self.assertEqual(r.table_description.item_count, 0)
            self.assertEqual(r.table_description.key_schema[0].attribute_name, "h")
            self.assertEqual(r.table_description.key_schema[0].key_type, "HASH")
            self.assertEqual(r.table_description.key_schema[1].attribute_name, "r")
            self.assertEqual(r.table_description.key_schema[1].key_type, "RANGE")
            self.assertEqual(r.table_description.local_secondary_indexes[0].index_name, "the_lsi")
            self.assertEqual(r.table_description.local_secondary_indexes[0].index_size_bytes, 0)
            self.assertEqual(r.table_description.local_secondary_indexes[0].item_count, 0)
            self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[0].attribute_name, "h")
            self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[0].key_type, "HASH")
            self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[1].attribute_name, "rr")
            self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[1].key_type, "RANGE")
            self.assertEqual(r.table_description.local_secondary_indexes[0].projection.non_key_attributes, None)
            self.assertEqual(r.table_description.local_secondary_indexes[0].projection.projection_type, "ALL")
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, datetime.datetime(1970, 1, 1))
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, datetime.datetime(1970, 1, 1))
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 1)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 2)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")

    def testGlobalSecondaryIndexWithProjection(self):
        r = self.connection.request(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _lv.STRING)
                .project("toto", "titi")
                .provisioned_throughput(3, 4)
        )

        _tst.fix_table_description(r.table_description)

        with _tst.cover("r", r) as r:
            self.assertDateTimeIsReasonable(r.table_description.creation_date_time)
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_name, "h")
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_type, "S")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_name, "hh")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_type, "S")
            self.assertEqual(r.table_description.global_secondary_indexes[0].index_name, "the_gsi")
            self.assertEqual(r.table_description.global_secondary_indexes[0].index_size_bytes, 0)
            self.assertEqual(r.table_description.global_secondary_indexes[0].index_status, "ACTIVE")
            self.assertEqual(r.table_description.global_secondary_indexes[0].item_count, 0)
            self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].attribute_name, "hh")
            self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].key_type, "HASH")
            self.assertEqual(r.table_description.global_secondary_indexes[0].projection.non_key_attributes[0], "toto")
            self.assertEqual(r.table_description.global_secondary_indexes[0].projection.non_key_attributes[1], "titi")
            self.assertEqual(r.table_description.global_secondary_indexes[0].projection.projection_type, "INCLUDE")
            self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.last_decrease_date_time, None)
            self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.last_increase_date_time, None)
            self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.number_of_decreases_today, None)
            self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 3)
            self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 4)
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


class CreateTableErrorLocalIntegTests(_tst.LocalIntegTests):
    def testDefineUnusedAttribute(self):
        with self.assertRaises(_lv.ValidationException) as catcher:
            self.connection.request(
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

    def testDontDefineKeyAttribute(self):
        with self.assertRaises(_lv.ValidationException) as catcher:
            self.connection.request(
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

    def testDontDefineAnyAttribute(self):
        with self.assertRaises(_lv.ValidationException) as catcher:
            self.connection.request(
                _lv.CreateTable("Aaa").hash_key("h").provisioned_throughput(1, 2)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "No Attribute Schema Defined",
            },)
        )
