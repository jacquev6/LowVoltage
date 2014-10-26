# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation as _Operation, OperationProxy as _OperationProxy
import LowVoltage.return_types as _rtyp
import LowVoltage.attribute_types as _atyp
import LowVoltage.tests.dynamodb_local
from LowVoltage.tests.cover import cover
import LowVoltage.exceptions as _exn


def _fix_order_for_tests(d):
    d.attribute_definitions = sorted(d.attribute_definitions, key=lambda d: d.attribute_name)


class CreateTable(_Operation):
    class Result(object):
        def __init__(
            self,
            TableDescription=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_ResponseElements
            # - TableDescription: done
            self.table_description = None if TableDescription is None else _rtyp.TableDescription(**TableDescription)

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
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_RequestParameters
        # - AttributeDefinitions: done
        # - KeySchema: done
        # - ProvisionedThroughput: done
        # - TableName: done
        # - GlobalSecondaryIndexes: done
        # - LocalSecondaryIndexes: done
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

    class _Index(_OperationProxy):
        def __init__(self, table, name):
            super(CreateTable._Index, self).__init__(table)
            self.__name = name
            self.__hash_key = None
            self.__range_key = None
            self.__projection = None

        def table(self):
            return self._operation

        def hash_key(self, name, typ=None):
            self.__hash_key = name
            if typ is not None:
                self._operation.attribute_definition(name, typ)
            return self

        def range_key(self, name, typ=None):
            self.__range_key = name
            if typ is not None:
                self._operation.attribute_definition(name, typ)
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
            self.__lsis[name] = self._IndexWithThroughput(self, name)
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
            CreateTable("Foo").hash_key("h", _atyp.STRING).build(),
            {
                "TableName": "Foo",
                "AttributeDefinitions": [{"AttributeName": "h", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "h", "KeyType": "HASH"}],
            }
        )

    def testAttributeDefinition(self):
        self.assertEqual(
            CreateTable("Foo").attribute_definition("h", _atyp.STRING).build(),
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
            CreateTable("Foo").range_key("r", _atyp.STRING).build(),
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
            CreateTable("Foo").global_secondary_index("foo").hash_key("hh", _atyp.STRING).build(),
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
            CreateTable("Foo").global_secondary_index("foo").range_key("rr", _atyp.STRING).build(),
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
            CreateTable("Foo").global_secondary_index("foo").attribute_definition("bar", _atyp.NUMBER).build(),
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


class CreateTableIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def tearDown(self):
        self.connection.request(DeleteTable("Aaa"))

    def testSimplestTable(self):
        r = self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

        with cover("r", r) as r:
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_name, "h")
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_type, "S")
            self.assertEqual(r.table_description.global_secondary_indexes, None)
            self.assertEqual(r.table_description.item_count, 0)
            self.assertEqual(r.table_description.key_schema[0].attribute_name, "h")
            self.assertEqual(r.table_description.key_schema[0].key_type, "HASH")
            self.assertEqual(r.table_description.local_secondary_indexes, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 1)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 2)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")

    def testSimpleGlobalSecondaryIndex(self):
        r = self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _atyp.STRING)
                .project_all()
                .provisioned_throughput(3, 4)
        )

        _fix_order_for_tests(r.table_description)

        with cover("r", r) as r:
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
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 1)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 2)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")

    def testSimpleLocalSecondaryIndex(self):
        r = self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).range_key("r", _atyp.STRING).provisioned_throughput(1, 2)
                .local_secondary_index("the_lsi").hash_key("h").range_key("rr", _atyp.STRING).project_all()
        )

        _fix_order_for_tests(r.table_description)

        with cover("r", r) as r:
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
            self.assertEqual(r.table_description.local_secondary_indexes[0].index_status, None)
            self.assertEqual(r.table_description.local_secondary_indexes[0].item_count, 0)
            self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[0].attribute_name, "h")
            self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[0].key_type, "HASH")
            self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[1].attribute_name, "rr")
            self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[1].key_type, "RANGE")
            self.assertEqual(r.table_description.local_secondary_indexes[0].projection.non_key_attributes, None)
            self.assertEqual(r.table_description.local_secondary_indexes[0].projection.projection_type, "ALL")
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 1)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 2)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")

    def testGlobalSecondaryIndexWithProjection(self):
        r = self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _atyp.STRING)
                .project("toto", "titi")
                .provisioned_throughput(3, 4)
        )

        _fix_order_for_tests(r.table_description)

        with cover("r", r) as r:
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
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 1)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 2)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")


class CreateTableErrorTests(LowVoltage.tests.dynamodb_local.TestCase):
    def testDefineUnusedAttribute(self):
        with self.assertRaises(_exn.ValidationException) as catcher:
            self.connection.request(
                CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
                    .attribute_definition("x", _atyp.STRING)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "The number of attributes in key schema must match the number of attributesdefined in attribute definitions.",
            },)
        )

    def testDontDefineKeyAttribute(self):
        with self.assertRaises(_exn.ValidationException) as catcher:
            self.connection.request(
                CreateTable("Aaa").hash_key("h").provisioned_throughput(1, 2)
                    .attribute_definition("x", _atyp.STRING)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "Hash Key not specified in Attribute Definitions.  Type unknown.",
            },)
        )

    def testDontDefineAnyAttribute(self):
        with self.assertRaises(_exn.ValidationException) as catcher:
            self.connection.request(
                CreateTable("Aaa").hash_key("h").provisioned_throughput(1, 2)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "No Attribute Schema Defined",
            },)
        )


class DeleteTable(_Operation):
    class Result(object):
        def __init__(
            self,
            TableDescription=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#API_DeleteTable_ResponseElements
            # - TableDescription: done
            self.table_description = None if TableDescription is None else _rtyp.TableDescription(**TableDescription)

    def __init__(self, table_name):
        super(DeleteTable, self).__init__("DeleteTable")
        self.__table_name = table_name

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#API_DeleteTable_RequestParameters
        # - TableName: done
        return {"TableName": self.__table_name}


class DeleteTableUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(DeleteTable("Foo").name, "DeleteTable")

    def testBuild(self):
        self.assertEqual(DeleteTable("Foo").build(), {"TableName": "Foo"})


class DeleteTableIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def test(self):
        r = self.connection.request(DeleteTable("Aaa"))

        with cover("r", r) as r:
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_name, "h")
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_type, "S")
            self.assertEqual(r.table_description.global_secondary_indexes, None)
            self.assertEqual(r.table_description.item_count, 0)
            self.assertEqual(r.table_description.key_schema[0].attribute_name, "h")
            self.assertEqual(r.table_description.key_schema[0].key_type, "HASH")
            self.assertEqual(r.table_description.local_secondary_indexes, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 1)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 2)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")


class DescribeTable(_Operation):
    class Result(object):
        def __init__(
            self,
            Table=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html#API_DescribeTable_ResponseElements
            # - Table: done
            self.table = None if Table is None else _rtyp.TableDescription(**Table)

    def __init__(self, table_name):
        super(DescribeTable, self).__init__("DescribeTable")
        self.__table_name = table_name

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html#API_DescribeTable_RequestParameters
        # - TableName: done
        return {"TableName": self.__table_name}


class DescribeTableUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(DescribeTable("Foo").name, "DescribeTable")

    def testBuild(self):
        self.assertEqual(DescribeTable("Foo").build(), {"TableName": "Foo"})


class DescribeTableIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(DeleteTable("Aaa"))

    def test(self):
        r = self.connection.request(DescribeTable("Aaa"))

        with cover("r", r) as r:
            self.assertEqual(r.table.attribute_definitions[0].attribute_name, "h")
            self.assertEqual(r.table.attribute_definitions[0].attribute_type, "S")
            self.assertEqual(r.table.global_secondary_indexes, None)
            self.assertEqual(r.table.item_count, 0)
            self.assertEqual(r.table.key_schema[0].attribute_name, "h")
            self.assertEqual(r.table.key_schema[0].key_type, "HASH")
            self.assertEqual(r.table.local_secondary_indexes, None)
            self.assertEqual(r.table.provisioned_throughput.last_decrease_date_time, None)
            self.assertEqual(r.table.provisioned_throughput.last_increase_date_time, None)
            self.assertEqual(r.table.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table.provisioned_throughput.read_capacity_units, 1)
            self.assertEqual(r.table.provisioned_throughput.write_capacity_units, 2)
            self.assertEqual(r.table.table_name, "Aaa")
            self.assertEqual(r.table.table_size_bytes, 0)
            self.assertEqual(r.table.table_status, "ACTIVE")


class ListTables(_Operation):
    class Result(object):
        def __init__(
            self,
            LastEvaluatedTableName=None,
            TableNames=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html#API_ListTables_ResponseElements
            # - LastEvaluatedTableName: done
            # - TableNames: done
            self.last_evaluated_table_name = LastEvaluatedTableName
            self.table_names = TableNames

    def __init__(self):
        super(ListTables, self).__init__("ListTables")
        self.__limit = None
        self.__exclusive_start_table_name = None

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html#API_ListTables_RequestParameters
        # - ExclusiveStartTableName: done
        # - Limit: done
        data = {}
        if self.__limit:
            data["Limit"] = str(self.__limit)
        if self.__exclusive_start_table_name:
            data["ExclusiveStartTableName"] = self.__exclusive_start_table_name
        return data

    def limit(self, limit):
        self.__limit = limit
        return self

    def exclusive_start_table_name(self, table_name):
        self.__exclusive_start_table_name = table_name
        return self


class ListTablesUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(ListTables().name, "ListTables")

    def testNoArguments(self):
        self.assertEqual(ListTables().build(), {})

    def testLimit(self):
        self.assertEqual(ListTables().limit(42).build(), {"Limit": "42"})

    def testExclusiveStartTableName(self):
        self.assertEqual(ListTables().exclusive_start_table_name("Bar").build(), {"ExclusiveStartTableName": "Bar"})


class ListTablesIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )
        self.connection.request(
            CreateTable("Bbb").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )
        self.connection.request(
            CreateTable("Ccc").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(DeleteTable("Aaa"))
        self.connection.request(DeleteTable("Bbb"))
        self.connection.request(DeleteTable("Ccc"))

    def testAllArguments(self):
        r = self.connection.request(ListTables().exclusive_start_table_name("Aaa").limit(1))

        with cover("r", r) as r:
            self.assertEqual(r.last_evaluated_table_name, "Bbb")
            self.assertEqual(r.table_names[0], "Bbb")

    def testNoArguments(self):
        r = self.connection.request(ListTables())

        with cover("r", r) as r:
            self.assertEqual(r.last_evaluated_table_name, None)
            self.assertEqual(r.table_names[0], "Aaa")
            self.assertEqual(r.table_names[1], "Bbb")
            self.assertEqual(r.table_names[2], "Ccc")


class UpdateTable(_Operation):
    class Result(object):
        def __init__(
            self,
            TableDescription=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html#API_UpdateTable_ResponseElements
            # - TableDescription: done
            self.table_description = None if TableDescription is None else _rtyp.TableDescription(**TableDescription)

    def __init__(self, table_name):
        super(UpdateTable, self).__init__("UpdateTable")
        self.__table_name = table_name
        self.__read_capacity_units = None
        self.__write_capacity_units = None
        self.__gsis = {}

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html#API_UpdateTable_RequestParameters
        # - TableName: done
        # - GlobalSecondaryIndexUpdates: done
        # - ProvisionedThroughput: done
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

    class _IndexWithThroughput(_OperationProxy):
        def __init__(self, table, name):
            super(UpdateTable._IndexWithThroughput, self).__init__(table)
            self.__name = name
            self.__read_capacity_units = None
            self.__write_capacity_units = None

        def table(self):
            return self._operation

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
        self.__read_capacity_units = read_capacity_units
        self.__write_capacity_units = write_capacity_units
        return self

    def global_secondary_index(self, name):
        if name not in self.__gsis:
            self.__gsis[name] = self._IndexWithThroughput(self, name)
        return self.__gsis[name]


class UpdateTableUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(UpdateTable("Foo").name, "UpdateTable")

    def testNoArguments(self):
        self.assertEqual(UpdateTable("Foo").build(), {"TableName": "Foo"})

    def testThroughput(self):
        self.assertEqual(
            UpdateTable("Foo").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def testGsi(self):
        self.assertEqual(
            UpdateTable("Foo").global_secondary_index("the_gsi").build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi"}},
                ],
            }
        )

    def testGsiprovisioned_Throughput(self):
        self.assertEqual(
            UpdateTable("Foo").global_secondary_index("the_gsi").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
                ],
            }
        )

    def testBackToGsiAfterBackToTable(self):
        self.assertEqual(
            UpdateTable("Foo").global_secondary_index("the_gsi").table().provisioned_throughput(12, 13).global_secondary_index("the_gsi").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 12, "WriteCapacityUnits": 13},
            }
        )


class UpdateTableIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _atyp.STRING)
                .project_all()
                .provisioned_throughput(3, 4)
        )

    def tearDown(self):
        self.connection.request(DeleteTable("Aaa"))

    def testThroughput(self):
        r = self.connection.request(
            UpdateTable("Aaa").provisioned_throughput(2, 4)
        )

        _fix_order_for_tests(r.table_description)

        with cover("r", r) as r:
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
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 2)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 4)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")

    def testGsiprovisioned_Throughput(self):
        r = self.connection.request(
            UpdateTable("Aaa").global_secondary_index("the_gsi").provisioned_throughput(6, 8)
        )

        _fix_order_for_tests(r.table_description)

        with cover("r", r) as r:
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
            self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 6)
            self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 8)
            self.assertEqual(r.table_description.item_count, 0)
            self.assertEqual(r.table_description.key_schema[0].attribute_name, "h")
            self.assertEqual(r.table_description.key_schema[0].key_type, "HASH")
            self.assertEqual(r.table_description.local_secondary_indexes, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, None)
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 1)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 2)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
