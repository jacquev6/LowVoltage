# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation as _Operation, OperationProxy as _OperationProxy
import LowVoltage.return_types as _rtyp
import LowVoltage.attribute_types as _atyp
import LowVoltage.tests.dynamodb_local


class CreateTable(_Operation):
    class Result(object):
        def __init__(self, TableDescription=None):
            self.table_description = None if TableDescription is None else _rtyp.TableDescription(**TableDescription)

    def __init__(self, table_name):
        super(CreateTable, self).__init__("CreateTable")
        self.__table_name = table_name
        self.__hash_key = None
        self.__range_key = None
        self.__attribute_types = {}
        self.__read_throughput = None
        self.__write_throughput = None
        self.__gsis = {}
        self.__lsis = {}

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
                self._operation.attribute(name, typ)
            return self

        def range_key(self, name, typ=None):
            self.__range_key = name
            if typ is not None:
                self._operation.attribute(name, typ)
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
            self.__read_throughput = None
            self.__write_throughput = None

        def read_throughput(self, units):
            self.__read_throughput = units
            return self

        def write_throughput(self, units):
            self.__write_throughput = units
            return self

        def _build(self):
            data = super(CreateTable._IndexWithThroughput, self)._build()
            throughput = {}
            if self.__read_throughput:
                throughput["ReadCapacityUnits"] = self.__read_throughput
            if self.__write_throughput:
                throughput["WriteCapacityUnits"] = self.__write_throughput
            if throughput:
                data["ProvisionedThroughput"] = throughput
            return data

    def hash_key(self, name, typ=None):
        self.__hash_key = name
        if typ is not None:
            self.attribute(name, typ)
        return self

    def range_key(self, name, typ=None):
        self.__range_key = name
        if typ is not None:
            self.attribute(name, typ)
        return self

    def attribute(self, name, typ):
        self.__attribute_types[name] = typ
        return self

    def read_throughput(self, units):
        self.__read_throughput = units
        return self

    def write_throughput(self, units):
        self.__write_throughput = units
        return self

    def global_secondary_index(self, name):
        if name not in self.__gsis:
            self.__gsis[name] = self._IndexWithThroughput(self, name)
        return self.__gsis[name]

    def local_secondary_index(self, name):
        if name not in self.__lsis:
            self.__lsis[name] = self._IndexWithThroughput(self, name)
        return self.__lsis[name]

    def build(self):
        data = {"TableName": self.__table_name}
        schema = []
        if self.__hash_key:
            schema.append({"AttributeName": self.__hash_key, "KeyType": "HASH"})
        if self.__range_key:
            schema.append({"AttributeName": self.__range_key, "KeyType": "RANGE"})
        if schema:
            data["KeySchema"] = schema
        if self.__attribute_types:
            data["AttributeDefinitions"] = [
                {"AttributeName": name, "AttributeType": typ}
                for name, typ in self.__attribute_types.iteritems()
            ]
        throughput = {}
        if self.__read_throughput:
            throughput["ReadCapacityUnits"] = self.__read_throughput
        if self.__write_throughput:
            throughput["WriteCapacityUnits"] = self.__write_throughput
        if throughput:
            data["ProvisionedThroughput"] = throughput
        if self.__gsis:
            data["GlobalSecondaryIndexes"] = [i._build() for i in self.__gsis.itervalues()]
        if self.__lsis:
            data["LocalSecondaryIndexes"] = [i._build() for i in self.__lsis.itervalues()]
        return data


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

    def testAttribute(self):
        self.assertEqual(
            CreateTable("Foo").attribute("h", _atyp.STRING).build(),
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

    def testReadThroughput(self):
        self.assertEqual(
            CreateTable("Foo").read_throughput(42).build(),
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"ReadCapacityUnits": 42},
            }
        )

    def testWriteThroughput(self):
        self.assertEqual(
            CreateTable("Foo").write_throughput(42).build(),
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"WriteCapacityUnits": 42},
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

    def testGlobalSecondaryIndexWriteThrouput(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").write_throughput(42).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "ProvisionedThroughput": {"WriteCapacityUnits": 42}},
                ],
            }
        )

    def testGlobalSecondaryIndexReadThroughput(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").read_throughput(42).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "ProvisionedThroughput": {"ReadCapacityUnits": 42}},
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
            CreateTable("Foo").global_secondary_index("foo").table().read_throughput(42).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [{"IndexName": "foo"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 42},
            }
        )

    def testImplicitBackToTableAfterGsi(self):
        self.assertEqual(
            CreateTable("Foo").global_secondary_index("foo").attribute("bar", _atyp.NUMBER).build(),
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
                .table().read_throughput(42)
                .global_secondary_index("foo").write_throughput(12)
                .build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexes": [
                    {"IndexName": "foo", "ProvisionedThroughput": {"WriteCapacityUnits": 12}}
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 42},
            }
        )

    def testBackToLsiAfterBackToTable(self):
        self.assertEqual(
            CreateTable("Foo")
                .local_secondary_index("foo")
                .table().read_throughput(42)
                .local_secondary_index("foo").project_all()
                .build(),
            {
                "TableName": "Foo",
                "LocalSecondaryIndexes": [
                    {"IndexName": "foo", "Projection": {"ProjectionType": "ALL"}}
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 42},
            }
        )


class CreateTableIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def tearDown(self):
        self.connection.request(DeleteTable("Aaa"))

    def testSimplestTable(self):
        r = self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
        )

        # @todo Assert all members
        self.assertEqual(r.table_description.table_name, "Aaa")

    def testSimpleGlobalSecondaryIndex(self):
        r = self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _atyp.STRING)
                .project_all()
                .read_throughput(2).write_throughput(2)
        )

        # @todo Assert all members
        self.assertEqual(r.table_description.table_name, "Aaa")

    def testSimpleLocalSecondaryIndex(self):
        r = self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).range_key("r", _atyp.STRING).read_throughput(1).write_throughput(1)
                .local_secondary_index("the_lsi").hash_key("h").range_key("rr", _atyp.STRING).project_all()
        )

        # @todo Assert all members
        self.assertEqual(r.table_description.table_name, "Aaa")

    def testGlobalSecondaryIndexWithProjection(self):
        r = self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _atyp.STRING)
                .project("toto", "titi")
                .read_throughput(2).write_throughput(2)
        )

        # @todo Assert all members
        self.assertEqual(r.table_description.table_name, "Aaa")


class DeleteTable(_Operation):
    class Result(object):
        def __init__(self, TableDescription=None):
            self.table_description = None if TableDescription is None else _rtyp.TableDescription(**TableDescription)

    def __init__(self, table_name):
        super(DeleteTable, self).__init__("DeleteTable")
        self.__table_name = table_name

    def build(self):
        return {"TableName": self.__table_name}


class DeleteTableUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(DeleteTable("Foo").name, "DeleteTable")

    def testBuild(self):
        self.assertEqual(DeleteTable("Foo").build(), {"TableName": "Foo"})


class DeleteTableIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
        )

    def test(self):
        r = self.connection.request(DeleteTable("Aaa"))

        # @todo Assert all members
        self.assertEqual(r.table_description.item_count, 0)
        self.assertEqual(r.table_description.table_name, "Aaa")
        self.assertEqual(r.table_description.table_size_bytes, 0)
        self.assertEqual(r.table_description.table_status, "ACTIVE")  # Should be "DELETING"?


class DescribeTable(_Operation):
    class Result(object):
        def __init__(self, Table=None):
            self.table = None if Table is None else _rtyp.TableDescription(**Table)

    def __init__(self, table_name):
        super(DescribeTable, self).__init__("DescribeTable")
        self.__table_name = table_name

    def build(self):
        return {"TableName": self.__table_name}


class DescribeTableUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(DescribeTable("Foo").name, "DescribeTable")

    def testBuild(self):
        self.assertEqual(DescribeTable("Foo").build(), {"TableName": "Foo"})


class DescribeTableIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
        )

    def tearDown(self):
        self.connection.request(DeleteTable("Aaa"))

    def test(self):
        r = self.connection.request(DescribeTable("Aaa"))

        # @todo Assert all members
        self.assertEqual(r.table.item_count, 0)
        self.assertEqual(r.table.table_name, "Aaa")
        self.assertEqual(r.table.table_size_bytes, 0)
        self.assertEqual(r.table.table_status, "ACTIVE")


class ListTables(_Operation):
    class Result(object):
        def __init__(self, TableNames=None, LastEvaluatedTableName=None):
            self.table_names = TableNames
            self.last_evaluated_table_name = LastEvaluatedTableName

    def __init__(self):
        super(ListTables, self).__init__("ListTables")
        self.__limit = None
        self.__exclusive_start_table_name = None

    def limit(self, limit):
        self.__limit = limit
        return self

    def exclusive_start_table_name(self, table_name):
        self.__exclusive_start_table_name = table_name
        return self

    def build(self):
        data = {}
        if self.__limit:
            data["Limit"] = str(self.__limit)
        if self.__exclusive_start_table_name:
            data["ExclusiveStartTableName"] = self.__exclusive_start_table_name
        return data


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
            CreateTable("Aaa").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
        )
        self.connection.request(
            CreateTable("Bbb").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
        )
        self.connection.request(
            CreateTable("Ccc").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
        )

    def tearDown(self):
        self.connection.request(DeleteTable("Aaa"))
        self.connection.request(DeleteTable("Bbb"))
        self.connection.request(DeleteTable("Ccc"))

    def testAllArguments(self):
        r = self.connection.request(ListTables().exclusive_start_table_name("Aaa").limit(1))
        self.assertEqual(r.table_names, ["Bbb"])
        self.assertEqual(r.last_evaluated_table_name, "Bbb")

    def testNoArguments(self):
        r = self.connection.request(ListTables())
        self.assertEqual(r.table_names, ["Aaa", "Bbb", "Ccc"])
        self.assertEqual(r.last_evaluated_table_name, None)


# @todo Support LSI and GSI
class UpdateTable(_Operation):
    class Result(object):
        def __init__(self, TableDescription=None):
            self.table_description = None if TableDescription is None else _rtyp.TableDescription(**TableDescription)

    def __init__(self, table_name):
        super(UpdateTable, self).__init__("UpdateTable")
        self.__table_name = table_name
        self.__read_throughput = None
        self.__write_throughput = None

    def read_throughput(self, units):
        self.__read_throughput = units
        return self

    def write_throughput(self, units):
        self.__write_throughput = units
        return self

    def build(self):
        data = {"TableName": self.__table_name}
        throughput = {}
        if self.__read_throughput:
            throughput["ReadCapacityUnits"] = self.__read_throughput
        if self.__write_throughput:
            throughput["WriteCapacityUnits"] = self.__write_throughput
        if throughput:
            data["ProvisionedThroughput"] = throughput
        return data


class UpdateTableUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(UpdateTable("Foo").name, "UpdateTable")

    def testNoArguments(self):
        self.assertEqual(UpdateTable("Foo").build(), {"TableName": "Foo"})

    def testReadThroughput(self):
        self.assertEqual(
            UpdateTable("Foo").read_throughput(42).build(),
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"ReadCapacityUnits": 42},
            }
        )

    def testWriteThroughput(self):
        self.assertEqual(
            UpdateTable("Foo").write_throughput(42).build(),
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"WriteCapacityUnits": 42},
            }
        )


class UpdateTableIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
        )

    def tearDown(self):
        self.connection.request(DeleteTable("Aaa"))

    def testThroughput(self):
        r = self.connection.request(
            UpdateTable("Aaa").read_throughput(2).write_throughput(2)
        )

        # @todo Assert all members
        self.assertEqual(r.table_description.table_name, "Aaa")


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
