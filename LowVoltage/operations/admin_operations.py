# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation as _Operation
import LowVoltage.return_types as _rtyp
import LowVoltage.attribute_types as _atyp
import LowVoltage.tests.dynamodb_local


# @todo Support LSI and GSI
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


class CreateTableIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def tearDown(self):
        self.connection.request(DeleteTable("Aaa"))

    def testSimplestTable(self):
        r = self.connection.request(
            CreateTable("Aaa").hash_key("h", _atyp.STRING).read_throughput(1).write_throughput(1)
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



if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
