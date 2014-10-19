# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation as _Operation
import LowVoltage.types as _typ
import LowVoltage.tests.dynamodb_local


class DeleteTable(_Operation):
    class Result(object):
        def __init__(self, TableDescription=None):
            self.table_description = None if TableDescription is None else _typ.TableDescription(**TableDescription)

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
    def test(self):
        self.connection.request(
            "CreateTable",
            {
                "TableName": "Aaa",
                "AttributeDefinitions": [{"AttributeName": "hash", "AttributeType": "S"}],
                "KeySchema":[{"AttributeName": "hash", "KeyType": "HASH"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            }
        )

        r = self.connection.request(DeleteTable("Aaa"))
        # r.table_description.creation_date_time
        self.assertEqual(r.table_description.item_count, 0)
        self.assertEqual(r.table_description.table_name, "Aaa")
        self.assertEqual(r.table_description.table_size_bytes, 0)
        self.assertEqual(r.table_description.table_status, "ACTIVE")  # Should be "DELETING"?


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
    def testNoArguments(self):
        r = self.connection.request(ListTables())
        self.assertEqual(r.table_names, [])
        self.assertEqual(r.last_evaluated_table_name, None)

    def testAllArguments(self):
        self.connection.request(
            "CreateTable",
            {
                "TableName": "Aaa",
                "AttributeDefinitions": [{"AttributeName": "hash", "AttributeType": "S"}],
                "KeySchema":[{"AttributeName": "hash", "KeyType": "HASH"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            }
        )
        self.connection.request(
            "CreateTable",
            {
                "TableName": "Bbb",
                "AttributeDefinitions": [{"AttributeName": "hash", "AttributeType": "S"}],
                "KeySchema":[{"AttributeName": "hash", "KeyType": "HASH"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            }
        )
        self.connection.request(
            "CreateTable",
            {
                "TableName": "Ccc",
                "AttributeDefinitions": [{"AttributeName": "hash", "AttributeType": "S"}],
                "KeySchema":[{"AttributeName": "hash", "KeyType": "HASH"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            }
        )

        r = self.connection.request(ListTables().exclusive_start_table_name("Aaa").limit(1))
        self.assertEqual(r.table_names, ["Bbb"])
        self.assertEqual(r.last_evaluated_table_name, "Bbb")

        self.connection.request(DeleteTable("Aaa"))
        self.connection.request(DeleteTable("Bbb"))
        self.connection.request(DeleteTable("Ccc"))


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
