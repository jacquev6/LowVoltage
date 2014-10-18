# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation
import LowVoltage.tests.dynamodb_local


class ListTables(Operation):
    class Result(object):
        def __init__(self, TableNames, LastEvaluatedTableName=None):
            self.table_names = TableNames
            self.last_evaluated_table_name = LastEvaluatedTableName

    def __init__(self):
        super(ListTables, self).__init__("ListTables")

    def build(self):
        data = {}
        return data


class ListTablesUnitTests(unittest.TestCase):
    pass


class ListTablesIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def testNoArguments(self):
        r = self.connection.request(ListTables())
        self.assertEqual(r.table_names, [])
        self.assertEqual(r.last_evaluated_table_name, None)


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
