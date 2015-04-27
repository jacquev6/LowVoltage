# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


def iterate_list_tables(connection):
    """
    Make as many :class:`.ListTables` actions as needed to iterate over all tables.
    That is until :attr:`.ListTablesResponse.last_evaluated_table_name` is ``None``.

    >>> for table in iterate_list_tables(connection):
    ...   print table
    LowVoltage.Tests.Doc.1
    LowVoltage.Tests.Doc.2
    """

    r = connection(_lv.ListTables())
    for table_name in r.table_names:
        yield table_name
    while r.last_evaluated_table_name is not None:
        r = connection(_lv.ListTables().exclusive_start_table_name(r.last_evaluated_table_name))
        for table_name in r.table_names:
            yield table_name


class IterateListTablesUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(IterateListTablesUnitTests, self).setUp()
        self.connection = self.mocks.create("connection")

    def test_no_tables(self):
        self.connection.expect._call_.withArguments(self.ActionChecker("ListTables", {})).andReturn(_lv.ListTablesResponse(TableNames=[]))

        self.assertEqual(
            list(iterate_list_tables(self.connection.object)),
            []
        )

    def test_one_page(self):
        self.connection.expect._call_.withArguments(self.ActionChecker("ListTables", {})).andReturn(_lv.ListTablesResponse(TableNames=["A", "B", "C"]))

        self.assertEqual(
            list(iterate_list_tables(self.connection.object)),
            ["A", "B", "C"]
        )

    def test_one_page_followed_by_empty_page(self):
        self.connection.expect._call_.withArguments(self.ActionChecker("ListTables", {})).andReturn(_lv.ListTablesResponse(TableNames=["A", "B", "C"], LastEvaluatedTableName="D"))
        self.connection.expect._call_.withArguments(self.ActionChecker("ListTables", {"ExclusiveStartTableName": "D"})).andReturn(_lv.ListTablesResponse(TableNames=[]))

        self.assertEqual(
            list(iterate_list_tables(self.connection.object)),
            ["A", "B", "C"]
        )

    def test_several_pages(self):
        self.connection.expect._call_.withArguments(self.ActionChecker("ListTables", {})).andReturn(_lv.ListTablesResponse(TableNames=["A", "B", "C"], LastEvaluatedTableName="D"))
        self.connection.expect._call_.withArguments(self.ActionChecker("ListTables", {"ExclusiveStartTableName": "D"})).andReturn(_lv.ListTablesResponse(TableNames=["E", "F", "G"], LastEvaluatedTableName="H"))
        self.connection.expect._call_.withArguments(self.ActionChecker("ListTables", {"ExclusiveStartTableName": "H"})).andReturn(_lv.ListTablesResponse(TableNames=["I", "J", "K"]))

        self.assertEqual(
            list(iterate_list_tables(self.connection.object)),
            ["A", "B", "C", "E", "F", "G", "I", "J", "K"]
        )
