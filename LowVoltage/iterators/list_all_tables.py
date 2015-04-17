# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import MockMockMock

import LowVoltage as _lv
import LowVoltage.testing as _tst


class ListAllTables(object):
    """Make as many ListTables as needed to iterate over all tables"""
    # Don't implement anything else than forward iteration. (Remember PyGithub's PaginatedList; it was too difficult to maintain for niche use-cases)
    # Clients can use raw ListTables actions to implement their specific needs.

    def __init__(self, connection):
        self.__connection = connection
        self.__current_iter = [].__iter__()
        self.__next_start_table = None
        self.__done = False

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.__current_iter.next()
        except StopIteration:
            if self.__done:
                raise
            else:
                r = self.__connection.request(_lv.ListTables().exclusive_start_table_name(self.__next_start_table))
                self.__next_start_table = r.last_evaluated_table_name
                self.__done = self.__next_start_table is None
                self.__current_iter = r.table_names.__iter__()
                return self.__current_iter.next()


class ListAllTablesUnitTests(unittest.TestCase):
    def setUp(self):
        self.mocks = MockMockMock.Engine()
        self.connection = self.mocks.create("connection")

    def tearDown(self):
        self.mocks.tearDown()

    class Checker(object):
        def __init__(self, expected_payload):
            self.__expected_payload = expected_payload

        def __call__(self, args, kwds):
            assert len(args) == 1
            assert len(kwds) == 0
            action, = args
            return action.name == "ListTables" and action.build() == self.__expected_payload

    def test_no_tables(self):
        self.connection.expect.request.withArguments(self.Checker({})).andReturn(_lv.ListTables.Result(TableNames=[]))

        self.assertEqual(
            list(ListAllTables(self.connection.object)),
            []
        )

    def test_one_page(self):
        self.connection.expect.request.withArguments(self.Checker({})).andReturn(_lv.ListTables.Result(TableNames=["A", "B", "C"]))

        self.assertEqual(
            list(ListAllTables(self.connection.object)),
            ["A", "B", "C"]
        )

    def test_one_page_followed_by_empty_page(self):
        self.connection.expect.request.withArguments(self.Checker({})).andReturn(_lv.ListTables.Result(TableNames=["A", "B", "C"], LastEvaluatedTableName="D"))
        self.connection.expect.request.withArguments(self.Checker({"ExclusiveStartTableName": "D"})).andReturn(_lv.ListTables.Result(TableNames=[]))

        self.assertEqual(
            list(ListAllTables(self.connection.object)),
            ["A", "B", "C"]
        )

    def test_several_pages(self):
        self.connection.expect.request.withArguments(self.Checker({})).andReturn(_lv.ListTables.Result(TableNames=["A", "B", "C"], LastEvaluatedTableName="D"))
        self.connection.expect.request.withArguments(self.Checker({"ExclusiveStartTableName": "D"})).andReturn(_lv.ListTables.Result(TableNames=["E", "F", "G"], LastEvaluatedTableName="H"))
        self.connection.expect.request.withArguments(self.Checker({"ExclusiveStartTableName": "H"})).andReturn(_lv.ListTables.Result(TableNames=["I", "J", "K"]))

        self.assertEqual(
            list(ListAllTables(self.connection.object)),
            ["A", "B", "C", "E", "F", "G", "I", "J", "K"]
        )


class ListAllTablesLocalIntegTests(_tst.dynamodb_local.TestCase):
    table_names = ["Tab{:03}".format(i) for i in range(103)]

    def setUp(self):
        for t in self.table_names:
            self.connection.request(
                _lv.CreateTable(t).hash_key("h", _lv.STRING).provisioned_throughput(1, 1)
            )

    def tearDown(self):
        for t in self.table_names:
            self.connection.request(_lv.DeleteTable(t))

    def test(self):
        self.assertEqual(
            list(_lv.ListAllTables(self.connection)),
            self.table_names
        )
