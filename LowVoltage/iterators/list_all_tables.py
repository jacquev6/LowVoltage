# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

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


class ListAllTablesLocalIntegTests(_tst.dynamodb_local.TestCase):
    def __test(self, tables_count):
        table_names = ["Tab{:03}".format(i) for i in range(tables_count)]

        for t in table_names:
            self.connection.request(
                _lv.CreateTable(t).hash_key("h", _lv.STRING).provisioned_throughput(1, 1)
            )

        self.assertEqual(
            list(_lv.ListAllTables(self.connection)),
            table_names
        )

        for t in table_names:
            self.connection.request(_lv.DeleteTable(t))

    def test_list_0_tables(self):
        self.__test(0)

    def test_list_99_tables(self):
        self.__test(99)

    def test_list_100_tables(self):
        self.__test(100)

    def test_list_101_tables(self):
        self.__test(101)

    def test_list_102_tables(self):
        self.__test(102)

    def test_list_250_tables(self):
        self.__test(250)
