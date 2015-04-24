# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .return_types import _is_str, _is_list_of_str


class ListTablesResponse(object):
    """
    The `ListTables request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html#API_ListTables_ResponseElements>`__.
    """

    def __init__(
        self,
        LastEvaluatedTableName=None,
        TableNames=None,
        **dummy
    ):
        self.__last_evaluated_table_name = LastEvaluatedTableName
        self.__table_names = TableNames

    @property
    def last_evaluated_table_name(self):
        """
        If not None, you should give it to :meth:`.exclusive_start_table_name` in a subsequent :class:`.ListTables`.

        :type: None or string
        """
        if _is_str(self.__last_evaluated_table_name):  # pragma no branch (Defensive code)
            return self.__last_evaluated_table_name

    @property
    def table_names(self):
        """
        :type: None or list of string
        """
        if _is_list_of_str(self.__table_names):  # pragma no branch (Defensive code)
            return self.__table_names


class ListTables(Action):
    """
    The `ListTables response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html#API_ListTables_RequestParameters>`__.
    """

    def __init__(self):
        super(ListTables, self).__init__("ListTables")
        self.__limit = None
        self.__exclusive_start_table_name = None

    def build(self):
        data = {}
        if self.__limit:
            data["Limit"] = str(self.__limit)
        if self.__exclusive_start_table_name:
            data["ExclusiveStartTableName"] = self.__exclusive_start_table_name
        return data

    @staticmethod
    def Result(**kwds):
        return ListTablesResponse(**kwds)

    def limit(self, limit):
        """
        @todo Document
        """
        self.__limit = limit
        return self

    def exclusive_start_table_name(self, table_name):
        """
        @todo Document
        """
        self.__exclusive_start_table_name = table_name
        return self


class ListTablesUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(ListTables().name, "ListTables")

    def test_no_arguments(self):
        self.assertEqual(ListTables().build(), {})

    def test_limit(self):
        self.assertEqual(ListTables().limit(42).build(), {"Limit": "42"})

    def test_exclusive_start_table_name(self):
        self.assertEqual(ListTables().exclusive_start_table_name("Bar").build(), {"ExclusiveStartTableName": "Bar"})


class ListTablesLocalIntegTests(_tst.LocalIntegTests):
    def setUp(self):
        super(ListTablesLocalIntegTests, self).setUp()
        self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )
        self.connection(
            _lv.CreateTable("Bbb").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )
        self.connection(
            _lv.CreateTable("Ccc").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        self.connection(_lv.DeleteTable("Bbb"))
        self.connection(_lv.DeleteTable("Ccc"))
        super(ListTablesLocalIntegTests, self).tearDown()

    def test_all_arguments(self):
        r = self.connection(_lv.ListTables().exclusive_start_table_name("Aaa").limit(1))

        self.assertEqual(r.last_evaluated_table_name, "Bbb")
        self.assertEqual(r.table_names[0], "Bbb")

    def test_no_arguments(self):
        r = self.connection(_lv.ListTables())

        self.assertEqual(r.table_names[0], "Aaa")
        self.assertEqual(r.table_names[1], "Bbb")
        self.assertEqual(r.table_names[2], "Ccc")
