# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`ListTables`, the connection will return a :class:`ListTablesResponse`:

>>> r = connection(ListTables())
>>> r
<LowVoltage.actions.list_tables.ListTablesResponse ...>
>>> r.table_names
[u'LowVoltage.Tests.Doc.1', u'LowVoltage.Tests.Doc.2']

See also the :func:`.iterate_list_tables` compound. And :ref:`actions-vs-compounds` in the user guide.
"""

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .return_types import _is_str, _is_list_of_str
from .next_gen_mixins import OptionalIntParameter, OptionalStringParameter


class ListTablesResponse(object):
    """
    The `ListTables response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html#API_ListTables_ResponseElements>`__.
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
        The name of the last table that was considered during the request.
        If not None, you should give it to :meth:`~ListTables.exclusive_start_table_name` in a subsequent :class:`ListTables`.

        The :func:`.iterate_list_tables` compound does that for you.

        :type: ``None`` or string
        """
        if _is_str(self.__last_evaluated_table_name):
            return self.__last_evaluated_table_name

    @property
    def table_names(self):
        """
        The names of the tables.

        :type: ``None`` or list of string
        """
        if _is_list_of_str(self.__table_names):
            return self.__table_names


class ListTables(Action):
    """
    The `ListTables request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html#API_ListTables_RequestParameters>`__.
    """

    def __init__(self):
        super(ListTables, self).__init__("ListTables", ListTablesResponse)
        self.__limit = OptionalIntParameter("Limit", self)
        self.__exclusive_start_table_name = OptionalStringParameter("ExclusiveStartTableName", self)

    @property
    def payload(self):
        data = {}
        data.update(self.__limit.payload)
        data.update(self.__exclusive_start_table_name.payload)
        return data

    def limit(self, limit):
        """
        Set Limit. The response will contain at most this number of table names.

        >>> r = connection(ListTables().limit(1))
        >>> r.table_names
        [u'LowVoltage.Tests.Doc.1']
        >>> r.last_evaluated_table_name
        u'LowVoltage.Tests.Doc.1'
        """
        return self.__limit.set(limit)

    def exclusive_start_table_name(self, table_name):
        """
        Set ExclusiveStartTableName. The response will contains tables that are after this one.
        Typically the :attr:`~ListTablesResponse.last_evaluated_table_name` of a previous response.

        The :func:`.iterate_list_tables` compound does that for you.

        >>> connection(
        ...   ListTables()
        ...     .exclusive_start_table_name("LowVoltage.Tests.Doc.1")
        ... ).table_names
        [u'LowVoltage.Tests.Doc.2']
        """
        return self.__exclusive_start_table_name.set(table_name)


class ListTablesUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(ListTables().name, "ListTables")

    def test_no_arguments(self):
        self.assertEqual(ListTables().payload, {})

    def test_limit(self):
        self.assertEqual(ListTables().limit(42).payload, {"Limit": 42})

    def test_exclusive_start_table_name(self):
        self.assertEqual(ListTables().exclusive_start_table_name("Bar").payload, {"ExclusiveStartTableName": "Bar"})


class ListTablesResponseUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = ListTablesResponse()
        self.assertIsNone(r.last_evaluated_table_name)
        self.assertIsNone(r.table_names)

    def test_all_set(self):
        unprocessed_keys = object()
        r = ListTablesResponse(LastEvaluatedTableName="Foo", TableNames=["Bar"])
        self.assertEqual(r.last_evaluated_table_name, "Foo")
        self.assertEqual(r.table_names, ["Bar"])
