# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
@todo Document
"""

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_value_to_db, _convert_db_to_dict
from .next_gen_mixins import proxy
from .next_gen_mixins import (
    ConsistentRead,
    ExclusiveStartKey,
    ExpressionAttributeNames,
    ExpressionAttributeValues,
    FilterExpression,
    Limit,
    ProjectionExpression,
    ReturnConsumedCapacity,
    ScalarValue,
    Select,
)
from .return_types import ConsumedCapacity, _is_dict, _is_int, _is_list_of_dict


class QueryResponse(object):
    """
    The `Query response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_ResponseElements>`__.
    """

    def __init__(
        self,
        ConsumedCapacity=None,
        Count=None,
        Items=None,
        LastEvaluatedKey=None,
        ScannedCount=None,
        **dummy
    ):
        self.__consumed_capacity = ConsumedCapacity
        self.__count = Count
        self.__items = Items
        self.__last_evaluated_key = LastEvaluatedKey
        self.__scanned_count = ScannedCount

    @property
    def consumed_capacity(self):
        """
        The capacity consumed by the request. If you used :meth:`~.Query.return_consumed_capacity_total` or :meth:`~.Query.return_consumed_capacity_indexes`.

        :type: None or :class:`.ConsumedCapacity`
        """
        if _is_dict(self.__consumed_capacity):  # pragma no branch (Defensive code)
            return ConsumedCapacity(**self.__consumed_capacity)

    @property
    def count(self):
        """
        The number of items matching the query.

        :type: None or long
        """
        if _is_int(self.__count):  # pragma no branch (Defensive code)
            return long(self.__count)

    @property
    def items(self):
        """
        The items matching the query. Unless you used :meth:`.Query.select_count`.

        :type: None or list of dict
        """
        if _is_list_of_dict(self.__items):  # pragma no branch (Defensive code)
            return [_convert_db_to_dict(i) for i in self.__items]

    @property
    def last_evaluated_key(self):
        """
        The key of the last item evaluated by the query. If not None, it should be given to :meth:`~.Query.exclusive_start_key` is a subsequent :class:`.Query`.

        :type: None or dict
        """
        if _is_dict(self.__last_evaluated_key):  # pragma no branch (Defensive code)
            return _convert_db_to_dict(self.__last_evaluated_key)

    @property
    def scanned_count(self):
        """
        The number of item scanned during the query. This can be different from :attr:`~.QueryResponse.count` when using :meth:`~.Query.filter_expression`.

        :type: None or long
        """
        if _is_int(self.__scanned_count):  # pragma no branch (Defensive code)
            return long(self.__scanned_count)


class Query(Action):
    """
    The `Query request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestParameters>`__.
    """

    def __init__(self, table_name):
        super(Query, self).__init__("Query")
        self.__table_name = table_name
        self.__conditions = {}
        self.__consistent_read = ConsistentRead(self)
        self.__exclusive_start_key = ExclusiveStartKey(self)
        self.__expression_attribute_names = ExpressionAttributeNames(self)
        self.__expression_attribute_values = ExpressionAttributeValues(self)
        self.__filter_expression = FilterExpression(self)
        self.__index_name = ScalarValue("IndexName")(self)
        self.__limit = Limit(self)
        self.__projection_expression = ProjectionExpression(self)
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__scan_index_forward = ScalarValue("ScanIndexForward")(self)
        self.__select = Select(self)

    def build(self):
        data = {"TableName": self.__table_name}
        if self.__conditions:
            data["KeyConditions"] = self.__conditions
        data.update(self.__consistent_read.build())
        data.update(self.__exclusive_start_key.build())
        data.update(self.__expression_attribute_names.build())
        data.update(self.__expression_attribute_values.build())
        data.update(self.__filter_expression.build())
        data.update(self.__index_name.build())
        data.update(self.__limit.build())
        data.update(self.__projection_expression.build())
        data.update(self.__return_consumed_capacity.build())
        data.update(self.__scan_index_forward.build())
        data.update(self.__select.build())
        return data

    @staticmethod
    def Result(**kwds):
        return QueryResponse(**kwds)

    def key_eq(self, name, value):
        """
        @todo Document
        """
        self.__conditions[name] = {"ComparisonOperator": "EQ", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_le(self, name, value):
        """
        @todo Document
        """
        self.__conditions[name] = {"ComparisonOperator": "LE", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_lt(self, name, value):
        """
        @todo Document
        """
        self.__conditions[name] = {"ComparisonOperator": "LT", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_ge(self, name, value):
        """
        @todo Document
        """
        self.__conditions[name] = {"ComparisonOperator": "GE", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_gt(self, name, value):
        """
        @todo Document
        """
        self.__conditions[name] = {"ComparisonOperator": "GT", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_begins_with(self, name, value):
        """
        @todo Document
        """
        self.__conditions[name] = {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_between(self, name, lo, hi):
        """
        @todo Document
        """
        self.__conditions[name] = {"ComparisonOperator": "BETWEEN", "AttributeValueList": [_convert_value_to_db(lo), _convert_value_to_db(hi)]}
        return self

    @proxy("Query")
    def exclusive_start_key(self, key):
        """
        @todo doctest
        """
        return self.__exclusive_start_key.set(key)

    @proxy
    def limit(self, limit):
        """
        @todo doctest
        """
        return self.__limit.set(limit)

    @proxy
    def select_count(self):
        """
        @todo doctest
        """
        return self.__select.count()

    @proxy
    def select_all_attributes(self):
        """
        @todo doctest
        """
        return self.__select.all_attributes()

    @proxy
    def select_all_projected_attributes(self):
        """
        @todo doctest
        """
        return self.__select.all_projected_attributes()

    def index_name(self, name):
        """
        Set Index. The request will use this index instead of the table key.

        @todo doctest
        """
        return self.__index_name.set(name)

    def scan_index_forward_true(self):
        """
        @todo Document
        """
        return self.__scan_index_forward.set(True)

    def scan_index_forward_false(self):
        """
        @todo Document
        """
        return self.__scan_index_forward.set(False)

    @proxy
    def project(self, *names):
        """
        @todo doctest
        """
        return self.__projection_expression.add(*names)

    @proxy
    def filter_expression(self, expression):
        """
        @todo doctest
        """
        return self.__filter_expression.set(expression)

    @proxy
    def expression_attribute_name(self, synonym, name):
        """
        @todo doctest
        """
        return self.__expression_attribute_names.add(synonym, name)

    @proxy
    def expression_attribute_value(self, name, value):
        """
        @todo doctest
        """
        return self.__expression_attribute_values.add(name, value)

    @proxy
    def consistent_read_true(self):
        """
        >>> connection(
        ...   Query(table)
        ...     .key_eq("h", 0)
        ...     .consistent_read_true()
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        1.0
        """
        return self.__consistent_read.true()

    @proxy
    def consistent_read_false(self):
        """
        >>> connection(
        ...   Query(table)
        ...     .key_eq("h", 0)
        ...     .consistent_read_false()
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        0.5
        """
        return self.__consistent_read.false()

    @proxy
    def return_consumed_capacity_total(self):
        """
        >>> connection(
        ...   Query(table)
        ...     .key_eq("h", 0)
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        0.5
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_indexes(self):
        """
        >>> c1 = connection(
        ...   Query(table)
        ...     .key_eq("h", 0)
        ...     .return_consumed_capacity_indexes()
        ... ).consumed_capacity
        >>> c1.capacity_units
        0.5
        >>> c1.table.capacity_units
        0.5
        >>> print c1.global_secondary_indexes
        None

        >>> c2 = connection(
        ...   Query(table)
        ...     .index_name("gsi")
        ...     .key_eq("gh", 0)
        ...     .return_consumed_capacity_indexes()
        ... ).consumed_capacity
        >>> c2.capacity_units
        0.5
        >>> c2.table.capacity_units
        0.0
        >>> c2.global_secondary_indexes["gsi"].capacity_units
        0.5
        """
        return self.__return_consumed_capacity.indexes()

    @proxy
    def return_consumed_capacity_none(self):
        """
        >>> print connection(
        ...   Query(table)
        ...     .key_eq("h", 0)
        ...     .return_consumed_capacity_none()
        ... ).consumed_capacity
        None
        """
        return self.__return_consumed_capacity.none()


class QueryUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(Query("Aaa").name, "Query")

    def test_table_name(self):
        self.assertEqual(Query("Aaa").build(), {"TableName": "Aaa"})

    def test_key_eq(self):
        self.assertEqual(
            Query("Aaa").key_eq("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def test_key_le(self):
        self.assertEqual(
            Query("Aaa").key_le("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def test_key_lt(self):
        self.assertEqual(
            Query("Aaa").key_lt("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def test_key_ge(self):
        self.assertEqual(
            Query("Aaa").key_ge("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def test_key_gt(self):
        self.assertEqual(
            Query("Aaa").key_gt("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "GT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def test_key_begins_with(self):
        self.assertEqual(
            Query("Aaa").key_begins_with("name", u"prefix").build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [{"S": "prefix"}]}},
            }
        )

    def test_key_between(self):
        self.assertEqual(
            Query("Aaa").key_between("name", 42, 44).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "42"}, {"N": "44"}]}},
            }
        )

    def test_exclusive_start_key(self):
        self.assertEqual(Query("Aaa").exclusive_start_key({"h": u"v"}).build(), {"TableName": "Aaa", "ExclusiveStartKey": {"h": {"S": "v"}}})

    def test_limit(self):
        self.assertEqual(Query("Aaa").limit(4).build(), {"TableName": "Aaa", "Limit": 4})

    def test_select_all_attributes(self):
        self.assertEqual(Query("Aaa").select_all_attributes().build(), {"TableName": "Aaa", "Select": "ALL_ATTRIBUTES"})

    def test_select_all_projected_attributes(self):
        self.assertEqual(Query("Aaa").select_all_projected_attributes().build(), {"TableName": "Aaa", "Select": "ALL_PROJECTED_ATTRIBUTES"})

    def test_select_count(self):
        self.assertEqual(Query("Aaa").select_count().build(), {"TableName": "Aaa", "Select": "COUNT"})

    def test_expression_attribute_name(self):
        self.assertEqual(Query("Aaa").expression_attribute_name("n", "p").build(), {"TableName": "Aaa", "ExpressionAttributeNames": {"#n": "p"}})

    def test_expression_attribute_value(self):
        self.assertEqual(Query("Aaa").expression_attribute_value("n", u"p").build(), {"TableName": "Aaa", "ExpressionAttributeValues": {":n": {"S": "p"}}})

    def test_project(self):
        self.assertEqual(Query("Aaa").project("a").build(), {"TableName": "Aaa", "ProjectionExpression": "a"})

    def test_return_consumed_capacity_total(self):
        self.assertEqual(Query("Aaa").return_consumed_capacity_total().build(), {"TableName": "Aaa", "ReturnConsumedCapacity": "TOTAL"})

    def test_return_consumed_capacity_indexes(self):
        self.assertEqual(Query("Aaa").return_consumed_capacity_indexes().build(), {"TableName": "Aaa", "ReturnConsumedCapacity": "INDEXES"})

    def test_return_consumed_capacity_none(self):
        self.assertEqual(Query("Aaa").return_consumed_capacity_none().build(), {"TableName": "Aaa", "ReturnConsumedCapacity": "NONE"})

    def test_filter_expression(self):
        self.assertEqual(Query("Aaa").filter_expression("a=b").build(), {"TableName": "Aaa", "FilterExpression": "a=b"})

    def test_consistent_read_true(self):
        self.assertEqual(Query("Aaa").consistent_read_true().build(), {"TableName": "Aaa", "ConsistentRead": True})

    def test_consistent_read_false(self):
        self.assertEqual(Query("Aaa").consistent_read_false().build(), {"TableName": "Aaa", "ConsistentRead": False})

    def test_index_name(self):
        self.assertEqual(Query("Aaa").index_name("foo").build(), {"TableName": "Aaa", "IndexName": "foo"})

    def test_scan_index_forward_true(self):
        self.assertEqual(Query("Aaa").scan_index_forward_true().build(), {"TableName": "Aaa", "ScanIndexForward": True})

    def test_scan_index_forward_false(self):
        self.assertEqual(Query("Aaa").scan_index_forward_false().build(), {"TableName": "Aaa", "ScanIndexForward": False})


class QueryLocalIntegTests(_tst.LocalIntegTestsWithTableHR):
    def setUp(self):
        super(QueryLocalIntegTests, self).setUp()
        self.connection(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"0", "r": 41, "v": 0},
            {"h": u"0", "r": 42, "v": 1},
            {"h": u"0", "r": 43, "v": 2},
            {"h": u"0", "r": 44, "v": 3},
            {"h": u"0", "r": 45, "v": 4},
            {"h": u"1", "r": 42, "v": 2},
            {"h": u"2", "r": 42, "v": 3},
        ))

    def test_simple_query(self):
        r = self.connection(
            _lv.Query("Aaa").key_eq("h", u"1")
        )

        self.assertEqual(r.count, 1)
        self.assertEqual(r.items[0], {"h": "1", "r": 42, "v": 2})
        self.assertEqual(r.last_evaluated_key, None)
        self.assertEqual(r.scanned_count, 1)

    def test_complex_query(self):
        r = self.connection(
            _lv.Query("Aaa").key_eq("h", u"0").key_between("r", 42, 44)
                .scan_index_forward_false()
                .project("r", "v")
                .filter_expression("#p<>:v")
                .expression_attribute_name("p", "v")
                .expression_attribute_value("v", 2)
                .limit(2)
        )

        self.assertEqual(r.count, 1)
        self.assertEqual(r.items[0], {"r": 44, "v": 3})
        self.assertEqual(r.last_evaluated_key, {"h": u"0", "r": 43})
        self.assertEqual(r.scanned_count, 2)


class QueryConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(QueryConnectedIntegTests, self).setUp()
        self.connection(_lv.PutItem(self.table, self.item))

    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(QueryConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_total(self):
        r = self.connection(_lv.Query(self.table).key_eq("tab_h", u"0").return_consumed_capacity_total())

        self.assertEqual(r.consumed_capacity.capacity_units, 0.5)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.table, None)
        self.assertEqual(r.consumed_capacity.table_name, self.table)
