# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict
from .expression_mixins import ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ProjectionExpressionMixin, FilterExpressionMixin
from .return_mixins import ReturnConsumedCapacityMixin
from .return_types import ConsumedCapacity_, _is_dict, _is_int, _is_list_of_dict


class Query(
    Action,
    ExpressionAttributeNamesMixin,
    ExpressionAttributeValuesMixin,
    ProjectionExpressionMixin,
    FilterExpressionMixin,
    ReturnConsumedCapacityMixin,
):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestParameters"""

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_ResponseElements"""

        def __init__(
            self,
            ConsumedCapacity=None,
            Count=None,
            Items=None,
            LastEvaluatedKey=None,
            ScannedCount=None,
            **dummy
        ):
            self.consumed_capacity = None
            if _is_dict(ConsumedCapacity):  # pragma no branch (Defensive code)
                self.consumed_capacity = ConsumedCapacity_(**ConsumedCapacity)

            self.count = None
            if _is_int(Count):  # pragma no branch (Defensive code)
                self.count = long(Count)

            self.items = None
            if _is_list_of_dict(Items):  # pragma no branch (Defensive code)
                self.items = [_convert_db_to_dict(i) for i in Items]

            self.last_evaluated_key = None
            if _is_dict(LastEvaluatedKey):  # pragma no branch (Defensive code)
                self.last_evaluated_key = _convert_db_to_dict(LastEvaluatedKey)

            self.scanned_count = None
            if _is_int(ScannedCount):  # pragma no branch (Defensive code)
                self.scanned_count = long(ScannedCount)

    def __init__(self, table_name):
        super(Query, self).__init__("Query")
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ProjectionExpressionMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        FilterExpressionMixin.__init__(self)
        self.__table_name = table_name
        self.__conditions = {}
        self.__exclusive_start_key = None
        self.__limit = None
        self.__select = None
        self.__consistent_read = None
        self.__index_name = None
        self.__scan_index_forward = None

    def build(self):
        data = {"TableName": self.__table_name}
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_projection_expression())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_filter_expression())
        if self.__conditions:
            data["KeyConditions"] = self.__conditions
        if self.__exclusive_start_key:
            data["ExclusiveStartKey"] = _convert_dict_to_db(self.__exclusive_start_key)
        if self.__limit:
            data["Limit"] = self.__limit
        if self.__select:
            data["Select"] = self.__select
        if self.__consistent_read is not None:
            data["ConsistentRead"] = self.__consistent_read
        if self.__index_name:
            data["IndexName"] = self.__index_name
        if self.__scan_index_forward is not None:
            data["ScanIndexForward"] = self.__scan_index_forward
        return data

    def key_eq(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "EQ", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_le(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "LE", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_lt(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "LT", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_ge(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "GE", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_gt(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "GT", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_begins_with(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_between(self, name, lo, hi):
        self.__conditions[name] = {"ComparisonOperator": "BETWEEN", "AttributeValueList": [_convert_value_to_db(lo), _convert_value_to_db(hi)]}
        return self

    def exclusive_start_key(self, key):
        self.__exclusive_start_key = key
        return self

    def limit(self, limit):
        self.__limit = limit
        return self

    def select_count(self):
        self.__select = "COUNT"
        return self

    def select_all_attributes(self):
        self.__select = "ALL_ATTRIBUTES"
        return self

    def select_all_projected_attributes(self):
        self.__select = "ALL_PROJECTED_ATTRIBUTES"
        return self

    def select_specific_attributes(self):
        self.__select = "SPECIFIC_ATTRIBUTES"
        return self

    def consistent_read_true(self):
        self.__consistent_read = True
        return self

    def consistent_read_false(self):
        self.__consistent_read = False
        return self

    def index_name(self, name):
        self.__index_name = name
        return self

    def scan_index_forward_true(self):
        self.__scan_index_forward = True
        return self

    def scan_index_forward_false(self):
        self.__scan_index_forward = False
        return self


class QueryUnitTests(_tst.UnitTests):
    def testName(self):
        self.assertEqual(Query("Aaa").name, "Query")

    def testTableName(self):
        self.assertEqual(Query("Aaa").build(), {"TableName": "Aaa"})

    def testKeyEq(self):
        self.assertEqual(
            Query("Aaa").key_eq("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyLe(self):
        self.assertEqual(
            Query("Aaa").key_le("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyLt(self):
        self.assertEqual(
            Query("Aaa").key_lt("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyGe(self):
        self.assertEqual(
            Query("Aaa").key_ge("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyGt(self):
        self.assertEqual(
            Query("Aaa").key_gt("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "GT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyBeginsWith(self):
        self.assertEqual(
            Query("Aaa").key_begins_with("name", u"prefix").build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [{"S": "prefix"}]}},
            }
        )

    def testKeyBetween(self):
        self.assertEqual(
            Query("Aaa").key_between("name", 42, 44).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "42"}, {"N": "44"}]}},
            }
        )

    def testExclusiveStartKey(self):
        self.assertEqual(Query("Aaa").exclusive_start_key({"h": u"v"}).build(), {"TableName": "Aaa", "ExclusiveStartKey": {"h": {"S": "v"}}})

    def testLimit(self):
        self.assertEqual(Query("Aaa").limit(4).build(), {"TableName": "Aaa", "Limit": 4})

    def testSelect(self):
        self.assertEqual(Query("Aaa").select_all_attributes().build(), {"TableName": "Aaa", "Select": "ALL_ATTRIBUTES"})
        self.assertEqual(Query("Aaa").select_all_projected_attributes().build(), {"TableName": "Aaa", "Select": "ALL_PROJECTED_ATTRIBUTES"})
        self.assertEqual(Query("Aaa").select_count().build(), {"TableName": "Aaa", "Select": "COUNT"})
        self.assertEqual(Query("Aaa").select_specific_attributes().build(), {"TableName": "Aaa", "Select": "SPECIFIC_ATTRIBUTES"})

    def testExpressionAttributeName(self):
        self.assertEqual(Query("Aaa").expression_attribute_name("n", "p").build(), {"TableName": "Aaa", "ExpressionAttributeNames": {"#n": "p"}})

    def testExpressionAttributeValue(self):
        self.assertEqual(Query("Aaa").expression_attribute_value("n", u"p").build(), {"TableName": "Aaa", "ExpressionAttributeValues": {":n": {"S": "p"}}})

    def testProject(self):
        self.assertEqual(Query("Aaa").project("a").build(), {"TableName": "Aaa", "ProjectionExpression": "a"})

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(Query("Aaa").return_consumed_capacity_none().build(), {"TableName": "Aaa", "ReturnConsumedCapacity": "NONE"})

    def testFilterExpression(self):
        self.assertEqual(Query("Aaa").filter_expression("a=b").build(), {"TableName": "Aaa", "FilterExpression": "a=b"})

    def testConsistentRead(self):
        self.assertEqual(Query("Aaa").consistent_read_true().build(), {"TableName": "Aaa", "ConsistentRead": True})
        self.assertEqual(Query("Aaa").consistent_read_false().build(), {"TableName": "Aaa", "ConsistentRead": False})

    def testIndexName(self):
        self.assertEqual(Query("Aaa").index_name("foo").build(), {"TableName": "Aaa", "IndexName": "foo"})

    def testScanIndexForward(self):
        self.assertEqual(Query("Aaa").scan_index_forward_true().build(), {"TableName": "Aaa", "ScanIndexForward": True})
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

    def testSimpleQuery(self):
        r = self.connection(
            _lv.Query("Aaa").key_eq("h", u"1")
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": "1", "r": 42, "v": 2})
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 1)

    def testComplexQuery(self):
        r = self.connection(
            _lv.Query("Aaa").key_eq("h", u"0").key_between("r", 42, 44)
                .scan_index_forward_false()
                .project("r", "v")
                .filter_expression("#p<>:v")
                .expression_attribute_name("p", "v")
                .expression_attribute_value("v", 2)
                .limit(2)
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
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
        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity.capacity_units, 0.5)
            self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.table, None)
            self.assertEqual(r.consumed_capacity.table_name, self.table)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], self.item)
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 1)
