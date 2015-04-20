# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_db_to_dict
from .expression_mixins import ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ProjectionExpressionMixin, FilterExpressionMixin
from .return_mixins import ReturnConsumedCapacityMixin
from .return_types import ConsumedCapacity_, _is_dict, _is_int, _is_list_of_dict


class Scan(
    Action,
    ExpressionAttributeNamesMixin,
    ExpressionAttributeValuesMixin,
    ProjectionExpressionMixin,
    FilterExpressionMixin,
    ReturnConsumedCapacityMixin,
):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_RequestParameters"""

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_ResponseElements"""

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
        super(Scan, self).__init__("Scan")
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ProjectionExpressionMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        FilterExpressionMixin.__init__(self)
        self.__table_name = table_name
        self.__exclusive_start_key = None
        self.__limit = None
        self.__select = None
        self.__segment = None
        self.__total_segments = None

    def build(self):
        data = {"TableName": self.__table_name}
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_projection_expression())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_filter_expression())
        if self.__segment is not None:
            data["Segment"] = self.__segment
        if self.__total_segments:
            data["TotalSegments"] = self.__total_segments
        if self.__exclusive_start_key:
            data["ExclusiveStartKey"] = _convert_dict_to_db(self.__exclusive_start_key)
        if self.__limit:
            data["Limit"] = self.__limit
        if self.__select:
            data["Select"] = self.__select
        return data

    def segment(self, segment, total_segments):
        self.__segment = segment
        self.__total_segments = total_segments
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

    def select_specific_attributes(self):
        self.__select = "SPECIFIC_ATTRIBUTES"
        return self


class ScanUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(Scan("Aaa").name, "Scan")

    def testTableName(self):
        self.assertEqual(Scan("Aaa").build(), {"TableName": "Aaa"})

    def testSegment(self):
        self.assertEqual(Scan("Aaa").segment(0, 2).build(), {"TableName": "Aaa", "Segment": 0, "TotalSegments": 2})
        self.assertEqual(Scan("Aaa").segment(1, 2).build(), {"TableName": "Aaa", "Segment": 1, "TotalSegments": 2})

    def testExclusiveStartKey(self):
        self.assertEqual(Scan("Aaa").exclusive_start_key({"h": u"v"}).build(), {"TableName": "Aaa", "ExclusiveStartKey": {"h": {"S": "v"}}})

    def testLimit(self):
        self.assertEqual(Scan("Aaa").limit(4).build(), {"TableName": "Aaa", "Limit": 4})

    def testSelect(self):
        self.assertEqual(Scan("Aaa").select_all_attributes().build(), {"TableName": "Aaa", "Select": "ALL_ATTRIBUTES"})
        self.assertEqual(Scan("Aaa").select_count().build(), {"TableName": "Aaa", "Select": "COUNT"})
        self.assertEqual(Scan("Aaa").select_specific_attributes().build(), {"TableName": "Aaa", "Select": "SPECIFIC_ATTRIBUTES"})

    def testExpressionAttributeName(self):
        self.assertEqual(Scan("Aaa").expression_attribute_name("n", "p").build(), {"TableName": "Aaa", "ExpressionAttributeNames": {"#n": "p"}})

    def testExpressionAttributeValue(self):
        self.assertEqual(Scan("Aaa").expression_attribute_value("n", u"p").build(), {"TableName": "Aaa", "ExpressionAttributeValues": {":n": {"S": "p"}}})

    def testProject(self):
        self.assertEqual(Scan("Aaa").project("a").build(), {"TableName": "Aaa", "ProjectionExpression": "a"})

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(Scan("Aaa").return_consumed_capacity_none().build(), {"TableName": "Aaa", "ReturnConsumedCapacity": "NONE"})

    def testFilterExpression(self):
        self.assertEqual(Scan("Aaa").filter_expression("a=b").build(), {"TableName": "Aaa", "FilterExpression": "a=b"})


class ScanLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def setUp(self):
        super(ScanLocalIntegTests, self).setUp()
        self.connection.request(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"0", "v": 0},
            {"h": u"1", "v": 1},
            {"h": u"2", "v": 2},
            {"h": u"3", "v": 3},
        ))

    def testSimpleScan(self):
        r = self.connection.request(
            _lv.Scan("Aaa")
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 4)
            items = sorted((r.items[i] for i in range(4)), key=lambda i: i["h"])
            self.assertEqual(items, [{"h": u"0", "v": 0}, {"h": u"1", "v": 1}, {"h": u"2", "v": 2}, {"h": u"3", "v": 3}])
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 4)

    def testPaginatedSegmentedScan(self):
        # If this test fails randomly, change it to assert on the sum and union of the results
        r01 = self.connection.request(
            _lv.Scan("Aaa").segment(0, 2).limit(1)
        )
        r02 = self.connection.request(
            _lv.Scan("Aaa").segment(0, 2).exclusive_start_key({"h": u"1"})
        )
        r11 = self.connection.request(
            _lv.Scan("Aaa").segment(1, 2).limit(1)
        )
        r12 = self.connection.request(
            _lv.Scan("Aaa").segment(1, 2).exclusive_start_key({"h": u"0"})
        )

        with _tst.cover("r01", r01) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": u"1", "v": 1})
            self.assertEqual(r.last_evaluated_key, {"h": u"1"})
            self.assertEqual(r.scanned_count, 1)

        with _tst.cover("r02", r02) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": u"3", "v": 3})
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 1)

        with _tst.cover("r11", r11) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": u"0", "v": 0})
            self.assertEqual(r.last_evaluated_key, {"h": u"0"})
            self.assertEqual(r.scanned_count, 1)

        with _tst.cover("r12", r12) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": u"2", "v": 2})
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 1)

    def testFilteredScan(self):
        r = self.connection.request(
            _lv.Scan("Aaa").filter_expression("v>:v").expression_attribute_value("v", 1).project("h")
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 2)
            self.assertEqual(r.items[0], {"h": u"3"})
            self.assertEqual(r.items[1], {"h": u"2"})
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 4)


class ScanConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(ScanConnectedIntegTests, self).setUp()
        self.connection.request(_lv.PutItem(self.table, self.item))

    def tearDown(self):
        self.connection.request(_lv.DeleteItem(self.table, self.tab_key))
        super(ScanConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_total(self):
        r = self.connection.request(_lv.Scan(self.table).return_consumed_capacity_total())
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
