# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`Scan`, the connection will return a :class:`ScanResponse`:

>>> connection(Scan(table))
<LowVoltage.actions.scan.ScanResponse ...>

Items are accessed like this:

>>> sorted(connection(Scan(table)).items)
[{u'h': 0, u'gr': 0, u'gh': 0}, {u'h': 1, u'gr': 0, u'gh': 0}, {u'h': 2, u'gr': 0, u'gh': 0}, {u'h': 3, u'gr': 0, u'gh': 0}, {u'h': 4, u'gr': 0, u'gh': 0}, {u'h': 5, u'gr': 0, u'gh': 0}, {u'h': 6, u'gr': 0, u'gh': 0}, {u'h': 7, u'gr': 0, u'gh': 0}, {u'h': 8, u'gr': 0, u'gh': 0}, {u'h': 9, u'gr': 0, u'gh': 0}]

Note that items are returned in an undefined order. Here we sort them to make the doctest stable.
"""

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_db_to_dict
from .next_gen_mixins import proxy
from .next_gen_mixins import (
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


class ScanResponse(object):
    """
    The `Scan response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_ResponseElements>`__.
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
        The capacity consumed by the request. If you used :meth:`~.Scan.return_consumed_capacity_total`.

        :type: None or :class:`.ConsumedCapacity`
        """
        if _is_dict(self.__consumed_capacity):  # pragma no branch (Defensive code)
            return ConsumedCapacity(**self.__consumed_capacity)

    @property
    def count(self):
        """
        The number of items matching the scan.

        :type: None or long
        """
        if _is_int(self.__count):  # pragma no branch (Defensive code)
            return long(self.__count)

    @property
    def items(self):
        """
        The items matching the scan. Unless you used :meth:`.Scan.select_count`.

        :type: None or list of dict
        """
        if _is_list_of_dict(self.__items):  # pragma no branch (Defensive code)
            return [_convert_db_to_dict(i) for i in self.__items]

    @property
    def last_evaluated_key(self):
        """
        The key of the last item evaluated by the scan. If not None, it should be given to :meth:`~.Scan.exclusive_start_key` is a subsequent :class:`.Scan`.

        :type: None or dict
        """
        if _is_dict(self.__last_evaluated_key):  # pragma no branch (Defensive code)
            return _convert_db_to_dict(self.__last_evaluated_key)

    @property
    def scanned_count(self):
        """
        The number of item scanned during the scan. This can be different from :attr:`~.ScanResponse.count` when using :meth:`~.Scan.filter_expression`.

        :type: None or long
        """
        if _is_int(self.__scanned_count):  # pragma no branch (Defensive code)
            return long(self.__scanned_count)


class Scan(Action):
    """
    The `Scan request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_RequestParameters>`__.
    """

    def __init__(self, table_name):
        super(Scan, self).__init__("Scan")
        self.__table_name = table_name
        self.__exclusive_start_key = ExclusiveStartKey(self)
        self.__expression_attribute_names = ExpressionAttributeNames(self)
        self.__expression_attribute_values = ExpressionAttributeValues(self)
        self.__filter_expression = FilterExpression(self)
        self.__limit = Limit(self)
        self.__projection_expression = ProjectionExpression(self)
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__segment = ScalarValue("Segment")(self)
        self.__select = Select(self)
        self.__total_segments = ScalarValue("TotalSegments")(self)

    def build(self):
        data = {"TableName": self.__table_name}
        data.update(self.__exclusive_start_key.build())
        data.update(self.__expression_attribute_names.build())
        data.update(self.__expression_attribute_values.build())
        data.update(self.__filter_expression.build())
        data.update(self.__limit.build())
        data.update(self.__projection_expression.build())
        data.update(self.__return_consumed_capacity.build())
        data.update(self.__segment.build())
        data.update(self.__select.build())
        data.update(self.__total_segments.build())
        return data

    @staticmethod
    def Result(**kwds):
        return ScanResponse(**kwds)

    def segment(self, segment, total_segments):
        """
        Set Segment and TotalSegments for a parallel scan. @todo See also ScanIterator.parallelize

        @todo doctest
        """
        self.__segment.set(segment)
        return self.__total_segments.set(total_segments)

    @proxy("Scan")
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
        >>> r = connection(Scan(table).select_count())
        >>> r.count
        10L
        >>> print r.items
        None
        """
        return self.__select.count()

    @proxy
    def select_all_attributes(self):
        """
        @todo doctest
        """
        return self.__select.all_attributes()

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
    def return_consumed_capacity_total(self):
        """
        >>> connection(
        ...   Scan(table)
        ...     .limit(4)
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        0.5
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_none(self):
        """
        >>> print connection(
        ...   Scan(table)
        ...     .limit(4)
        ...     .return_consumed_capacity_none()
        ... ).consumed_capacity
        None
        """
        return self.__return_consumed_capacity.none()


class ScanUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(Scan("Aaa").name, "Scan")

    def test_table_name(self):
        self.assertEqual(Scan("Aaa").build(), {"TableName": "Aaa"})

    def test_segment(self):
        self.assertEqual(Scan("Aaa").segment(0, 2).build(), {"TableName": "Aaa", "Segment": 0, "TotalSegments": 2})

    def test_exclusive_start_key(self):
        self.assertEqual(Scan("Aaa").exclusive_start_key({"h": u"v"}).build(), {"TableName": "Aaa", "ExclusiveStartKey": {"h": {"S": "v"}}})

    def test_limit(self):
        self.assertEqual(Scan("Aaa").limit(4).build(), {"TableName": "Aaa", "Limit": 4})

    def test_select_all_attributes(self):
        self.assertEqual(Scan("Aaa").select_all_attributes().build(), {"TableName": "Aaa", "Select": "ALL_ATTRIBUTES"})

    def test_select_count(self):
        self.assertEqual(Scan("Aaa").select_count().build(), {"TableName": "Aaa", "Select": "COUNT"})

    def test_expression_attribute_name(self):
        self.assertEqual(Scan("Aaa").expression_attribute_name("n", "p").build(), {"TableName": "Aaa", "ExpressionAttributeNames": {"#n": "p"}})

    def test_expression_attribute_value(self):
        self.assertEqual(Scan("Aaa").expression_attribute_value("n", u"p").build(), {"TableName": "Aaa", "ExpressionAttributeValues": {":n": {"S": "p"}}})

    def test_project(self):
        self.assertEqual(Scan("Aaa").project("a").build(), {"TableName": "Aaa", "ProjectionExpression": "a"})

    def test_return_consumed_capacity_total(self):
        self.assertEqual(Scan("Aaa").return_consumed_capacity_total().build(), {"TableName": "Aaa", "ReturnConsumedCapacity": "TOTAL"})

    def test_return_consumed_capacity_none(self):
        self.assertEqual(Scan("Aaa").return_consumed_capacity_none().build(), {"TableName": "Aaa", "ReturnConsumedCapacity": "NONE"})

    def test_filter_expression(self):
        self.assertEqual(Scan("Aaa").filter_expression("a=b").build(), {"TableName": "Aaa", "FilterExpression": "a=b"})


class ScanLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def setUp(self):
        super(ScanLocalIntegTests, self).setUp()
        self.connection(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"0", "v": 0},
            {"h": u"1", "v": 1},
            {"h": u"2", "v": 2},
            {"h": u"3", "v": 3},
        ))

    def test_simple_scan(self):
        r = self.connection(
            _lv.Scan("Aaa")
        )


        self.assertEqual(r.count, 4)
        items = sorted((r.items[i] for i in range(4)), key=lambda i: i["h"])
        self.assertEqual(items, [{"h": u"0", "v": 0}, {"h": u"1", "v": 1}, {"h": u"2", "v": 2}, {"h": u"3", "v": 3}])
        self.assertEqual(r.last_evaluated_key, None)
        self.assertEqual(r.scanned_count, 4)

    def test_paginated_segmented_scan(self):
        # If this test fails randomly, change it to assert on the sum and union of the results
        r01 = self.connection(
            _lv.Scan("Aaa").segment(0, 2).limit(1)
        )
        r02 = self.connection(
            _lv.Scan("Aaa").segment(0, 2).exclusive_start_key({"h": u"1"})
        )
        r11 = self.connection(
            _lv.Scan("Aaa").segment(1, 2).limit(1)
        )
        r12 = self.connection(
            _lv.Scan("Aaa").segment(1, 2).exclusive_start_key({"h": u"0"})
        )

        self.assertEqual(r01.count, 1)
        self.assertEqual(r01.items[0], {"h": u"1", "v": 1})
        self.assertEqual(r01.last_evaluated_key, {"h": u"1"})
        self.assertEqual(r01.scanned_count, 1)

        self.assertEqual(r02.count, 1)
        self.assertEqual(r02.items[0], {"h": u"3", "v": 3})
        self.assertEqual(r02.last_evaluated_key, None)
        self.assertEqual(r02.scanned_count, 1)

        self.assertEqual(r11.count, 1)
        self.assertEqual(r11.items[0], {"h": u"0", "v": 0})
        self.assertEqual(r11.last_evaluated_key, {"h": u"0"})
        self.assertEqual(r11.scanned_count, 1)

        self.assertEqual(r12.count, 1)
        self.assertEqual(r12.items[0], {"h": u"2", "v": 2})
        self.assertEqual(r12.last_evaluated_key, None)
        self.assertEqual(r12.scanned_count, 1)

    def test_filtered_scan(self):
        r = self.connection(
            _lv.Scan("Aaa").filter_expression("v>:v").expression_attribute_value("v", 1).project("h")
        )


        self.assertEqual(r.count, 2)
        self.assertEqual(r.items[0], {"h": u"3"})
        self.assertEqual(r.items[1], {"h": u"2"})
        self.assertEqual(r.last_evaluated_key, None)
        self.assertEqual(r.scanned_count, 4)


class ScanConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(ScanConnectedIntegTests, self).setUp()
        self.connection(_lv.PutItem(self.table, self.item))

    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(ScanConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_total(self):
        r = self.connection(_lv.Scan(self.table).return_consumed_capacity_total())

        self.assertEqual(r.consumed_capacity.capacity_units, 0.5)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.table, None)
        self.assertEqual(r.consumed_capacity.table_name, self.table)
