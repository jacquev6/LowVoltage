# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


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
