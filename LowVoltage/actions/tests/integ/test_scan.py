# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


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

    def test_index_name(self):
        r = self.connection(_lv.Scan(self.table).index_name("gsi"))

        self.assertEqual(len(r.items), 1)

    def test_return_consumed_capacity_total(self):
        r = self.connection(_lv.Scan(self.table).return_consumed_capacity_total())

        self.assertEqual(r.consumed_capacity.capacity_units, 0.5)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.table, None)
        self.assertEqual(r.consumed_capacity.table_name, self.table)
