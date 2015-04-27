# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class ScanIteratorLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    keys = [u"{:03}".format(k) for k in range(15)]

    def setUp(self):
        super(ScanIteratorLocalIntegTests, self).setUp()
        self.connection(
            _lv.BatchWriteItem().table("Aaa").put(
                {"h": h, "xs": "x" * 300000}  # 300kB items ensure a single Query will return at most 4 items
                for h in self.keys
            )
        )

    def test_simple_scan(self):
        self.assertEqual(
            sorted(item["h"] for item in _lv.iterate_scan(self.connection, _lv.Scan("Aaa"))),
            self.keys
        )

    def test_parallel_scan(self):
        keys = []
        for segment in _lv.parallelize_scan(_lv.Scan("Aaa"), 3):
            keys.extend(item["h"] for item in _lv.iterate_scan(self.connection, segment))
        self.assertEqual(sorted(keys), self.keys)
