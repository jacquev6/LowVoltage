# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import copy

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .iterator import Iterator


class ScanIterator(Iterator):
    """Make as many "Scan" actions as needed to iterate over all items."""

    @classmethod
    def parallelize(cls, connection, scan, threads):
        """Create several ScanIterator to be used in parallel"""

        return [
            cls(connection, copy.deepcopy(scan).segment(i, threads))
            for i in range(threads)
        ]

    def __init__(self, connection, scan):
        Iterator.__init__(self, connection, scan)

    def process(self, action, r):
        if r.last_evaluated_key is None:
            action = None
        else:
            action.exclusive_start_key(r.last_evaluated_key)
        return action, r.items


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
            sorted(item["h"] for item in _lv.ScanIterator(self.connection, _lv.Scan("Aaa"))),
            self.keys
        )

    def test_parallel_scan(self):
        keys = []
        for segment in ScanIterator.parallelize(self.connection, _lv.Scan("Aaa"), 3):
            keys.extend(item["h"] for item in segment)
        self.assertEqual(sorted(keys), self.keys)
