# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest
import copy

import MockMockMock

import LowVoltage as _lv
import LowVoltage.testing as _tst


class ScanIterator(object):
    """Make as many "Scan" actions as needed to iterate over the result"""

    @classmethod
    def parallelize(cls, connection, scan, threads):
        return [
            cls(connection, copy.deepcopy(scan).segment(i, threads))
            for i in range(threads)
        ]

    def __init__(self, connection, scan):
        self.__connection = connection
        self.__next_scan =  scan
        self.__current_iter = [].__iter__()
        self.__done = False

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.__current_iter.next()
        except StopIteration:
            if self.__done:
                raise
            else:
                r = self.__connection.request(self.__next_scan)
                if r.last_evaluated_key is None:
                    self.__done = True
                else:
                    self.__next_scan.exclusive_start_key(r.last_evaluated_key)
                self.__current_iter = r.items.__iter__()
                return self.__current_iter.next()


class ScanIteratorLocalIntegTests(_tst.dynamodb_local.TestCase):
    keys = range(15)

    def setUp(self):
        self.connection.request(
            _lv.CreateTable("Aaa")
                .hash_key("h", _lv.NUMBER)
                .provisioned_throughput(1, 2)
        )
        for h in self.keys:
            self.connection.request(_lv.PutItem("Aaa", {"h": h, "xs": "x" * 300000}))  # 300kB items ensure a single Scan will return at most 4 items

    def tearDown(self):
        self.connection.request(_lv.DeleteTable("Aaa"))

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
