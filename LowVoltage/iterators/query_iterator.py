# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import MockMockMock

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .iterator import Iterator


class QueryIterator(Iterator):
    """Make as many "Query" actions as needed to iterate over all matching items"""

    def __init__(self, connection, query):
        Iterator.__init__(self, connection, query)

    def process(self, action, r):
        done = r.last_evaluated_key is None
        action.exclusive_start_key(r.last_evaluated_key)
        items = r.items
        return done, action, items


class QueryIteratorLocalIntegTests(_tst.dynamodb_local.TestCase):
    range_keys = range(10)

    def setUp(self):
        self.connection.request(
            _lv.CreateTable("Aaa")
                .hash_key("h", _lv.NUMBER)
                .range_key("r", _lv.NUMBER)
                .provisioned_throughput(1, 2)
        )
        for r in self.range_keys:
            self.connection.request(_lv.PutItem("Aaa", {"h": 0, "r": r, "xs": "x" * 300000}))  # 300kB items ensure a single Query will return at most 4 items

    def tearDown(self):
        self.connection.request(_lv.DeleteTable("Aaa"))

    def test_simple_query(self):
        self.assertEqual(
            sorted(item["r"] for item in _lv.QueryIterator(self.connection, _lv.Query("Aaa").key_eq("h", 0))),
            self.range_keys
        )
