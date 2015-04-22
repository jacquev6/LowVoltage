# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .iterator import Iterator


class QueryIterator(Iterator):
    """Make as many "Query" actions as needed to iterate over all matching items."""

    def __init__(self, connection, query):
        Iterator.__init__(self, connection, query)

    def process(self, action, r):
        if r.last_evaluated_key is None:
            action = None
        else:
            action.exclusive_start_key(r.last_evaluated_key)
        return action, r.items


class QueryIteratorLocalIntegTests(_tst.LocalIntegTestsWithTableHR):
    keys = range(15)

    def setUp(self):
        super(QueryIteratorLocalIntegTests, self).setUp()
        self.connection(
            _lv.BatchWriteItem().table("Aaa").put(
                {"h": u"0", "r": r, "xs": "x" * 300000}  # 300kB items ensure a single Query will return at most 4 items
                for r in self.keys
            )
        )

    def test_simple_query(self):
        self.assertEqual(
            sorted(item["r"] for item in _lv.QueryIterator(self.connection, _lv.Query("Aaa").key_eq("h", u"0"))),
            self.keys
        )
