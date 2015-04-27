# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


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
            sorted(item["r"] for item in _lv.iterate_query(self.connection, _lv.Query("Aaa").key_eq("h", u"0"))),
            self.keys
        )
