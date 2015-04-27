# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class IterateBatchGetItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def key(self, i):
        return u"{:03}".format(i)

    def setUp(self):
        super(IterateBatchGetItemLocalIntegTests, self).setUp()
        _lv.batch_put_item(self.connection, "Aaa", [{"h": self.key(i), "xs": "x" * 300000} for i in range(250)])  # 300kB items ensure a single BatchGetItem will return at most 55 items

    def test(self):
        keys = [item["h"] for item in _lv.iterate_batch_get_item(self.connection, "Aaa", ({"h": self.key(i)} for i in range(250)))]
        self.assertEqual(sorted(keys), [self.key(i) for i in range(250)])
