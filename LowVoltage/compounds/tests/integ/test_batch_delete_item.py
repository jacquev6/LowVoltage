# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class BatchDeleteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def key(self, i):
        return u"{:03}".format(i)

    def setUp(self):
        super(BatchDeleteItemLocalIntegTests, self).setUp()
        _lv.batch_put_item(self.connection, "Aaa", [{"h": self.key(i)} for i in range(100)])

    def test(self):
        _lv.batch_delete_item(self.connection, "Aaa", [{"h": self.key(i)} for i in range(100)])
        self.assertEqual([], list(_lv.iterate_scan(self.connection, _lv.Scan("Aaa"))))
