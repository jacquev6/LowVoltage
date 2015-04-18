# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


def BatchPutItem(connection, table, items):
    """Make as many "BatchWriteItem" actions as needed to put all specified items"""

    while len(items) != 0:
        connection.request(_lv.BatchWriteItem().table(table).put(items[:25]))
        items = items[25:]


class BatchPutItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def key(self, i):
        return u"{:03}".format(i)

    def test(self):
        _lv.BatchPutItem(self.connection, "Aaa", [{"h": self.key(i)} for i in range(100)])
        self.assertEqual(len(list(_lv.ScanIterator(self.connection, _lv.Scan("Aaa")))), 100)
