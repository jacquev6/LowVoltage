# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


def BatchDeleteItem(connection, table, keys):
    """Make as many "BatchWriteItem" actions as needed to delete all specified keys"""

    while len(keys) != 0:
        connection.request(_lv.BatchWriteItem().table(table).delete(keys[:25]))
        keys = keys[25:]


class BatchDeleteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def key(self, i):
        return u"{:03}".format(i)

    def setUpItems(self):
        # @todo Use a LargeBatchWriteItem when implemented
        for i in range(4):
            self.connection.request(_lv.BatchWriteItem().table("Aaa").put({"h": self.key(i * 25 + j)} for j in range(25)))

    def test(self):
        _lv.BatchDeleteItem(self.connection, "Aaa", [{"h": self.key(i)} for i in range(100)])
        self.assertEqual([], list(_lv.ScanIterator(self.connection, _lv.Scan("Aaa"))))
