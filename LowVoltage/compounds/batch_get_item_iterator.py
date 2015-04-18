# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .iterator import Iterator


class BatchGetItemIterator(Iterator):
    """Make as many "BatchGetItem" actions as needed to iterate over all specified items

    Warning: items are returned by DynamoDB in an unspecified order and LowVoltage does not try to change that.
    """

    def __init__(self, connection, table, keys):
        self.__table = table
        self.__keys = list(keys)
        Iterator.__init__(self, connection, self.__next_action())

    def __next_action(self):
        if len(self.__keys) > 0:
            action = _lv.BatchGetItem().table(self.__table).keys(self.__keys[:100])
            self.__keys = self.__keys[100:]
            return action
        else:
            return None

    def process(self, action, r):
        return self.__next_action(), r.responses[self.__table]


class BatchGetItemIteratorLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def key(self, i):
        return u"{:03}".format(i)

    def setUpItems(self):
        # @todo Use a LargeBatchWriteItem when implemented
        for i in range(10):
            self.connection.request(_lv.BatchWriteItem().table("Aaa").put({"h": self.key(i * 25 + j)} for j in range(25)))

    def test(self):
        keys = [item["h"] for item in _lv.BatchGetItemIterator(self.connection, "Aaa", ({"h": self.key(i)} for i in range(250)))]
        self.assertEqual(sorted(keys), [self.key(i) for i in range(250)])

    def test_no_keys(self):
        for item in _lv.BatchGetItemIterator(self.connection, "Aaa", []):
            self.fail()
