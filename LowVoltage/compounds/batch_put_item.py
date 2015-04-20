# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import MockMockMock

import LowVoltage as _lv
import LowVoltage.testing as _tst


def BatchPutItem(connection, table, *items):
    """Make as many "BatchWriteItem" actions as needed to put all specified items. Including UnprocessedItems."""

    put = []
    for item in items:
        if isinstance(item, dict):
            item = [item]
        put.extend(item)

    unprocessed_items = []

    while len(put) != 0:
        r = connection.request(_lv.BatchWriteItem().table(table).put(put[:25]))
        put = put[25:]
        if isinstance(r.unprocessed_items, dict) and table in r.unprocessed_items:
            unprocessed_items += r.unprocessed_items[table]

    while len(unprocessed_items) != 0:
        r = connection.request(_lv.BatchWriteItem({table: unprocessed_items[:25]}))
        unprocessed_items = unprocessed_items[25:]
        if isinstance(r.unprocessed_items, dict) and table in r.unprocessed_items:
            unprocessed_items += r.unprocessed_items[table]


class BatchPutItemUnitTests(unittest.TestCase):
    def setUp(self):
        self.mocks = MockMockMock.Engine()
        self.connection = self.mocks.create("connection")
        super(BatchPutItemUnitTests, self).setUp()

    def tearDown(self):
        self.mocks.tearDown()
        super(BatchPutItemUnitTests, self).tearDown()

    class Checker(object):
        def __init__(self, expected_payload):
            self.__expected_payload = expected_payload

        def __call__(self, args, kwds):
            assert len(args) == 1
            assert len(kwds) == 0
            action, = args
            return action.name == "BatchWriteItem" and action.build() == self.__expected_payload

    def test_no_keys(self):
        BatchPutItem(self.connection.object, "Aaa", [])

    def test_one_page(self):
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"S": "a"}}}}, {"PutRequest": {"Item": {"h": {"S": "b"}}}}]}})
        ).andReturn(
            _lv.BatchWriteItem.Result()
        )

        BatchPutItem(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})

    def test_several_pages(self):
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(0, 25)]}})
        ).andReturn(
            _lv.BatchWriteItem.Result()
        )
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(25, 50)]}})
        ).andReturn(
            _lv.BatchWriteItem.Result()
        )
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(50, 60)]}})
        ).andReturn(
            _lv.BatchWriteItem.Result()
        )

        BatchPutItem(self.connection.object, "Aaa", ({"h": i} for i in range(60)))

    def test_one_unprocessed_item(self):
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"S": "a"}}}}, {"PutRequest": {"Item": {"h": {"S": "b"}}}}]}})
        ).andReturn(
            _lv.BatchWriteItem.Result(UnprocessedItems={"Aaa": [{"PutRequest": {"Item": {"h": {"S": "c"}}}}]})
        )
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"S": "c"}}}}]}})
        ).andReturn(
            _lv.BatchWriteItem.Result()
        )

        BatchPutItem(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})

    def test_several_pages_of_unprocessed_item(self):
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(0, 25)]}})
        ).andReturn(
            _lv.BatchWriteItem.Result(UnprocessedItems={"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(100, 110)]})
        )
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(25, 35)]}})
        ).andReturn(
            _lv.BatchWriteItem.Result(UnprocessedItems={"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(110, 120)]})
        )
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(100, 120)]}})
        ).andReturn(
            _lv.BatchWriteItem.Result(UnprocessedItems={"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(120, 130)]})
        )
        self.connection.expect.request.withArguments(
            self.Checker({"RequestItems": {"Aaa": [{"PutRequest": {"Item": {"h": {"N": str(i)}}}} for i in range(120, 130)]}})
        ).andReturn(
            _lv.BatchWriteItem.Result()
        )

        BatchPutItem(self.connection.object, "Aaa", [{"h": i} for i in range(35)])


class BatchPutItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def key(self, i):
        return u"{:03}".format(i)

    def test(self):
        _lv.BatchPutItem(self.connection, "Aaa", [{"h": self.key(i)} for i in range(100)])
        self.assertEqual(len(list(_lv.ScanIterator(self.connection, _lv.Scan("Aaa")))), 100)
