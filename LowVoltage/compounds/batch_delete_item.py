# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


def BatchDeleteItem(connection, table, *keys):
    """Make as many "BatchWriteItem" actions as needed to delete all specified keys. Including UnprocessedItems."""

    delete = []
    for key in keys:
        if isinstance(key, dict):
            key = [key]
        delete.extend(key)

    unprocessed_items = []

    while len(delete) != 0:
        r = connection(_lv.BatchWriteItem().table(table).delete(delete[:25]))
        delete = delete[25:]
        if isinstance(r.unprocessed_items, dict) and table in r.unprocessed_items:
            unprocessed_items += r.unprocessed_items[table]

    # @todo Maybe wait a bit before retrying unprocessed items? Same in BatchPutItem and BatchGetItemIterator.
    # @todo In the first loop, maybe wait a bit before next request if we get unprocessed items? Might not be a good idea.

    while len(unprocessed_items) != 0:
        r = connection(_lv.BatchWriteItem({table: unprocessed_items[:25]}))
        unprocessed_items = unprocessed_items[25:]
        if isinstance(r.unprocessed_items, dict) and table in r.unprocessed_items:
            unprocessed_items += r.unprocessed_items[table]


class BatchDeleteItemUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(BatchDeleteItemUnitTests, self).setUp()
        self.connection = self.mocks.create("connection")

    def test_no_keys(self):
        BatchDeleteItem(self.connection.object, "Aaa", [])

    def test_one_page(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"S": "a"}}}}, {"DeleteRequest": {"Key": {"h": {"S": "b"}}}}]}})
        ).andReturn(
            _lv.BatchWriteItemResponse()
        )

        BatchDeleteItem(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})

    def test_several_pages(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(0, 25)]}})
        ).andReturn(
            _lv.BatchWriteItemResponse()
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(25, 50)]}})
        ).andReturn(
            _lv.BatchWriteItemResponse()
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(50, 60)]}})
        ).andReturn(
            _lv.BatchWriteItemResponse()
        )

        BatchDeleteItem(self.connection.object, "Aaa", ({"h": i} for i in range(60)))

    def test_one_unprocessed_item(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"S": "a"}}}}, {"DeleteRequest": {"Key": {"h": {"S": "b"}}}}]}})
        ).andReturn(
            _lv.BatchWriteItemResponse(UnprocessedItems={"Aaa": [{"DeleteRequest": {"Key": {"h": {"S": "c"}}}}]})
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"S": "c"}}}}]}})
        ).andReturn(
            _lv.BatchWriteItemResponse()
        )

        BatchDeleteItem(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})

    def test_several_pages_of_unprocessed_item(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(0, 25)]}})
        ).andReturn(
            _lv.BatchWriteItemResponse(UnprocessedItems={"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(100, 110)]})
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(25, 35)]}})
        ).andReturn(
            _lv.BatchWriteItemResponse(UnprocessedItems={"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(110, 120)]})
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(100, 120)]}})
        ).andReturn(
            _lv.BatchWriteItemResponse(UnprocessedItems={"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(120, 130)]})
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"N": str(i)}}}} for i in range(120, 130)]}})
        ).andReturn(
            _lv.BatchWriteItemResponse()
        )

        BatchDeleteItem(self.connection.object, "Aaa", [{"h": i} for i in range(35)])


class BatchDeleteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def key(self, i):
        return u"{:03}".format(i)

    def setUp(self):
        super(BatchDeleteItemLocalIntegTests, self).setUp()
        _lv.BatchPutItem(self.connection, "Aaa", [{"h": self.key(i)} for i in range(100)])

    def test(self):
        _lv.BatchDeleteItem(self.connection, "Aaa", [{"h": self.key(i)} for i in range(100)])
        self.assertEqual([], list(_lv.ScanIterator(self.connection, _lv.Scan("Aaa"))))
