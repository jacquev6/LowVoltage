# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from LowVoltage.variadic import variadic


@variadic(dict)
def batch_delete_item(connection, table, keys):
    """
    Make as many :class:`.BatchWriteItem` actions as needed to delete all specified keys.
    Including processing :attr:`.BatchWriteItemResponse.unprocessed_items`.

    >>> batch_delete_item(
    ...   connection,
    ...   table,
    ...   {"h": 0},
    ...   {"h": 1},
    ...   {"h": 2}
    ... )
    """

    unprocessed_items = []

    while len(keys) != 0:
        r = connection(_lv.BatchWriteItem().table(table).delete(keys[:25]))
        keys = keys[25:]
        if isinstance(r.unprocessed_items, dict) and table in r.unprocessed_items:
            unprocessed_items.extend(r.unprocessed_items[table])

    while len(unprocessed_items) != 0:
        r = connection(_lv.BatchWriteItem().previous_unprocessed_items({table: unprocessed_items[:25]}))
        unprocessed_items = unprocessed_items[25:]
        if isinstance(r.unprocessed_items, dict) and table in r.unprocessed_items:
            unprocessed_items.extend(r.unprocessed_items[table])


class BatchDeleteItemUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(BatchDeleteItemUnitTests, self).setUp()
        self.connection = self.mocks.create("connection")

    def test_no_keys(self):
        batch_delete_item(self.connection.object, "Aaa", [])

    def test_one_page(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchWriteItem", {"RequestItems": {"Aaa": [{"DeleteRequest": {"Key": {"h": {"S": "a"}}}}, {"DeleteRequest": {"Key": {"h": {"S": "b"}}}}]}})
        ).andReturn(
            _lv.BatchWriteItemResponse()
        )

        batch_delete_item(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})

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

        batch_delete_item(self.connection.object, "Aaa", ({"h": i} for i in range(60)))

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

        batch_delete_item(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})

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

        batch_delete_item(self.connection.object, "Aaa", [{"h": i} for i in range(35)])
