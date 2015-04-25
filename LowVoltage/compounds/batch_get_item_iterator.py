# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .iterator import Iterator


class BatchGetItemIterator(Iterator):
    """Make as many "BatchGetItem" actions as needed to iterate over all specified items. Including UnprocessedKeys.

    Warning: items are returned by DynamoDB in an unspecified order and LowVoltage does not try to change that.
    """

    def __init__(self, connection, table, *keys):
        self.__table = table
        self.__keys = []
        for key in keys:
            if isinstance(key, dict):
                key = [key]
            self.__keys.extend(key)
        self.__unprocessed_keys = []
        Iterator.__init__(self, connection, self.__next_action())

    def __next_action(self):
        if len(self.__keys) > 0:
            action = _lv.BatchGetItem().table(self.__table).keys(self.__keys[:100])
            self.__keys = self.__keys[100:]
            return action
        elif len(self.__unprocessed_keys) > 0:
            action = _lv.BatchGetItem({self.__table: {"Keys": self.__unprocessed_keys[:100]}})
            self.__unprocessed_keys = self.__unprocessed_keys[100:]
            return action
        else:
            return None

    def process(self, action, r):
        if isinstance(r.unprocessed_keys, dict) and self.__table in r.unprocessed_keys and "Keys" in r.unprocessed_keys[self.__table]:
            self.__unprocessed_keys += r.unprocessed_keys[self.__table]["Keys"]
        return self.__next_action(), r.responses[self.__table]


class BatchGetItemIteratorUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(BatchGetItemIteratorUnitTests, self).setUp()
        self.connection = self.mocks.create("connection")

    def test_no_keys(self):
        self.assertEqual(
            list(_lv.BatchGetItemIterator(self.connection.object, "Aaa", [])),
            []
        )

    def test_one_page(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"S": "a"}}, {"h": {"S": "b"}}]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(Responses={"Aaa": [{"h": {"S": "c"}}, {"h": {"S": "d"}}]})
        )

        self.assertEqual(
            list(_lv.BatchGetItemIterator(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})),
            [{"h": "c"}, {"h": "d"}]
        )

    def test_one_unprocessed_key(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"S": "a"}}, {"h": {"S": "b"}}]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(Responses={"Aaa": [{"h": {"S": "c"}}]}, UnprocessedKeys={"Aaa": {"Keys": [{"h": {"S": "d"}}]}})
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"S": "d"}}]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(Responses={"Aaa": [{"h": {"S": "e"}}]})
        )

        self.assertEqual(
            list(_lv.BatchGetItemIterator(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})),
            [{"h": "c"}, {"h": "e"}]
        )

    def test_several_pages(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(0, 100)]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(Responses={"Aaa": [{"h": {"N": str(i)}} for i in range(1000, 1100)]})
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(100, 200)]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(Responses={"Aaa": [{"h": {"N": str(i)}} for i in range(1100, 1200)]})
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(200, 250)]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(Responses={"Aaa": [{"h": {"N": str(i)}} for i in range(1200, 1250)]})
        )

        self.assertEqual(
            list(_lv.BatchGetItemIterator(self.connection.object, "Aaa", ({"h": i} for i in range(0, 250)))),
            [{"h": i} for i in range(1000, 1250)]
        )

    def test_several_pages_of_unprocessed_keys(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(0, 100)]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(
                Responses={"Aaa": [{"h": {"N": str(i)}} for i in range(1000, 1100)]},
                UnprocessedKeys={"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(2000, 2075)]}}
            )
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(100, 150)]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(
                Responses={"Aaa": [{"h": {"N": str(i)}} for i in range(1100, 1150)]},
                UnprocessedKeys={"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(2075, 2150)]}}
            )
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(2000, 2100)]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(
                Responses={"Aaa": [{"h": {"N": str(i)}} for i in range(1150, 1200)]},
                UnprocessedKeys={"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(2150, 2175)]}}
            )
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"N": str(i)}} for i in range(2100, 2175)]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(
                Responses={"Aaa": [{"h": {"N": str(i)}} for i in range(1200, 1250)]},
            )
        )

        self.assertEqual(
            list(_lv.BatchGetItemIterator(self.connection.object, "Aaa", [{"h": i} for i in range(0, 150)])),
            [{"h": i} for i in range(1000, 1250)]
        )


class BatchGetItemIteratorLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def key(self, i):
        return u"{:03}".format(i)

    def setUp(self):
        super(BatchGetItemIteratorLocalIntegTests, self).setUp()
        _lv.BatchPutItem(self.connection, "Aaa", [{"h": self.key(i), "xs": "x" * 300000} for i in range(250)])  # 300kB items ensure a single BatchGetItem will return at most 55 items

    def test(self):
        keys = [item["h"] for item in _lv.BatchGetItemIterator(self.connection, "Aaa", ({"h": self.key(i)} for i in range(250)))]
        self.assertEqual(sorted(keys), [self.key(i) for i in range(250)])
