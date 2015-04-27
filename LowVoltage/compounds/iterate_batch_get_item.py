# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from LowVoltage.variadic import variadic


@variadic(dict)
def iterate_batch_get_item(connection, table, keys):
    """
    Make as many :class:`.BatchGetItem` actions as needed to iterate over all specified items.
    Including processing :attr:`.BatchGetItemResponse.unprocessed_keys`.

    .. Warning, this is NOT doctest. Because doctests aren't stable because items order changes.

    ::

        >>> for item in iterate_batch_get_item(connection, table, {"h": 0}, {"h": 1}, {"h": 2}):
        ...   print item
        {u'h': 1, u'gr': 0, u'gh': 0}
        {u'h': 2, u'gr': 0, u'gh': 0}
        {u'h': 0, u'gr': 0, u'gh': 0}

    Note that items are returned in an unspecified order.
    """
    unprocessed_keys = []

    while len(keys) != 0:
        r = connection(_lv.BatchGetItem().table(table).keys(keys[:100]))
        keys = keys[100:]
        if isinstance(r.unprocessed_keys, dict) and table in r.unprocessed_keys and "Keys" in r.unprocessed_keys[table]:
            unprocessed_keys.extend(r.unprocessed_keys[table]["Keys"])
        for item in r.responses.get(table, []):
            yield item

    while len(unprocessed_keys) != 0:
        r = connection(_lv.BatchGetItem().previous_unprocessed_keys({table: {"Keys": unprocessed_keys[:100]}}))
        unprocessed_keys = unprocessed_keys[100:]
        if isinstance(r.unprocessed_keys, dict) and table in r.unprocessed_keys and "Keys" in r.unprocessed_keys[table]:
            unprocessed_keys.extend(r.unprocessed_keys[table]["Keys"])
        for item in r.responses.get(table, []):
            yield item


class IterateBatchGetItemUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(IterateBatchGetItemUnitTests, self).setUp()
        self.connection = self.mocks.create("connection")

    def test_no_keys(self):
        self.assertEqual(
            list(_lv.iterate_batch_get_item(self.connection.object, "Aaa", [])),
            []
        )

    def test_one_page(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker("BatchGetItem", {"RequestItems": {"Aaa": {"Keys": [{"h": {"S": "a"}}, {"h": {"S": "b"}}]}}})
        ).andReturn(
            _lv.BatchGetItemResponse(Responses={"Aaa": [{"h": {"S": "c"}}, {"h": {"S": "d"}}]})
        )

        self.assertEqual(
            list(_lv.iterate_batch_get_item(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})),
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
            list(_lv.iterate_batch_get_item(self.connection.object, "Aaa", {"h": u"a"}, {"h": u"b"})),
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
            list(_lv.iterate_batch_get_item(self.connection.object, "Aaa", ({"h": i} for i in range(0, 250)))),
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
            list(_lv.iterate_batch_get_item(self.connection.object, "Aaa", [{"h": i} for i in range(0, 150)])),
            [{"h": i} for i in range(1000, 1250)]
        )
