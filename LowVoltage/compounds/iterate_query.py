# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


def iterate_query(connection, query):
    """
    Make as many :class:`.Query` actions as needed to iterate over all matching items.
    That is until :attr:`.QueryResponse.last_evaluated_key` is ``None``.

    >>> for item in iterate_query(connection, Query(table2).key_eq("h", 42).key_between("r1", 4, 7)):
    ...   print item
    {u'h': 42, u'r1': 4, u'r2': 6}
    {u'h': 42, u'r1': 5, u'r2': 5}
    {u'h': 42, u'r1': 6}
    {u'h': 42, u'r1': 7}

    The :class:`.Query` instance passed in must be discarded (it is modified during the iteration).
    """
    r = connection(query)
    for item in r.items:
        yield item
    while r.last_evaluated_key is not None:
        query.exclusive_start_key(r.last_evaluated_key)
        r = connection(query)
        for item in r.items:
            yield item


class IterateQueryUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(IterateQueryUnitTests, self).setUp()
        self.connection = self.mocks.create("connection")

    def test(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker(
                "Query",
                {
                    "TableName": "Table",
                    "KeyConditions": {"h": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "0"}]}}
                }
            )
        ).andReturn(
            _lv.QueryResponse(
                Items=[{"h": {"N": "0"}, "r": {"S": "foo"}}, {"h": {"N": "0"}, "r": {"S": "bar"}}],
                LastEvaluatedKey={"h": {"N": "0"}, "r": {"S": u"bar"}},
            )
        )
        self.connection.expect._call_.withArguments(
            self.ActionChecker(
                "Query",
                {
                    "TableName": "Table",
                    "KeyConditions": {"h": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "0"}]}},
                    "ExclusiveStartKey": {"h": {"N": "0"}, "r": {"S": "bar"}},
                }
            )
        ).andReturn(
            _lv.QueryResponse(
                Items=[{"h": {"N": "0"}, "r": {"S": "baz"}}],
            )
        )

        self.assertEqual(
            list(iterate_query(self.connection.object, _lv.Query("Table").key_eq("h", 0))),
            [{'h': 0, 'r': 'foo'}, {'h': 0, 'r': 'bar'}, {'h': 0, 'r': 'baz'}]
        )
