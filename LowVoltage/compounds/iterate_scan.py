# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import copy

import LowVoltage as _lv
import LowVoltage.testing as _tst


def iterate_scan(connection, scan):
    """
    Make as many :class:`.Scan` actions as needed to iterate over all matching items.
    That is until :attr:`.ScanResponse.last_evaluated_key` is ``None``.

    >>> for item in iterate_scan(connection, Scan(table)):
    ...   print item
    {u'a': 0, u'h': 7}
    {u'a': 0, u'h': 8}
    {u'h': 3, u'gr': 4, u'gh': 9}
    {u'h': 2, u'gr': 6, u'gh': 4}
    {u'a': 0, u'h': 9}
    {u'h': 4, u'gr': 2, u'gh': 16}
    {u'h': 6, u'gr': -2, u'gh': 36}
    {u'h': 1, u'gr': 8, u'gh': 1}
    {u'h': 0, u'gr': 10, u'gh': 0}
    {u'h': 5, u'gr': 0, u'gh': 25}

    The :class:`.Scan` instance passed in must be discarded (it is modified during the iteration).
    """
    r = connection(scan)
    for item in r.items:
        yield item
    while r.last_evaluated_key is not None:
        scan.exclusive_start_key(r.last_evaluated_key)
        r = connection(scan)
        for item in r.items:
            yield item


def parallelize_scan(scan, total_segments):
    """
    Create ``total_segments`` :class:`.Scan` to be used in `parallel <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/QueryAndScan.html#QueryAndScanParallelScan>`__.

    >>> segments = parallelize_scan(Scan(table), 3)

    Note that you would typically iterate other each segment in a different thread.

    >>> for segment in segments:
    ...   print "Segment"
    ...   for item in iterate_scan(connection, segment):
    ...     print item
    Segment
    {u'a': 0, u'h': 7}
    {u'a': 0, u'h': 8}
    {u'h': 3, u'gr': 4, u'gh': 9}
    Segment
    {u'h': 2, u'gr': 6, u'gh': 4}
    {u'a': 0, u'h': 9}
    {u'h': 4, u'gr': 2, u'gh': 16}
    {u'h': 6, u'gr': -2, u'gh': 36}
    Segment
    {u'h': 1, u'gr': 8, u'gh': 1}
    {u'h': 0, u'gr': 10, u'gh': 0}
    {u'h': 5, u'gr': 0, u'gh': 25}
    """
    return [
        copy.deepcopy(scan).segment(i, total_segments)
        for i in range(total_segments)
    ]


class IterateScanUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(IterateScanUnitTests, self).setUp()
        self.connection = self.mocks.create("connection")

    def test_iterate_scan(self):
        self.connection.expect._call_.withArguments(
            self.ActionChecker(
                "Scan",
                {
                    "TableName": "Table",
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
                "Scan",
                {
                    "TableName": "Table",
                    "ExclusiveStartKey": {"h": {"N": "0"}, "r": {"S": "bar"}},
                }
            )
        ).andReturn(
            _lv.QueryResponse(
                Items=[{"h": {"N": "0"}, "r": {"S": "baz"}}],
            )
        )

        self.assertEqual(
            list(iterate_scan(self.connection.object, _lv.Scan("Table"))),
            [{'h': 0, 'r': 'foo'}, {'h': 0, 'r': 'bar'}, {'h': 0, 'r': 'baz'}]
        )

    def test_parallelize_scan(self):
        s1, s2 = parallelize_scan(_lv.Scan("Table"), 2)
        self.assertEqual(s1.payload, {"TableName": "Table", "Segment": 0, "TotalSegments": 2})
        self.assertEqual(s2.payload, {"TableName": "Table", "Segment": 1, "TotalSegments": 2})
