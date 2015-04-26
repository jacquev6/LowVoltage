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
    {u'h': 7, u'gr': 0, u'gh': 0}
    {u'h': 8, u'gr': 0, u'gh': 0}
    {u'h': 3, u'gr': 0, u'gh': 0}
    {u'h': 2, u'gr': 0, u'gh': 0}
    {u'h': 9, u'gr': 0, u'gh': 0}
    {u'h': 4, u'gr': 0, u'gh': 0}
    {u'h': 6, u'gr': 0, u'gh': 0}
    {u'h': 1, u'gr': 0, u'gh': 0}
    {u'h': 0, u'gr': 0, u'gh': 0}
    {u'h': 5, u'gr': 0, u'gh': 0}

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

    (You would typically iterate other each segment in a different thread.)

    >>> for segment in segments:
    ...   print "Segment"
    ...   for item in iterate_scan(connection, segment):
    ...     print item
    Segment
    {u'h': 7, u'gr': 0, u'gh': 0}
    {u'h': 8, u'gr': 0, u'gh': 0}
    {u'h': 3, u'gr': 0, u'gh': 0}
    Segment
    {u'h': 2, u'gr': 0, u'gh': 0}
    {u'h': 9, u'gr': 0, u'gh': 0}
    {u'h': 4, u'gr': 0, u'gh': 0}
    {u'h': 6, u'gr': 0, u'gh': 0}
    Segment
    {u'h': 1, u'gr': 0, u'gh': 0}
    {u'h': 0, u'gr': 0, u'gh': 0}
    {u'h': 5, u'gr': 0, u'gh': 0}
    """
    return [
        copy.deepcopy(scan).segment(i, total_segments)
        for i in range(total_segments)
    ]


class ScanIteratorLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    keys = [u"{:03}".format(k) for k in range(15)]

    def setUp(self):
        super(ScanIteratorLocalIntegTests, self).setUp()
        self.connection(
            _lv.BatchWriteItem().table("Aaa").put(
                {"h": h, "xs": "x" * 300000}  # 300kB items ensure a single Query will return at most 4 items
                for h in self.keys
            )
        )

    def test_simple_scan(self):
        self.assertEqual(
            sorted(item["h"] for item in _lv.iterate_scan(self.connection, _lv.Scan("Aaa"))),
            self.keys
        )

    def test_parallel_scan(self):
        keys = []
        for segment in parallelize_scan(_lv.Scan("Aaa"), 3):
            keys.extend(item["h"] for item in iterate_scan(self.connection, segment))
        self.assertEqual(sorted(keys), self.keys)
