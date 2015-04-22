# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import collections
import datetime
import numbers

from .unit_tests import UnitTests


class NotCovered(Exception):
    pass


class _Cover(object):
    def __init__(self, prefix):
        self._prefix = prefix

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            not_covered = set(self._verify())
            if not_covered:
                raise NotCovered(sorted(not_covered))


class CoverAttributes(_Cover):
    def __init__(self, prefix, o):
        super(CoverAttributes, self).__init__(prefix)
        self.__o = o
        self.__not_covered = set(a for a in dir(o) if not a.startswith("_"))
        self.__children = {}

    def _verify(self):
        for nc in self.__not_covered:
            yield self._prefix + "." + nc
        for name, child in self.__children.iteritems():
            if isinstance(child, _Cover):
                for nc in child._verify():
                    yield nc

    def __getattr__(self, name):
        self.__not_covered.discard(name)
        if name not in self.__children:
            self.__children[name] = cover(self._prefix + "." + name, getattr(self.__o, name))
        return self.__children[name]


class CoverList(_Cover):
    def __init__(self, prefix, o):
        super(CoverList, self).__init__(prefix)
        self.__o = o
        self.__not_covered = set(range(len(o)))
        self.__children = {}

    def _verify(self):
        for nc in self.__not_covered:
            yield self._prefix + "[" + str(nc) + "]"
        for index, child in self.__children.iteritems():
            if isinstance(child, _Cover):
                for nc in child._verify():
                    yield nc

    def __getitem__(self, index):
        self.__not_covered.discard(index)
        if index not in self.__children:
            self.__children[index] = cover(self._prefix + "[" + str(index) + "]", self.__o[index])
        return self.__children[index]


def cover(prefix, anything):
    if isinstance(anything, (bool, numbers.Number, basestring, type(None), dict, datetime.datetime)):
        return anything
    elif isinstance(anything, list):
        return CoverList(prefix, anything)
    else:
        return CoverAttributes(prefix, anything)


class CoverSimpleAttributesUnitTests(UnitTests):
    class TestData:
        def __init__(self):
            self.a = True
            self.b = 42
            self.c = "abc"

    data = TestData()

    def testCoverAll(self):
        with cover("data", self.data) as u:
            self.assertEqual(u.a, True)
            self.assertEqual(u.b, 42)
            self.assertEqual(u.c, "abc")

    def testCoverSeveralTimes(self):
        with cover("data", self.data) as u:
            self.assertEqual(u.a, True)
            self.assertEqual(u.a, True)
            self.assertEqual(u.b, 42)
            self.assertEqual(u.c, "abc")

    def test_not_covered_attributes_raise(self):
        with self.assertRaises(NotCovered) as catcher:
            with cover("data", self.data) as u:
                u.b
        self.assertEqual(catcher.exception.args, (["data.a", "data.c"],))

    def test_exception_goes_through(self):
        exception = Exception()
        with self.assertRaises(Exception) as catcher:
            with cover("data", self.data) as u:
                raise exception
        self.assertIs(catcher.exception, exception)


class CoverObjectAttributesUnitTests(UnitTests):
    class TestData:
        def __init__(self, d):
            self.d = d
            self.e = d

    data = TestData(TestData(42))

    def testCoverAll(self):
        with cover("data", self.data) as u:
            self.assertEqual(u.d.d, 42)
            self.assertEqual(u.d.e, 42)
            self.assertEqual(u.e.d, 42)
            self.assertEqual(u.e.e, 42)

    def testDontCoverAttribute(self):
        with self.assertRaises(NotCovered) as catcher:
            with cover("data", self.data) as u:
                u.d.d
                u.d.e
        self.assertEqual(catcher.exception.args, (["data.e"],))

    def testDontCoverAttributesAttribute(self):
        with self.assertRaises(NotCovered) as catcher:
            with cover("data", self.data) as u:
                u.d.d
                u.d.e
                u.e.d
        self.assertEqual(catcher.exception.args, (["data.e.e"],))


class CoverListUnitTests(UnitTests):
    class TestData:
        def __init__(self):
            self.a = True

    data = [1, 2, TestData()]

    def testCoverAll(self):
        with cover("data", self.data) as u:
            self.assertEqual(u[0], 1)
            self.assertEqual(u[1], 2)
            self.assertEqual(u[2].a, True)

    def testCoverSeveralTimes(self):
        with cover("data", self.data) as u:
            self.assertEqual(u[0], 1)
            self.assertEqual(u[0], 1)
            self.assertEqual(u[1], 2)
            self.assertEqual(u[2].a, True)

    def testDontCoverItem(self):
        with self.assertRaises(NotCovered) as catcher:
            with cover("data", self.data) as u:
                u[0]
                u[2].a
        self.assertEqual(catcher.exception.args, (["data[1]"],))

    def testDontCoverItemsAttribute(self):
        with self.assertRaises(NotCovered) as catcher:
            with cover("data", self.data) as u:
                u[0]
                u[1]
                u[2]
        self.assertEqual(catcher.exception.args, (["data[2].a"],))
