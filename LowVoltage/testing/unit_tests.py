# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

try:
    import MockMockMock
    has_mock = True
except ImportError:  # pragma no cover (Test code)
    has_mock = False


class UnitTests(unittest.TestCase):
    pass


if has_mock:
    class UnitTestsWithMocks(UnitTests):
        class ActionChecker(object):
            def __init__(self, expected_name, expected_payload):
                self.__expected_name = expected_name
                self.__expected_payload = expected_payload

            def __call__(self, args, kwds):
                assert len(args) == 1
                assert len(kwds) == 0
                action, = args
                return action.name == self.__expected_name and action.build() == self.__expected_payload

        def setUp(self):
            super(UnitTestsWithMocks, self).setUp()
            self.mocks = MockMockMock.Engine()

        def tearDown(self):
            self.mocks.tearDown()
            super(UnitTestsWithMocks, self).tearDown()
else:
    class UnitTestsWithMocks(object):  # pragma no cover (Test code)
        pass
