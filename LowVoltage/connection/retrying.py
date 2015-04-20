# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import time
import unittest

import MockMockMock

import LowVoltage.testing as _tst
from LowVoltage.actions.action import Action
from .signing import SigningConnection
import LowVoltage.exceptions as _exn
import LowVoltage.policies as _pol


class RetryingConnection(object):
    """Connection decorator retrying failed requests (due to network, server and throttling errors)"""

    def __init__(self, connection, retry_policy):
        self.__connection = connection
        self.__retry_policy = retry_policy

    def request(self, action):
        errors = 0
        while True:
            try:
                return self.__connection.request(action)
            except _exn.Error as e:
                errors += 1
                delay = self.__retry_policy.get_retry_delay_on_exception(action, e, errors)
                if delay is None:
                    raise
                else:
                    time.sleep(delay)


class RetryingConnectionUnitTests(unittest.TestCase):
    def setUp(self):
        super(RetryingConnectionUnitTests, self).setUp()
        self.mocks = MockMockMock.Engine()
        self.policy = self.mocks.create("policy")
        self.basic_connection = self.mocks.create("connection")
        self.connection = RetryingConnection(self.basic_connection.object, self.policy.object)
        self.action = object()
        self.response = object()

    def tearDown(self):
        self.mocks.tearDown()
        super(RetryingConnectionUnitTests, self).tearDown()

    def test_unknown_exception_is_passed_through(self):
        exception = Exception()
        self.basic_connection.expect.request(self.action).andRaise(exception)
        with self.assertRaises(Exception) as catcher:
            self.connection.request(self.action)
        self.assertIs(catcher.exception, exception)

    def test_known_error_is_retried_until_success(self):
        exception = _exn.Error()
        self.basic_connection.expect.request(self.action).andRaise(exception)
        self.policy.expect.get_retry_delay_on_exception(self.action, exception, 1).andReturn(0)
        self.basic_connection.expect.request(self.action).andRaise(exception)
        self.policy.expect.get_retry_delay_on_exception(self.action, exception, 2).andReturn(0)
        self.basic_connection.expect.request(self.action).andRaise(exception)
        self.policy.expect.get_retry_delay_on_exception(self.action, exception, 3).andReturn(0)
        self.basic_connection.expect.request(self.action).andReturn(self.response)
        self.assertIs(self.connection.request(self.action), self.response)

    def test_known_error_is_retried_then_raised(self):
        exception = _exn.Error()
        self.basic_connection.expect.request(self.action).andRaise(exception)
        self.policy.expect.get_retry_delay_on_exception(self.action, exception, 1).andReturn(0)
        self.basic_connection.expect.request(self.action).andRaise(exception)
        self.policy.expect.get_retry_delay_on_exception(self.action, exception, 2).andReturn(0)
        self.basic_connection.expect.request(self.action).andRaise(exception)
        self.policy.expect.get_retry_delay_on_exception(self.action, exception, 3).andReturn(None)
        with self.assertRaises(_exn.Error) as catcher:
            self.connection.request(self.action)
        self.assertIs(catcher.exception, exception)


class RetryingConnectionLocalIntegTests(_tst.LocalIntegTests):
    class TestAction(Action):
        class Result(object):
            def __init__(self, **kwds):
                self.kwds = kwds

        def __init__(self, name, payload={}):
            Action.__init__(self, name)
            self.__payload = payload

        def build(self):
            return self.__payload

    def setUp(self):
        super(RetryingConnectionLocalIntegTests, self).setUp()
        self.connection = RetryingConnection(SigningConnection("us-west-2", _pol.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/"), _pol.ExponentialBackoffRetryPolicy(1, 2, 5))

    def test_request(self):
        r = self.connection.request(self.TestAction("ListTables"))
        self.assertIsInstance(r, self.TestAction.Result)
        self.assertEqual(r.kwds, {"TableNames": []})

    def test_client_error(self):
        with self.assertRaises(_exn.InvalidAction):
            self.connection.request(self.TestAction("UnexistingAction"))

    def test_network_error(self):
        connection = RetryingConnection(SigningConnection("us-west-2", _pol.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65555/"), _pol.ExponentialBackoffRetryPolicy(0, 1, 4))
        with self.assertRaises(_exn.NetworkError):
            connection.request(self.TestAction("ListTables"))

    def test_unexisting_table(self):
        with self.assertRaises(_exn.ResourceNotFoundException):
            self.connection.request(self.TestAction("GetItem", {"TableName": "Bbb"}))
