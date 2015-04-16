# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import MockMockMock

import LowVoltage
from .signing import SigningConnection
from .retrying import RetryingConnection
import LowVoltage.exceptions as _exn
import LowVoltage.policies as _pol


class CompletingConnection(object):
    """Connection decorator completing batch actions (UnprocessedKeys and UnprocessedItems)"""

    def __init__(self, connection):
        self.__connection = connection

    def request(self, action):
        r = self.__connection.request(action)
        if action.is_completable:
            next_action = action.get_completion_action(r)
            while next_action is not None:
                next_response = self.__connection.request(next_action)
                action.complete_response(r, next_response)
                next_action = action.get_completion_action(r)
        return r


class CompletingConnectionUnitTests(unittest.TestCase):
    def setUp(self):
        self.mocks = MockMockMock.Engine()
        self.base_connection = self.mocks.create("base_connection")
        self.action = self.mocks.create("action")
        self.connection = CompletingConnection(self.base_connection.object)

    def tearDown(self):
        self.mocks.tearDown()

    def test_dont_complete_uncompletable_action(self):
        r = object()
        self.base_connection.expect.request(self.action.object).andReturn(r)
        self.action.expect.is_completable.andReturn(False)

        self.assertIs(
            self.connection.request(self.action.object),
            r
        )

    def test_try_to_complete_action(self):
        r = object()
        self.base_connection.expect.request(self.action.object).andReturn(r)
        self.action.expect.is_completable.andReturn(True)
        self.action.expect.get_completion_action(r).andReturn(None)

        self.assertIs(
            self.connection.request(self.action.object),
            r
        )

    def test_complete_action_once(self):
        r1 = object()
        self.base_connection.expect.request(self.action.object).andReturn(r1)
        self.action.expect.is_completable.andReturn(True)
        a2 = object()
        self.action.expect.get_completion_action(r1).andReturn(a2)
        r2 = object()
        self.base_connection.expect.request(a2).andReturn(r2)
        self.action.expect.complete_response(r1, r2)
        self.action.expect.get_completion_action(r1).andReturn(None)

        self.assertIs(
            self.connection.request(self.action.object),
            r1
        )

    def test_complete_several_times(self):
        r1 = object()
        self.base_connection.expect.request(self.action.object).andReturn(r1)
        self.action.expect.is_completable.andReturn(True)
        a2 = object()
        self.action.expect.get_completion_action(r1).andReturn(a2)
        r2 = object()
        self.base_connection.expect.request(a2).andReturn(r2)
        self.action.expect.complete_response(r1, r2)
        a3 = object()
        self.action.expect.get_completion_action(r1).andReturn(a3)
        r3 = object()
        self.base_connection.expect.request(a3).andReturn(r3)
        self.action.expect.complete_response(r1, r3)
        a4 = object()
        self.action.expect.get_completion_action(r1).andReturn(a4)
        r4 = object()
        self.base_connection.expect.request(a4).andReturn(r4)
        self.action.expect.complete_response(r1, r4)
        self.action.expect.get_completion_action(r1).andReturn(None)

        self.assertIs(
            self.connection.request(self.action.object),
            r1
        )


class CompletingConnectionLocalIntegTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_connection = RetryingConnection(SigningConnection("us-west-2", _pol.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/"), _pol.ExponentialBackoffRetryPolicy(1, 2, 5))
        cls.connection = CompletingConnection(cls.base_connection)

    def setUp(self):
        self.connection.request(
            LowVoltage.CreateTable("Aaa").hash_key("h", LowVoltage.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(LowVoltage.DeleteTable("Aaa"))

    def test_complete_batch_get(self):
        for i in range(100):
            self.connection.request(LowVoltage.PutItem("Aaa", {"h": unicode(i), "xs": "x" * 300000}))

        batch_get = LowVoltage.BatchGetItem().table("Aaa").keys({"h": unicode(i)} for i in range(100))

        r = self.base_connection.request(batch_get)
        self.assertEqual(len(r.unprocessed_keys["Aaa"]["Keys"]), 45)
        self.assertEqual(len(r.responses["Aaa"]), 55)

        r = self.connection.request(batch_get)
        self.assertEqual(r.unprocessed_keys, {})
        self.assertEqual(len(r.responses["Aaa"]), 100)

