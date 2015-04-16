# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime
import hashlib
import hmac
import json
import unittest
import urlparse
import time

import requests
import MockMockMock

from LowVoltage.actions.action import Action as Action, ActionProxy as ActionProxy
import LowVoltage.exceptions as _exn
import LowVoltage.policies as _pol
import LowVoltage.testing.dynamodb_local


class StaticCredentials(object):
    """The simplest credential provider: a constant key/secret pair"""

    def __init__(self, key, secret):
        self.__credentials = (key, secret)

    def get(self):
        return self.__credentials


class BasicConnection(object):
    """Connection class responsible for signing and sending requests"""

    def __init__(self, region, credentials, endpoint):
        self.__region = region
        self.__credentials = credentials
        self.__endpoint = endpoint
        self.__host = urlparse.urlparse(self.__endpoint).hostname
        self.__session = requests.Session()

    def request(self, action):
        if not isinstance(action, (Action, ActionProxy)):
            raise TypeError

        payload = json.dumps(action.build())
        headers = self._sign(datetime.datetime.utcnow(), action.name, payload)
        try:
            r = self.__session.post(self.__endpoint, data=payload, headers=headers)
        except requests.exceptions.RequestException as e:
            raise _exn.NetworkError(e)
        except Exception as e:
            raise _exn.UnknownError(e)
        if r.status_code == 200:
            try:
                data = r.json()
            except ValueError:
                raise ServerError(r.text)
            return action.Result(**data)
        else:
            self._raise(r)

    def _raise(self, r):
        try:
            data = r.json()
            typ = data.get("__type")
        except ValueError:
            data = r.text
            typ = None
        if 400 <= r.status_code < 500:
            if typ is not None:
                for suffix, cls in _exn.client_errors:
                    if typ.endswith(suffix):
                        raise cls(data)
            raise _exn.UnknownClientError(data)
        elif 500 <= r.status_code < 600:
            raise _exn.ServerError(data)
        else:
            raise _exn.UnknownError(r.status_code, r.text)

    def _sign(self, now, action, payload):
        # http://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
        assert isinstance(now, datetime.datetime)
        assert isinstance(action, basestring)
        assert isinstance(payload, basestring)

        timestamp = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")

        headers = {
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Date": timestamp,
            "X-Amz-Target": "DynamoDB_20120810.{}".format(action),
            "Host": self.__host,
        }

        header_names = ";".join(key.lower() for key in sorted(headers.keys()))
        request = "POST\n/\n\n{}\n{}\n{}".format(
            "".join("{}:{}\n".format(key.lower(), val) for key, val in sorted(headers.iteritems())),
            header_names,
            hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        )
        credentials = "{}/{}/dynamodb/aws4_request".format(
            datestamp,
            self.__region,
        )
        to_sign = "AWS4-HMAC-SHA256\n{}\n{}\n{}".format(
            timestamp,
            credentials,
            hashlib.sha256(request.encode("utf-8")).hexdigest(),
        )

        aws_key, aws_secret = self.__credentials.get()

        key = hmac.new(
            hmac.new(
                hmac.new(
                    hmac.new(
                        "AWS4{}".format(aws_secret).encode("utf-8"),
                        datestamp.encode("utf-8"),
                        hashlib.sha256
                    ).digest(),
                    self.__region.encode("utf-8"),
                    hashlib.sha256
                ).digest(),
                "dynamodb".encode("utf-8"),
                hashlib.sha256
            ).digest(),
            "aws4_request".encode("utf-8"),
            hashlib.sha256
        ).digest()

        headers["Authorization"] = "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}".format(
            aws_key,
            credentials,
            header_names,
            hmac.new(key, to_sign.encode("utf-8"), hashlib.sha256).hexdigest(),
        )

        return headers


class BasicConnectionUnitTests(unittest.TestCase):
    class FakeResponse(object):
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

        def json(self):
            return json.loads(self.text)

    def setUp(self):
        self.connection = BasicConnection("us-west-2", StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/")

    def testSign(self):
        self.assertEqual(
            self.connection._sign(datetime.datetime(2014, 10, 4, 6, 33, 2), "Operation", '{"Payload": "Value"}'),
            {
                "Host": "localhost",
                "Content-Type": "application/x-amz-json-1.0",
                "Authorization": "AWS4-HMAC-SHA256 Credential=DummyKey/20141004/us-west-2/dynamodb/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=f47b4025d95692c1623d01bd7db6d53e68f7a8a28264c1ab3393477f0dae520a",
                "X-Amz-Date": "20141004T063302Z",
                "X-Amz-Target": "DynamoDB_20120810.Operation",
            }
        )

    def testBadAction(self):
        with self.assertRaises(TypeError):
            self.connection.request(42)

    def testUnknownError(self):
        with self.assertRaises(_exn.UnknownError) as catcher:
            self.connection._raise(self.FakeResponse(999, "{}"))
        self.assertEqual(catcher.exception.args, (999, "{}"))

    def testUnknownErrorWithoutJson(self):
        with self.assertRaises(_exn.UnknownError) as catcher:
            self.connection._raise(self.FakeResponse(999, "not json"))
        self.assertEqual(catcher.exception.args, (999, "not json"))

    def testServerError(self):
        with self.assertRaises(_exn.ServerError) as catcher:
            self.connection._raise(self.FakeResponse(500, '{"foo": "bar"}'))
        self.assertEqual(catcher.exception.args, ({"foo": "bar"},))

    def testServerErrorWithoutJson(self):
        with self.assertRaises(_exn.ServerError) as catcher:
            self.connection._raise(self.FakeResponse(500, "not json"))
        self.assertEqual(catcher.exception.args, ("not json",))

    def testClientErrorWithoutType(self):
        with self.assertRaises(_exn.ClientError) as catcher:
            self.connection._raise(self.FakeResponse(400, "{}"))
        self.assertEqual(catcher.exception.args, ({},))

    def testClientErrorWithUnknownType(self):
        with self.assertRaises(_exn.ClientError) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.UnhandledException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.UnhandledException", "Message": "tralala"},))

    def testClientErrorWithoutJson(self):
        with self.assertRaises(_exn.ClientError) as catcher:
            self.connection._raise(self.FakeResponse(400, "not json"))
        self.assertEqual(catcher.exception.args, ("not json",))

    def testResourceNotFoundException(self):
        with self.assertRaises(_exn.ResourceNotFoundException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ResourceNotFoundException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ResourceNotFoundException", "Message": "tralala"},))

    def testValidationException(self):
        with self.assertRaises(_exn.ValidationException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ValidationException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ValidationException", "Message": "tralala"},))

    def testConditionalCheckFailedException(self):
        with self.assertRaises(_exn.ConditionalCheckFailedException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ConditionalCheckFailedException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ConditionalCheckFailedException", "Message": "tralala"},))

    def testItemCollectionSizeLimitExceededException(self):
        with self.assertRaises(_exn.ItemCollectionSizeLimitExceededException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ItemCollectionSizeLimitExceededException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ItemCollectionSizeLimitExceededException", "Message": "tralala"},))

    def testProvisionedThroughputExceededException(self):
        with self.assertRaises(_exn.ProvisionedThroughputExceededException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ProvisionedThroughputExceededException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ProvisionedThroughputExceededException", "Message": "tralala"},))

    def testLimitExceededException(self):
        with self.assertRaises(_exn.LimitExceededException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.LimitExceededException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.LimitExceededException", "Message": "tralala"},))

    def testResourceInUseException(self):
        with self.assertRaises(_exn.ResourceInUseException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ResourceInUseException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ResourceInUseException", "Message": "tralala"},))


class BasicConnectionIntegTests(unittest.TestCase):
    class TestAction(Action):
        def build(self):
            return {}

    def test_network_error(self):
        connection = BasicConnection("us-west-2", LowVoltage.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65555/")
        with self.assertRaises(_exn.NetworkError):
            connection.request(self.TestAction("ListTables"))


class RetryingConnection:
    """Connection decorator retrying failed requests (due to network, server and throtling errors)"""

    def __init__(self, connection, error_policy):
        self.__connection = connection
        self.__error_policy = error_policy

    def request(self, action):
        errors = 0
        while True:
            try:
                return self.__connection.request(action)
            except _exn.Error as e:
                errors += 1
                delay = self.__error_policy.get_retry_delay_on_exception(action, e, errors)
                if delay is None:
                    raise
                else:
                    time.sleep(delay)


class RetryingConnectionUnitTests(unittest.TestCase):
    def setUp(self):
        self.mocks = MockMockMock.Engine()
        self.policy = self.mocks.create("policy")
        self.basic_connection = self.mocks.create("connection")
        self.connection = RetryingConnection(self.basic_connection.object, self.policy.object)
        self.action = object()
        self.response = object()

    def tearDown(self):
        self.mocks.tearDown()

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


class RetryingConnectionIntegTests(unittest.TestCase):
    class TestAction(Action):
        class Result(object):
            def __init__(self, **kwds):
                self.kwds = kwds

        def __init__(self, name, payload={}):
            Action.__init__(self, name)
            self.__payload = payload

        def build(self):
            return self.__payload

    @classmethod
    def setUpClass(cls):
        cls.connection = RetryingConnection(BasicConnection("us-west-2", LowVoltage.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/"), _pol.ExponentialBackoffErrorPolicy(1, 2, 5))

    def test_request(self):
        r = self.connection.request(self.TestAction("ListTables"))
        self.assertIsInstance(r, self.TestAction.Result)
        self.assertEqual(r.kwds, {"TableNames": []})

    def test_client_error(self):
        with self.assertRaises(_exn.InvalidAction):
            self.connection.request(self.TestAction("UnexistingAction"))

    def test_network_error(self):
        connection = RetryingConnection(BasicConnection("us-west-2", LowVoltage.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65555/"), _pol.ExponentialBackoffErrorPolicy(0, 1, 4))
        with self.assertRaises(_exn.NetworkError):
            connection.request(self.TestAction("ListTables"))

    def test_unexisting_table(self):
        with self.assertRaises(_exn.ResourceNotFoundException):
            self.connection.request(self.TestAction("GetItem", {"TableName": "Bbb"}))


class CompletingConnection:
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


class CompletingConnectionIntegTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_connection = RetryingConnection(BasicConnection("us-west-2", LowVoltage.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/"), _pol.ExponentialBackoffErrorPolicy(1, 2, 5))
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


class WaitingConnection:
    """Connection decorator waiting until admin actions are done (until table's state is ACTIVE)"""

    def __init__(self, connection):
        self.__connection = connection

    def request(self, action):
        # @todo Implement (should wait until CreateTable, UpdateTable and DeleteTable's effect is applied)
        return self.__connection.request(action)


def make_connection(
    region,
    credentials,
    endpoint=None,
    error_policy=_pol.ExponentialBackoffErrorPolicy(1, 2, 5),
    complete_batches=True,
    wait_for_tables=True,
):
    """Create a connection, using all decorators (RetryingConnection, CompletingConnection, WaitingConnection on top of a BasicConnection)"""
    # @todo Maybe allow injection of the Requests session to tweek low-level parameters (connection timeout, etc.)?

    if endpoint is None:
        endpoint = "https://dynamodb.{}.amazonaws.com/".format(region)
    connection = BasicConnection(region, credentials, endpoint)
    if error_policy is not None:
        connection = RetryingConnection(connection, error_policy)
    if complete_batches:
        connection = RetryingConnection(connection)
    if wait_for_tables:
        connection = WaitingConnection(connection)
    return connection
