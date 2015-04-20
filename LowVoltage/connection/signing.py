# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime
import hashlib
import hmac
import json
import unittest
import urlparse

import requests

from LowVoltage.actions.action import Action, ActionProxy
import LowVoltage.exceptions as _exn
import LowVoltage.policies as _pol


class SigningConnection(object):
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


class SigningConnectionUnitTests(unittest.TestCase):
    class FakeResponse(object):
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

        def json(self):
            return json.loads(self.text)

    def setUp(self):
        super(SigningConnectionUnitTests, self).setUp()
        self.connection = SigningConnection("us-west-2", _pol.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/")

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


class SigningConnectionLocalIntegTests(unittest.TestCase):
    class TestAction(Action):
        def build(self):
            return {}

    def test_network_error(self):
        connection = SigningConnection("us-west-2", _pol.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65555/")
        with self.assertRaises(_exn.NetworkError):
            connection.request(self.TestAction("ListTables"))
