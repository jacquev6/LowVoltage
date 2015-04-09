# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import datetime
import hashlib
import hmac
import json
import unittest
import urlparse

import requests

from LowVoltage.operations.operation import Operation as _Operation, OperationProxy as _OperationProxy
import LowVoltage.exceptions as _exn
import LowVoltage.tests.dynamodb_local


class StaticCredentials(object):
    def __init__(self, key, secret):
        self.__credentials = (key, secret)

    def get(self):
        return self.__credentials


class Connection(object):
    def __init__(self, region, credentials, endpoint=None):
        self.__region = region
        self.__credentials = credentials
        if endpoint is None:
            self.__endpoint = "https://dynamodb.{}.amazonaws.com/".format(region)  # pragma no cover (Covered by optional integ tests)
        else:
            self.__endpoint = endpoint
        self.__host = urlparse.urlparse(self.__endpoint).hostname
        self.__session = requests.Session()

    def request(self, operation, payload=None):
        if isinstance(operation, basestring):
            if isinstance(payload, basestring):
                return self._request_raw(operation, payload)
            elif isinstance(payload, dict):
                return self._request_json(operation, payload)
            else:
                raise TypeError("When 'operation' is a string, 'payload' should be a string or a dict")
        elif isinstance(operation, (_Operation, _OperationProxy)):
            if payload is None:
                return self._request_operation(operation)
            else:
                raise TypeError("When 'operation' is an Operation, 'payload' should be None")
        else:
            raise TypeError("'operation' should be an Operation or a string")

    def _request_operation(self, operation):
        return operation.Result(**self._request_json(operation.name, operation.build()))

    def _request_json(self, operation, payload):
        return json.loads(self._request_raw(operation, json.dumps(payload)))

    def _request_raw(self, operation, payload):
        headers = self._sign(datetime.datetime.utcnow(), operation, payload)
        r = self.__session.post(self.__endpoint, data=payload, headers=headers)
        if r.status_code == 200:
            return r.text
        else:
            self._raise(r)

    def _raise(self, r):
        try:
            data = r.json()
            typ = data.get("__type")
        except ValueError:
            data = r.text
            typ = None
        if r.status_code == 400:
            if typ is None:
                raise _exn.ClientError(data)
            elif typ.endswith("ResourceNotFoundException"):
                raise _exn.ResourceNotFoundException(data)
            elif typ.endswith("ValidationException"):
                raise _exn.ValidationException(data)
            elif typ.endswith("ConditionalCheckFailedException"):
                raise _exn.ConditionalCheckFailedException(data)
            elif typ.endswith("ItemCollectionSizeLimitExceededException"):
                raise _exn.ItemCollectionSizeLimitExceededException(data)
            elif typ.endswith("ProvisionedThroughputExceededException"):
                raise _exn.ProvisionedThroughputExceededException(data)
            elif typ.endswith("LimitExceededException"):
                raise _exn.LimitExceededException(data)
            elif typ.endswith("ResourceInUseException"):
                raise _exn.ResourceInUseException(data)
            else:
                raise _exn.ClientError(data)
        elif r.status_code == 500:
            raise _exn.ServerError(data)
        else:
            raise _exn.UnknownError(r.status_code, r.text)

    def _sign(self, now, operation, payload):
        # http://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
        assert isinstance(now, datetime.datetime)
        assert isinstance(operation, basestring)
        assert isinstance(payload, basestring)

        timestamp = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")

        headers = {
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Date": timestamp,
            "X-Amz-Target": "DynamoDB_20120810.{}".format(operation),
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


class ConnectionUnitTests(unittest.TestCase):
    class FakeResponse(object):
        def __init__(self, status_code, text):
            self.status_code = status_code
            self.text = text

        def json(self):
            return json.loads(self.text)

    def setUp(self):
        self.connection = Connection("us-west-2", StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/")

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

    def testBadOperation(self):
        with self.assertRaises(TypeError) as catcher:
            self.connection.request(42, {})
        self.assertEqual(catcher.exception.args, ("'operation' should be an Operation or a string",))

    def testBadPayload(self):
        with self.assertRaises(TypeError) as catcher:
            self.connection.request("Operation", 42)
        self.assertEqual(catcher.exception.args, ("When 'operation' is a string, 'payload' should be a string or a dict",))

    def testPayloadWithOperation(self):
        with self.assertRaises(TypeError) as catcher:
            self.connection.request(_Operation("Foo"), "")
        self.assertEqual(catcher.exception.args, ("When 'operation' is an Operation, 'payload' should be None",))

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


class ConnectionIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    class TestOperation(_Operation):
        class Result(object):
            def __init__(self, **kwds):
                self.kwds = kwds

        def build(self):
            return {}

    def test_request_with_text_payload(self):
        self.assertEqual(self.connection.request("ListTables", "{}"), '{"TableNames":[]}')

    def test_request_with_dict_payload(self):
        self.assertEqual(self.connection.request("ListTables", {}), {"TableNames": []})

    def test_request_with_operation(self):
        r = self.connection.request(self.TestOperation("ListTables"))
        self.assertIsInstance(r, self.TestOperation.Result)
        self.assertEqual(r.kwds, {"TableNames": []})

    def test_error_with_text_payload(self):
        with self.assertRaises(_exn.ClientError):
            self.connection.request("UnexistingOperation", "{}")

    def test_error_with_dict_payload(self):
        with self.assertRaises(_exn.ClientError):
            self.connection.request("UnexistingOperation", {})

    def test_error_with_operation(self):
        with self.assertRaises(_exn.ClientError):
            self.connection.request(self.TestOperation("UnexistingOperation"))


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
