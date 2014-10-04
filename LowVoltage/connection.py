# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>


import datetime
import hashlib
import hmac
import json
import unittest
import urlparse

import requests

from operations import PutItem, UpdateItem
import exceptions


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

    def request(self, operation, payload):
        headers, payload = self._sign(datetime.datetime.utcnow(), operation, payload)

        r = self.__session.post(self.__endpoint, data=payload, headers=headers)
        if r.status_code == 200:
            return r.json()
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
                raise exceptions.ClientError(data)
            elif typ.endswith("ResourceNotFoundException"):
                raise exceptions.ResourceNotFoundException(data)
            elif typ.endswith("ValidationException"):
                raise exceptions.ValidationException(data)
            else:
                raise exceptions.ClientError(data)
        elif r.status_code == 500:
            raise exceptions.ServerError(data)
        else:
            raise exceptions.UnknownError(r.status_code, r.text)

    def put_item(self, table_name, item):
        return PutItem(self, table_name, item)

    def update_item(self, table_name, key):
        return UpdateItem(self, table_name, key)

    def _sign(self, now, operation, payload):
        # http://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
        timestamp = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")

        headers = {
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Date": timestamp,
            "X-Amz-Target": "DynamoDB_20120810.{}".format(operation),
            "Host": self.__host,
        }

        payload = json.dumps(payload)

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

        return headers, payload


class ConnectionTestCase(unittest.TestCase):
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
            self.connection._sign(datetime.datetime(2014, 10, 4, 6, 33, 2), "Operation", {"Payload": "Value"}),
            (
                {
                    "Host": "localhost",
                    "Content-Type": "application/x-amz-json-1.0",
                    "Authorization": "AWS4-HMAC-SHA256 Credential=DummyKey/20141004/us-west-2/dynamodb/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=f47b4025d95692c1623d01bd7db6d53e68f7a8a28264c1ab3393477f0dae520a",
                    "X-Amz-Date": "20141004T063302Z",
                    "X-Amz-Target": "DynamoDB_20120810.Operation",
                },
                '{"Payload": "Value"}'
            )
        )

    def testUnknownError(self):
        with self.assertRaises(exceptions.UnknownError) as catcher:
            self.connection._raise(self.FakeResponse(999, "{}"))
        self.assertEqual(catcher.exception.args, (999, "{}"))

    def testUnknownErrorWithoutJson(self):
        with self.assertRaises(exceptions.UnknownError) as catcher:
            self.connection._raise(self.FakeResponse(999, "not json"))
        self.assertEqual(catcher.exception.args, (999, "not json"))

    def testServerError(self):
        with self.assertRaises(exceptions.ServerError) as catcher:
            self.connection._raise(self.FakeResponse(500, '{"foo": "bar"}'))
        self.assertEqual(catcher.exception.args, ({"foo": "bar"},))

    def testServerErrorWithoutJson(self):
        with self.assertRaises(exceptions.ServerError) as catcher:
            self.connection._raise(self.FakeResponse(500, "not json"))
        self.assertEqual(catcher.exception.args, ("not json",))

    def testClientErrorWithoutType(self):
        with self.assertRaises(exceptions.ClientError) as catcher:
            self.connection._raise(self.FakeResponse(400, "{}"))
        self.assertEqual(catcher.exception.args, ({},))

    def testClientErrorWithUnknownType(self):
        with self.assertRaises(exceptions.ClientError) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.UnhandledException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.UnhandledException", "Message": "tralala"},))

    def testClientErrorWithoutJson(self):
        with self.assertRaises(exceptions.ClientError) as catcher:
            self.connection._raise(self.FakeResponse(400, "not json"))
        self.assertEqual(catcher.exception.args, ("not json",))

    def testResourceNotFoundException(self):
        with self.assertRaises(exceptions.ResourceNotFoundException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ResourceNotFoundException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ResourceNotFoundException", "Message": "tralala"},))

    def testValidationException(self):
        with self.assertRaises(exceptions.ValidationException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ValidationException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ValidationException", "Message": "tralala"},))


if __name__ == "__main__":  # pragma no branch (Test code)
    unittest.main()  # pragma no cover (Test code)
