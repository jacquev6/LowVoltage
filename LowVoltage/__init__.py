# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import base64
import datetime
import glob
import hashlib
import hmac
import io
import json
import numbers
import os
import stat
import subprocess
import sys
import tarfile
import time
import unittest
import urlparse

import requests


class Error(Exception):
    pass


class UnknownError(Error):
    pass


class ServerError(Error):
    pass


class ClientError(Error):
    pass


class ResourceNotFoundException(ClientError):
    pass


class ValidationException(ClientError):
    pass


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
            self.__endpoint = "https://dynamodb.{}.amazonaws.com/".format(region)
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
                raise ClientError(data)
            elif typ.endswith("ResourceNotFoundException"):
                raise ResourceNotFoundException(data)
            elif typ.endswith("ValidationException"):
                raise ValidationException(data)
            else:
                raise ClientError(data)
        elif r.status_code == 500:
            raise ServerError(data)
        else:
            raise UnknownError(r.status_code, r.text)

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
            hashlib.sha256(payload).hexdigest(),
        )
        credentials = "{}/{}/dynamodb/aws4_request".format(
            datestamp,
            self.__region,
        )
        to_sign = "AWS4-HMAC-SHA256\n{}\n{}\n{}".format(
            timestamp,
            credentials,
            hashlib.sha256(request).hexdigest(),
        )

        aws_key, aws_secret = self.__credentials.get()

        key = hmac.new(
            hmac.new(
                hmac.new(
                    hmac.new(
                        "AWS4{}".format(aws_secret),
                        datestamp,
                        hashlib.sha256
                    ).digest(),
                    self.__region,
                    hashlib.sha256
                ).digest(),
                "dynamodb",
                hashlib.sha256
            ).digest(),
            "aws4_request",
            hashlib.sha256
        ).digest()

        headers["Authorization"] = "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}".format(
            aws_key,
            credentials,
            header_names,
            hmac.new(key, to_sign, hashlib.sha256).hexdigest(),
        )

        return headers, payload


class Operation(object):
    def __init__(self, operation, connection):
        self.__operation = operation
        self.__connection = connection

    def go(self):
        return self.__connection.request(self.__operation, self._build())

    def _convert_dict(self, attributes):
        return {
            key: self._convert_value(val)
            for key, val in attributes.iteritems()
        }

    def _convert_value(self, value):
        if isinstance(value, basestring):
            return {"S": value}
        elif isinstance(value, numbers.Integral):
            return {"N": str(value)}
        else:
            assert len(value) > 0
            if isinstance(value[0], basestring):
                return {"SS": value}
            elif isinstance(value[0], numbers.Integral):
                return {"NS": [str(n) for n in value]}
            else:
                assert False  # pragma no cover


class UpdateItem(Operation):
    def __init__(self, connection, table_name, key):
        super(UpdateItem, self).__init__("UpdateItem", connection)
        self.__table_name = table_name
        self.__key = key
        self.__attribute_updates = {}
        self.__conditional_operator = None
        self.__return_values = None
        self.__expected = {}

    def _build(self):
        data = {
            "TableName": self.__table_name,
            "Key": self._convert_dict(self.__key),
        }
        if self.__attribute_updates:
            data["AttributeUpdates"] = self.__attribute_updates
        if self.__conditional_operator:
            data["ConditionalOperator"] = self.__conditional_operator
        if self.__return_values:
            data["ReturnValues"] = self.__return_values
        if self.__expected:
            data["Expected"] = self.__expected
        return data

    def put(self, name, value):
        self.__attribute_updates[name] = {"Action": "PUT", "Value": self._convert_value(value)}
        return self

    def delete(self, name, value=None):
        self.__attribute_updates[name] = {"Action": "DELETE"}
        if value is not None:
            self.__attribute_updates[name]["Value"] = self._convert_value(value)
        return self

    def add(self, name, value):
        self.__attribute_updates[name] = {"Action": "ADD", "Value": self._convert_value(value)}
        return self

    def return_all_new_values(self):
        self.__return_values = "ALL_NEW"
        return self

    def return_updated_new_values(self):
        self.__return_values = "UPDATED_NEW"
        return self

    def return_all_old_values(self):
        self.__return_values = "ALL_OLD"
        return self

    def return_updated_old_values(self):
        self.__return_values = "UPDATED_OLD"
        return self

    def return_no_values(self):
        self.__return_values = "NONE"
        return self

    def conditional_operator(self, operator):
        self.__conditional_operator = operator
        return self

    def expect_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "EQ", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_not_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "NE", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_less_than_or_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "LE", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_less_than(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "LT", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_greater_than_or_equal(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "GE", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_greater_than(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "GT", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_not_null(self, name):
        self.__expected[name] = {"ComparisonOperator": "NOT_NULL"}
        return self

    def expect_null(self, name):
        self.__expected[name] = {"ComparisonOperator": "NULL"}
        return self

    def expect_contains(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "CONTAINS", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_not_contains(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "NOT_CONTAINS", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_begins_with(self, name, value):
        self.__expected[name] = {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [self._convert_value(value)]}
        return self

    def expect_in(self, name, values):
        self.__expected[name] = {"ComparisonOperator": "IN", "AttributeValueList": [self._convert_value(value) for value in values]}
        return self

    def expect_between(self, name, low, high):
        self.__expected[name] = {"ComparisonOperator": "BETWEEN", "AttributeValueList": [self._convert_value(low), self._convert_value(high)]}
        return self


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
        with self.assertRaises(UnknownError) as catcher:
            self.connection._raise(self.FakeResponse(999, "{}"))
        self.assertEqual(catcher.exception.args, (999, "{}"))

    def testUnknownErrorWithoutJson(self):
        with self.assertRaises(UnknownError) as catcher:
            self.connection._raise(self.FakeResponse(999, "not json"))
        self.assertEqual(catcher.exception.args, (999, "not json"))

    def testServerError(self):
        with self.assertRaises(ServerError) as catcher:
            self.connection._raise(self.FakeResponse(500, '{"foo": "bar"}'))
        self.assertEqual(catcher.exception.args, ({"foo": "bar"},))

    def testServerErrorWithoutJson(self):
        with self.assertRaises(ServerError) as catcher:
            self.connection._raise(self.FakeResponse(500, "not json"))
        self.assertEqual(catcher.exception.args, ("not json",))

    def testClientErrorWithoutType(self):
        with self.assertRaises(ClientError) as catcher:
            self.connection._raise(self.FakeResponse(400, "{}"))
        self.assertEqual(catcher.exception.args, ({},))

    def testClientErrorWithUnknownType(self):
        with self.assertRaises(ClientError) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.UnhandledException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.UnhandledException", "Message": "tralala"},))

    def testClientErrorWithoutJson(self):
        with self.assertRaises(ClientError) as catcher:
            self.connection._raise(self.FakeResponse(400, "not json"))
        self.assertEqual(catcher.exception.args, ("not json",))

    def testResourceNotFoundException(self):
        with self.assertRaises(ResourceNotFoundException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ResourceNotFoundException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ResourceNotFoundException", "Message": "tralala"},))

    def testValidationException(self):
        with self.assertRaises(ValidationException) as catcher:
            self.connection._raise(self.FakeResponse(400, '{"__type": "xxx.ValidationException", "Message": "tralala"}'))
        self.assertEqual(catcher.exception.args, ({"__type": "xxx.ValidationException", "Message": "tralala"},))


class UpdateItemTestCase(unittest.TestCase):
    def testStringKey(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "value"})._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "value"}},
            }
        )

    def testIntKey(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": 42})._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def testPutInt(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).put("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "PUT", "Value": {"N": "42"}}},
            }
        )

    def testDelete(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).delete("attr")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "DELETE"}},
            }
        )

    def testAddInt(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).add("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "ADD", "Value": {"N": "42"}}},
            }
        )

    def testDeleteSetOfInts(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).delete("attr", [42, 43])._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "DELETE", "Value": {"NS": ["42", "43"]}}},
            }
        )

    def testAddSetOfStrings(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).add("attr", ["42", "43"])._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "AttributeUpdates": {"attr": {"Action": "ADD", "Value": {"SS": ["42", "43"]}}},
            }
        )

    def testReturnAllNewValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_all_new_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_NEW",
            }
        )

    def testReturnUpdatedNewValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_updated_new_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_NEW",
            }
        )

    def testReturnAllOldValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_all_old_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def testReturnUpdatedOldValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_updated_old_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_OLD",
            }
        )

    def testReturnNoValues(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).return_no_values()._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testConditionalOperator(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).conditional_operator("AND")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ConditionalOperator": "AND",
            }
        )

    def testExpectEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_not_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThanOrEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_less_than_or_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectLessThan(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_less_than("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThanOrEqual(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_greater_than_or_equal("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectGreaterThan(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_greater_than("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "GT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotNull(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_not_null("attr")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_NULL"}},
            }
        )

    def testExpectNull(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_null("attr")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NULL"}},
            }
        )

    def testExpectContains(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_contains("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectNotContains(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_not_contains("attr", 42)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "NOT_CONTAINS", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testExpectBeginsWith(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_begins_with("attr", "prefix")._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [{"S": "prefix"}]}},
            }
        )

    def testExpectIn(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_in("attr", [42, 43])._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "IN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )

    def testExpectBetween(self):
        self.assertEqual(
            UpdateItem(None, "Table", {"hash": "h"}).expect_between("attr", 42, 43)._build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "Expected": {"attr": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "42"}, {"N": "43"}]}},
            }
        )


class IntegrationTestsMixin:
    @classmethod
    def createTestTables(cls):
        for payload in cls.__getTestTables():
            cls.connection.request("CreateTable", payload)
        sleep = True
        while sleep:
            sleep = False
            for payload in cls.__getTestTables():
                name = payload["TableName"]
                status = cls.connection.request("DescribeTable", {"TableName": name})["Table"]["TableStatus"]
                if status == "CREATING":
                    sleep = True
            if sleep:
                time.sleep(1)

    @classmethod
    def deleteTestTables(cls):
        for payload in cls.__getTestTables():
            name = payload["TableName"]
            cls.connection.request("DeleteTable", {"TableName": name})

    @classmethod
    def __getTestTables(cls):
        yield {
            "TableName": "LowVoltage.TableWithHash",
            "AttributeDefinitions": [{"AttributeName": "hash", "AttributeType": "S"}],
            "KeySchema":[{"AttributeName":"hash", "KeyType":"HASH"}],
            "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        }

    def testResourceNotFoundException(self):
        with self.assertRaises(ResourceNotFoundException) as catcher:
            self.connection.request(
                "DescribeTable",
                {"TableName": "UnexistingTable"}
            )
        self.assertIn(
            catcher.exception.args,
            [
                ({  # DynamoDBLocal
                    "Message": "Cannot do operations on a non-existent table",
                    "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
                },),
                ({  # Real DynamoDB
                    "message": "Requested resource not found: Table: UnexistingTable not found",
                    "__type": "com.amazonaws.dynamodb.v20120810#ResourceNotFoundException",
                },),
            ]
        )

    def testValidationException(self):
        with self.assertRaises(ValidationException) as catcher:
            self.connection.request(
                "PutItem",
                {"TableName": "LowVoltage.TableWithHash"}
            )
        self.assertIn(
            catcher.exception.args,
            [
                ({  # DynamoDBLocal
                    "Message": "Item to put in PutItem cannot be null",
                    "__type": "com.amazon.coral.validate#ValidationException",
                },),
                ({  # Real DynamoDB
                    "message": "1 validation error detected: Value null at 'item' failed to satisfy constraint: Member must not be null",
                    "__type": "com.amazon.coral.validate#ValidationException",
                },),
            ]
        )

    def testUpdateItem(self):
        update = (
            self.connection
                .update_item("LowVoltage.TableWithHash", {"hash": "testUpdateItem"})
                .put("a", 42)
                .return_all_new_values()
                .go()
        )
        self.assertEqual(
            update,
            {u'Attributes': {u'a': {u'N': u'42'}, u'hash': {u'S': u'testUpdateItem'}}}
        )


try:
    import AwsCredentials

    class RealIntegrationTestCase(IntegrationTestsMixin, unittest.TestCase):
        @classmethod
        def setUpClass(cls):
            cls.connection = Connection(AwsCredentials.region, StaticCredentials(AwsCredentials.key, AwsCredentials.secret))
            cls.createTestTables()

        @classmethod
        def tearDownClass(cls):
            cls.deleteTestTables()

except ImportError:  # pragma no cover
    pass  # pragma no cover


class LocalIntegrationTestCase(IntegrationTestsMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.exists(".dynamodblocal/DynamoDBLocal.jar"):
            archive = requests.get("http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest").content  # pragma no cover
            tarfile.open(fileobj=io.BytesIO(archive)).extractall(".dynamodblocal")  # pragma no cover
            # Fix permissions, needed at least when running with Cygwin's Python
            for f in glob.glob(".dynamodblocal/DynamoDBLocal_lib/*"):  # pragma no cover
                os.chmod(f, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR)  # pragma no cover

        cls.dynamodblocal = subprocess.Popen(
            ["java", "-Djava.library.path=./DynamoDBLocal_lib", "-jar", "DynamoDBLocal.jar", "-inMemory", "-port", "65432"],
            cwd=".dynamodblocal",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        time.sleep(1)
        assert cls.dynamodblocal.poll() is None

        cls.connection = Connection("us-west-2", StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/")
        cls.createTestTables()

    @classmethod
    def tearDownClass(cls):
        cls.dynamodblocal.kill()

    def testServerError(self):
        # DynamoDBLocal is not as robust as the real one. This is useful for our test coverage :)
        with self.assertRaises(ServerError) as catcher:
            self.connection.request(
                "PutItem",
                {
                    "TableName": "TableWithHash",
                    "Item": {"hash": 42}
                }
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "Message": "The request processing has failed because of an unknown error, exception or failure.",
                "__type": "com.amazonaws.dynamodb.v20120810#InternalFailure",
            },)
        )


if __name__ == "__main__":  # pragma no branch
    unittest.main()
