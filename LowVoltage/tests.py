# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import glob
import io
import os
import requests
import stat
import subprocess
import tarfile
import time
import unittest

from operations import DeleteItemTestCase, GetItemTestCase, PutItemTestCase, UpdateItemTestCase
from operations import BatchGetItemTestCase, BatchWriteItemTestCase
from connection import ConnectionTestCase

from LowVoltage import Connection, StaticCredentials, ValidationException, ResourceNotFoundException, ServerError, ConditionalCheckFailedException


class IntegrationTestsMixin:
    @classmethod
    def getTestTables(cls):
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

    def testConditionalCheckFailedException(self):
        with self.assertRaises(ConditionalCheckFailedException) as catcher:
            (self.connection
                .update_item("LowVoltage.TableWithHash", {"hash": "testUpdateItem"})
                .put("a", 42)
                .expect_not_null("foo")
                .return_values_all_new()
                .go())
        self.assertIn(
            catcher.exception.args,
            [
                ({  # DynamoDBLocal
                    "Message": "The conditional request failed",
                    "__type": "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
                },),
                ({  # Real DynamoDB
                    "message": "The conditional request failed",
                    "__type": "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
                },),
            ]
        )

    def testBatchGetItem(self):
        self.connection.put_item("LowVoltage.TableWithHash", {"hash": "testBatchGetItem", "a": 42, "b": "foo"}).go()
        get = (
            self.connection
                .batch_get_item()
                .table("LowVoltage.TableWithHash")
                .keys({"hash": "testBatchGetItem"})
                .attributes_to_get("a")
                .go()
        )
        self.assertEqual(
            get,
            {u'Responses': {u'LowVoltage.TableWithHash': [{u'a': {u'N': u'42'}}]}, u'UnprocessedKeys': {}}
        )

    def testBatchWriteItem(self):
        self.connection.put_item("LowVoltage.TableWithHash", {"hash": "testBatchWriteItem1", "a": 42, "b": "foo"}).go()
        write = (
            self.connection
                .batch_write_item()
                .table("LowVoltage.TableWithHash")
                .delete({"hash": "testBatchWriteItem1"})
                .put({"hash": "testBatchWriteItem2", "a": 43})
                .go()
        )
        self.assertEqual(
            write,
            {u'UnprocessedItems': {}}
        )
        self.assertEqual(
            self.connection.get_item("LowVoltage.TableWithHash", {"hash": "testBatchWriteItem1"}).go(),
            {}
        )
        self.assertEqual(
            self.connection.get_item("LowVoltage.TableWithHash", {"hash": "testBatchWriteItem2"}).go(),
            {
                "Item": {"hash": {"S": "testBatchWriteItem2"}, "a": {"N": "43"}}
            }
        )

    def testDeleteItem(self):
        self.connection.put_item("LowVoltage.TableWithHash", {"hash": "testDeleteItem"}).go()
        delete = (
            self.connection
                .delete_item("LowVoltage.TableWithHash", {"hash": "testDeleteItem"})
                .return_values_all_old()
                .go()
        )
        self.assertEqual(
            delete,
            {u'Attributes': {u'hash': {u'S': u'testDeleteItem'}}}
        )

    def testGetItem(self):
        self.connection.put_item("LowVoltage.TableWithHash", {"hash": "testGetItem", "a": 42, "b": "foo"}).go()
        get = (
            self.connection
                .get_item("LowVoltage.TableWithHash", {"hash": "testGetItem"})
                .attributes_to_get("a")
                .go()
        )
        self.assertEqual(
            get,
            {u'Item': {u'a': {u'N': u"42"}}}
        )

    def testPutItem(self):
        first_put = (
            self.connection
                .put_item("LowVoltage.TableWithHash", {"hash": "testPutItem", "a": 42, "b": "foo"})
                .return_values_all_old()
                .go()
        )
        self.assertEqual(
            first_put,
            {}
        )
        second_put = (
            self.connection
                .put_item("LowVoltage.TableWithHash", {"hash": "testPutItem", "a": 42, "c": "bar"})
                .return_values_all_old()
                .go()
        )
        self.assertEqual(
            second_put,
            {u'Attributes': {u'a': {u'N': u'42'}, u'b': {u'S': u'foo'}, u'hash': {u'S': u'testPutItem'}}}
        )

    def testUpdateItem(self):
        update = (
            self.connection
                .update_item("LowVoltage.TableWithHash", {"hash": "testUpdateItem"})
                .put("a", 42)
                .return_values_all_new()
                .go()
        )
        self.assertEqual(
            update,
            {u'Attributes': {u'a': {u'N': u'42'}, u'hash': {u'S': u'testUpdateItem'}}}
        )


class ExplorationTestsMixin:
    @classmethod
    def getTestTables(self):
        yield {
            "TableName": "LowVoltage.TableWithHash",
            "AttributeDefinitions": [{"AttributeName": "hash", "AttributeType": "S"}],
            "KeySchema":[{"AttributeName":"hash", "KeyType":"HASH"}],
            "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
        }

    def testExpressionsOnInt(self):
        self.assertEqual(
            self.connection.request(
                "PutItem",
                {
                    "TableName": "LowVoltage.TableWithHash",
                    "Item": {
                        "hash": {"S": "aaa"},
                        "a": {"N": "42"},
                        "b": {"N": "57"},
                    },
                }
            ),
            {}
        )

        self.assertEqual(
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.TableWithHash",
                    "Key": {"hash": {"S": "aaa",}},
                    "ConditionExpression": "(a=:old_a AND b<:max_b) OR (a=:new_a AND b<:max_b)",
                    "UpdateExpression": "SET a=:new_a ADD b :incr_b",
                    "ExpressionAttributeValues": {":old_a": {"N": "42"}, ":new_a": {"N": "43"}, ":max_b": {"N": "60"}, ":incr_b": {"N": "2"}},
                    "ReturnValues": "ALL_NEW",
                }
            ),
            {
                "Attributes": {
                    "hash": {"S": "aaa"},
                    "a": {"N": "43"},
                    "b": {"N": "59"},
                },
            }
        )

        self.assertEqual(
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.TableWithHash",
                    "Key": {"hash": {"S": "aaa",}},
                    "ConditionExpression": "(a=:old_a OR a=:new_a) AND b<:max_b",
                    "UpdateExpression": "SET a=:new_a ADD b :incr_b",
                    "ExpressionAttributeValues": {":old_a": {"N": "42"}, ":new_a": {"N": "43"}, ":max_b": {"N": "60"}, ":incr_b": {"N": "2"}},
                    "ReturnValues": "ALL_NEW",
                }
            ),
            {
                "Attributes": {
                    "hash": {"S": "aaa"},
                    "a": {"N": "43"},
                    "b": {"N": "61"},
                },
            }
        )

        self.assertEqual(
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.TableWithHash",
                    "Key": {"hash": {"S": "aaa",}},
                    "ConditionExpression": "a<b",
                    "UpdateExpression": "SET a=:new_a",
                    "ExpressionAttributeValues": {":new_a": {"N": "44"}},
                    "ReturnValues": "ALL_NEW",
                }
            ),
            {
                "Attributes": {
                    "hash": {"S": "aaa"},
                    "a": {"N": "44"},
                    "b": {"N": "61"},
                },
            }
        )

        with self.assertRaises(ConditionalCheckFailedException):
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.TableWithHash",
                    "Key": {"hash": {"S": "aaa",}},
                    "ConditionExpression": "a>b",
                    "UpdateExpression": "REMOVE a",
                }
            )

    def testValidationErrors(self):
        with self.assertRaises(ValidationException) as catcher:
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.TableWithHash",
                    "Key": {"hash": {"S": "bbb"}},
                    "ConditionExpression": "foo",
                }
            )
        self.assertIn(
            catcher.exception.args,
            [
                ({  # DynamoDBLocal
                    "Message": 'Invalid ConditionExpression: Syntax error; token: "<EOF>", near: "foo"',
                    "__type": "com.amazon.coral.validate#ValidationException",
                },),
                ({  # Real DynamoDB
                    "message": 'Invalid ConditionExpression: Syntax error; token: "<EOF>", near: "foo"',
                    "__type": "com.amazon.coral.validate#ValidationException",
                },),
            ]
        )

        with self.assertRaises(ValidationException) as catcher:
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.TableWithHash",
                    "Key": {"hash": {"S": "bbb"}},
                    "ConditionExpression": "foo=:foo",
                }
            )
        self.assertIn(
            catcher.exception.args,
            [
                ({  # DynamoDBLocal
                    "Message": 'Invalid ConditionExpression: An expression attribute value used in expression is not defined; attribute value: :foo',
                    "__type": "com.amazon.coral.validate#ValidationException",
                },),
                ({  # Real DynamoDB
                    "message": 'Invalid ConditionExpression: An expression attribute value used in expression is not defined; attribute value: :foo',
                    "__type": "com.amazon.coral.validate#ValidationException",
                },),
            ]
        )


class TestsMixin:
    @classmethod
    def createTestTables(cls):
        for payload in cls.getTestTables():
            cls.connection.request("CreateTable", payload)
        sleep = True
        while sleep:
            sleep = False
            for payload in cls.getTestTables():
                name = payload["TableName"]
                status = cls.connection.request("DescribeTable", {"TableName": name})["Table"]["TableStatus"]
                if status == "CREATING":
                    sleep = True
            if sleep:
                time.sleep(1)

    @classmethod
    def deleteTestTables(cls):
        for payload in cls.getTestTables():
            name = payload["TableName"]
            cls.connection.request("DeleteTable", {"TableName": name})


try:
    import AwsCredentials

    class RealTestsMixin(TestsMixin):
        @classmethod
        def setUpClass(cls):
            cls.connection = Connection(AwsCredentials.region, StaticCredentials(AwsCredentials.key, AwsCredentials.secret))
            cls.createTestTables()

        @classmethod
        def tearDownClass(cls):
            cls.deleteTestTables()


    class RealIntegrationTestCase(IntegrationTestsMixin, RealTestsMixin, unittest.TestCase):
        pass


    class RealExplorationTestCase(ExplorationTestsMixin, RealTestsMixin, unittest.TestCase):
        pass

except ImportError:  # pragma no cover
    pass  # pragma no cover


class LocalTestsMixin(TestsMixin):
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


class LocalIntegrationTestCase(IntegrationTestsMixin, LocalTestsMixin, unittest.TestCase):
    pass


class LocalExplorationTestCase(ExplorationTestsMixin, LocalTestsMixin, unittest.TestCase):
    pass


if __name__ == "__main__":  # pragma no branch (Test code)
    unittest.main()  # pragma no cover (Test code)
