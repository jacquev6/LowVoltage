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

from operations import PutItemTestCase, UpdateItemTestCase
from connection import ConnectionTestCase

from LowVoltage import Connection, StaticCredentials, ValidationException, ResourceNotFoundException, ServerError


class IntegrationTestsMixin:
    @classmethod
    def createTestTables(cls):  # pragma no cover (Covered by optional integ tests)
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
    def deleteTestTables(cls):  # pragma no cover (Covered by optional integ tests)
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

    def testPutItem(self):
        first_put = (
            self.connection
                .put_item("LowVoltage.TableWithHash", {"hash": "testPutItem", "a": 42, "b": "foo"})
                .return_all_old_values()
                .go()
        )
        self.assertEqual(
            first_put,
            {}
        )
        second_put = (
            self.connection
                .put_item("LowVoltage.TableWithHash", {"hash": "testPutItem", "a": 42, "c": "bar"})
                .return_all_old_values()
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
                .return_all_new_values()
                .go()
        )
        self.assertEqual(
            update,
            {u'Attributes': {u'a': {u'N': u'42'}, u'hash': {u'S': u'testUpdateItem'}}}
        )


try:
    import AwsCredentials

    class RealIntegrationTestCase(IntegrationTestsMixin, unittest.TestCase):  # pragma no cover (Covered by optional integ tests)
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


if __name__ == "__main__":  # pragma no branch (Test code)
    unittest.main()  # pragma no cover (Test code)
