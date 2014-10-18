# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage import *
import LowVoltage.tests.dynamodb_local


class ExplorationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.connection = Connection("us-west-2", StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/")
        cls.connection.request(
            "CreateTable",
            {
                "TableName": "LowVoltage.ExplorationTests",
                "AttributeDefinitions": [{"AttributeName": "hash", "AttributeType": "S"}],
                "KeySchema":[{"AttributeName":"hash", "KeyType":"HASH"}],
                "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
            }
        )

    @classmethod
    def tearDownClass(cls):
        cls.connection.request(
            "DeleteTable",
            {
                "TableName": "LowVoltage.ExplorationTests",
            }
        )

    def testExpressionsOnInt(self):
        self.assertEqual(
            self.connection.request(
                "PutItem",
                {
                    "TableName": "LowVoltage.ExplorationTests",
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
                    "TableName": "LowVoltage.ExplorationTests",
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
                    "TableName": "LowVoltage.ExplorationTests",
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
                    "TableName": "LowVoltage.ExplorationTests",
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
                    "TableName": "LowVoltage.ExplorationTests",
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
                    "TableName": "LowVoltage.ExplorationTests",
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
                    "TableName": "LowVoltage.ExplorationTests",
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

        with self.assertRaises(ValidationException) as catcher:
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.ExplorationTests",
                    "Key": {"hash": {"S": "aaa",}},
                    "ConditionExpression": "a+:delta<b",
                    "UpdateExpression": "SET a=:new_a",
                    "ExpressionAttributeValues": {":new_a": {"N": "45"}, ":delta": {"N": "3"}},
                }
            )
        self.assertIn(
            catcher.exception.args,
            [
                ({  # DynamoDBLocal
                    "Message": 'Invalid ConditionExpression: Syntax error; token: "+", near: "a+:delta"',
                    "__type": "com.amazon.coral.validate#ValidationException",
                },),
                ({  # Real DynamoDB
                    "message": 'Invalid ConditionExpression: Syntax error; token: "+", near: "a+:delta"',
                    "__type": "com.amazon.coral.validate#ValidationException",
                },),
            ]
        )

    def testConditionExpressionFunctions(self):
        with self.assertRaises(ConditionalCheckFailedException):
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.ExplorationTests",
                    "Key": {"hash": {"S": "ccc"}},
                    "ConditionExpression": "attribute_exists(a)",
                    "UpdateExpression": "SET a=:new_a",
                    "ExpressionAttributeValues": {":new_a": {"N": "42"}},
                }
            )

        with self.assertRaises(ConditionalCheckFailedException):
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.ExplorationTests",
                    "Key": {"hash": {"S": "ccc"}},
                    "ConditionExpression": "contains(b, :b)",
                    "UpdateExpression": "SET a=:new_a",
                    "ExpressionAttributeValues": {":new_a": {"N": "42"}, ":b": {"N": "42"}},
                }
            )

        with self.assertRaises(ConditionalCheckFailedException):
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.ExplorationTests",
                    "Key": {"hash": {"S": "ccc"}},
                    "ConditionExpression": "begins_with(b, :b)",
                    "UpdateExpression": "SET a=:new_a",
                    "ExpressionAttributeValues": {":new_a": {"N": "42"}, ":b": {"S": "prefix"}},
                }
            )

        with self.assertRaises(ConditionalCheckFailedException):
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.ExplorationTests",
                    "Key": {"hash": {"S": "ccc"}},
                    "ConditionExpression": "b IN (:b1, :b2, :b3)",
                    "UpdateExpression": "SET a=:new_a",
                    "ExpressionAttributeValues": {":new_a": {"N": "42"}, ":b1": {"N": "42"}, ":b2": {"N": "43"}, ":b3": {"N": "44"}},
                }
            )

        with self.assertRaises(ConditionalCheckFailedException):
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.ExplorationTests",
                    "Key": {"hash": {"S": "ccc"}},
                    "ConditionExpression": "b BETWEEN :b1 AND :b2",
                    "UpdateExpression": "SET a=:new_a",
                    "ExpressionAttributeValues": {":new_a": {"N": "42"}, ":b1": {"N": "42"}, ":b2": {"N": "44"}},
                }
            )

        with self.assertRaises(ConditionalCheckFailedException):
            self.connection.request(
                "UpdateItem",
                {
                    "TableName": "LowVoltage.ExplorationTests",
                    "Key": {"hash": {"S": "ccc"}},
                    "ConditionExpression": "#n1=:v",
                    "UpdateExpression": "SET #n2=:v",
                    "ExpressionAttributeValues": {":v": {"N": "42"}},
                    "ExpressionAttributeNames": {"#n1": "aaa", "#n2": "bbb"},
                }
            )


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
