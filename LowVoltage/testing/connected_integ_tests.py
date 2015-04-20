# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime

import testresources

import LowVoltage as _lv


_execution_date = datetime.datetime.now()


class DynamoDbResourceManager(testresources.TestResourceManager):
    def make(self, dependencies):
        connection = _lv.make_connection("eu-west-1", _lv.EnvironmentCredentials())

        table = _execution_date.strftime("LowVoltage.IntegTests.%Y-%m-%d.%H-%M-%S.%f")

        connection.request(
            _lv.CreateTable(table).hash_key("tab_h", _lv.STRING).range_key("tab_r", _lv.NUMBER).provisioned_throughput(1, 1)
                .global_secondary_index("gsi").hash_key("gsi_h", _lv.STRING).range_key("gsi_r", _lv.NUMBER).project_all().provisioned_throughput(1, 1)
                .local_secondary_index("lsi").hash_key("tab_h").range_key("lsi_r", _lv.NUMBER).project_all().provisioned_throughput(1, 1)
        )
        _lv.WaitForTableActivation(connection, table)

        return table

    def clean(self, table):
        connection = _lv.make_connection("eu-west-1", _lv.EnvironmentCredentials())
        connection.request(_lv.DeleteTable(table))


class ConnectedIntegTests(testresources.ResourcedTestCase):
    # Create an IAM user, populate the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
    # Use the following IAM policy to limit access to specific tables:
    # {
    #     "Version": "2012-10-17",
    #     "Statement": [
    #         {
    #             "Action": ["dynamodb:*"],
    #             "Effect": "Allow",
    #             "Resource": "arn:aws:dynamodb:eu-west-1:284086980038:table/LowVoltage.IntegTests.*"
    #         }
    #     ]
    # }
    # @todo Could we refine the policy to ensure the total provisioned throughput is capped?

    def setUp(self):
        super(ConnectedIntegTests, self).setUp()
        self.connection = _lv.make_connection("eu-west-1", _lv.EnvironmentCredentials())

    @classmethod
    def make_table_name(cls):
        assert cls.__name__.endswith("ConnectedIntegTests")
        return _execution_date.strftime("LowVoltage.IntegTests.%Y-%m-%d.%H-%M-%S.%f.{}".format(cls.__name__[:19]))


class ConnectedIntegTestsWithTable(ConnectedIntegTests):
    resources = [("table", DynamoDbResourceManager())]

    item = {"tab_h": u"0", "tab_r": 0, "gsi_h": u"1", "gsi_r": 1, "lsi_r": 2}
    tab_key = {"tab_h": u"0", "tab_r": 0}
    gsi_key = {"gsi_h": u"1", "gsi_r": 1}
    lsi_key = {"tab_h": u"0", "lsi_r": 2}
