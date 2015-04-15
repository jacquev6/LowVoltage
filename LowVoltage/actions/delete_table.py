# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime
import unittest

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .return_types import TableDescription_, _is_dict


class DeleteTable(Action):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#API_DeleteTable_RequestParameters"""

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#API_DeleteTable_ResponseElements"""

        def __init__(
            self,
            TableDescription=None,
            **dummy
        ):
            self.table_description = None
            if _is_dict(TableDescription):  # pragma no branch (Defensive code)
                self.table_description = TableDescription_(**TableDescription)

    def __init__(self, table_name):
        super(DeleteTable, self).__init__("DeleteTable")
        self.__table_name = table_name

    def build(self):
        return {"TableName": self.__table_name}


class DeleteTableUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(DeleteTable("Foo").name, "DeleteTable")

    def testBuild(self):
        self.assertEqual(DeleteTable("Foo").build(), {"TableName": "Foo"})


class DeleteTableIntegTests(_tst.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )

    def test(self):
        r = self.connection.request(_lv.DeleteTable("Aaa"))

        with _tst.cover("r", r) as r:
            self.assertDateTimeIsReasonable(r.table_description.creation_date_time)
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_name, "h")
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_type, "S")
            self.assertEqual(r.table_description.global_secondary_indexes, None)
            self.assertEqual(r.table_description.item_count, 0)
            self.assertEqual(r.table_description.key_schema[0].attribute_name, "h")
            self.assertEqual(r.table_description.key_schema[0].key_type, "HASH")
            self.assertEqual(r.table_description.local_secondary_indexes, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, datetime.datetime(1970, 1, 1))
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, datetime.datetime(1970, 1, 1))
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 1)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 2)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")
