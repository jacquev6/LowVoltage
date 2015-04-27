# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime

import LowVoltage as _lv
import LowVoltage.testing as _tst


class DescribeTableLocalIntegTests(_tst.LocalIntegTests):
    def setUp(self):
        super(DescribeTableLocalIntegTests, self).setUp()
        self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        super(DescribeTableLocalIntegTests, self).tearDown()

    def test(self):
        r = self.connection(_lv.DescribeTable("Aaa"))

        self.assertDateTimeIsReasonable(r.table.creation_date_time)
        self.assertEqual(r.table.attribute_definitions[0].attribute_name, "h")
        self.assertEqual(r.table.attribute_definitions[0].attribute_type, "S")
        self.assertEqual(r.table.global_secondary_indexes, None)
        self.assertEqual(r.table.item_count, 0)
        self.assertEqual(r.table.key_schema[0].attribute_name, "h")
        self.assertEqual(r.table.key_schema[0].key_type, "HASH")
        self.assertEqual(r.table.local_secondary_indexes, None)
        self.assertEqual(r.table.provisioned_throughput.last_decrease_date_time, datetime.datetime(1970, 1, 1))
        self.assertEqual(r.table.provisioned_throughput.last_increase_date_time, datetime.datetime(1970, 1, 1))
        self.assertEqual(r.table.provisioned_throughput.number_of_decreases_today, 0)
        self.assertEqual(r.table.provisioned_throughput.read_capacity_units, 1)
        self.assertEqual(r.table.provisioned_throughput.write_capacity_units, 2)
        self.assertEqual(r.table.table_name, "Aaa")
        self.assertEqual(r.table.table_size_bytes, 0)
        self.assertEqual(r.table.table_status, "ACTIVE")
