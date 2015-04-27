# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime

import LowVoltage as _lv
import LowVoltage.testing as _tst


class CreateTableLocalIntegTests(_tst.LocalIntegTests):
    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        super(CreateTableLocalIntegTests, self).tearDown()

    def test_simplest_table(self):
        r = self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )

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

    def test_simple_global_secondary_index(self):
        r = self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _lv.STRING)
                .project_all()
                .provisioned_throughput(3, 4)
        )

        self.assertEqual(r.table_description.global_secondary_indexes[0].index_name, "the_gsi")
        self.assertEqual(r.table_description.global_secondary_indexes[0].index_size_bytes, 0)
        self.assertEqual(r.table_description.global_secondary_indexes[0].index_status, "ACTIVE")
        self.assertEqual(r.table_description.global_secondary_indexes[0].item_count, 0)
        self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].attribute_name, "hh")
        self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].key_type, "HASH")
        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.non_key_attributes, None)
        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.projection_type, "ALL")
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.last_decrease_date_time, None)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.last_increase_date_time, None)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.number_of_decreases_today, None)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 3)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 4)

    def test_simple_local_secondary_index(self):
        r = self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).range_key("r", _lv.STRING).provisioned_throughput(1, 2)
                .local_secondary_index("the_lsi").hash_key("h").range_key("rr", _lv.STRING).project_all()
        )

        self.assertEqual(r.table_description.local_secondary_indexes[0].index_name, "the_lsi")
        self.assertEqual(r.table_description.local_secondary_indexes[0].index_size_bytes, 0)
        self.assertEqual(r.table_description.local_secondary_indexes[0].item_count, 0)
        self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[0].attribute_name, "h")
        self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[0].key_type, "HASH")
        self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[1].attribute_name, "rr")
        self.assertEqual(r.table_description.local_secondary_indexes[0].key_schema[1].key_type, "RANGE")
        self.assertEqual(r.table_description.local_secondary_indexes[0].projection.non_key_attributes, None)
        self.assertEqual(r.table_description.local_secondary_indexes[0].projection.projection_type, "ALL")

    def test_global_secondary_index_with_projection(self):
        r = self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _lv.STRING)
                .project("toto", "titi")
                .provisioned_throughput(3, 4)
        )

        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.non_key_attributes[0], "toto")
        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.non_key_attributes[1], "titi")
        self.assertEqual(r.table_description.global_secondary_indexes[0].projection.projection_type, "INCLUDE")


class CreateTableErrorLocalIntegTests(_tst.LocalIntegTests):
    def test_define_unused_attribute(self):
        with self.assertRaises(_lv.ValidationException) as catcher:
            self.connection(
                _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                    .attribute_definition("x", _lv.STRING)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "The number of attributes in key schema must match the number of attributesdefined in attribute definitions.",
            },)
        )

    def test_dont_define_key_attribute(self):
        with self.assertRaises(_lv.ValidationException) as catcher:
            self.connection(
                _lv.CreateTable("Aaa").hash_key("h").provisioned_throughput(1, 2)
                    .attribute_definition("x", _lv.STRING)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "Hash Key not specified in Attribute Definitions.  Type unknown.",
            },)
        )

    def test_dont_define_any_attribute(self):
        with self.assertRaises(_lv.ValidationException) as catcher:
            self.connection(
                _lv.CreateTable("Aaa").hash_key("h").provisioned_throughput(1, 2)
            )
        self.assertEqual(
            catcher.exception.args,
            ({
                "__type": "com.amazon.coral.validate#ValidationException",
                "Message": "No Attribute Schema Defined",
            },)
        )
