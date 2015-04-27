# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class UpdateTableLocalIntegTests(_tst.LocalIntegTests):
    def setUp(self):
        super(UpdateTableLocalIntegTests, self).setUp()
        self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
                .global_secondary_index("the_gsi")
                .hash_key("hh", _lv.STRING)
                .project_all()
                .provisioned_throughput(3, 4)
        )

    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        super(UpdateTableLocalIntegTests, self).tearDown()

    def test_provisioned_throughput(self):
        r = self.connection(
            _lv.UpdateTable("Aaa").provisioned_throughput(2, 4)
        )

        self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 2)
        self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 4)

    def test_update_gsi_provisioned_throughput(self):
        r = self.connection(
            _lv.UpdateTable("Aaa").update_global_secondary_index("the_gsi").provisioned_throughput(6, 8)
        )

        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 6)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 8)

    def test_delete_and_create_gsi(self):
        r = self.connection(_lv.UpdateTable("Aaa").delete_global_secondary_index("the_gsi"))
        self.assertEqual(r.table_description.global_secondary_indexes[0].index_status, "DELETING")

        _lv.wait_for_table_activation(self.connection, "Aaa")

        r = self.connection(_lv.DescribeTable("Aaa"))
        self.assertEqual(r.table.global_secondary_indexes, None)
        self.assertEqual(len(r.table.attribute_definitions), 1)  # The previous definition of attribute "hh" has disapeared.

        r = self.connection(
            _lv.UpdateTable("Aaa")
                .create_global_secondary_index("new_gsi")
                    .provisioned_throughput(1, 2)
                    .hash_key("nh", _lv.NUMBER)
                    .project_all()
        )

        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 1)
        self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 2)
        self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].attribute_name, "nh")
        self.assertEqual(r.table_description.global_secondary_indexes[0].key_schema[0].key_type, "HASH")
        self.assertEqual(len(r.table_description.attribute_definitions), 2)
