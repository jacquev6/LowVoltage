# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action, ActionProxy
from .return_types import TableDescription_, _is_dict


class UpdateTable(Action):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html#API_UpdateTable_RequestParameters"""

    # @todo Create and delete secondary indexes!

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateTable.html#API_UpdateTable_ResponseElements"""
        def __init__(
            self,
            TableDescription=None,
            **dummy
        ):
            self.table_description = None
            if _is_dict(TableDescription):  # pragma no branch (Defensive code)
                self.table_description = TableDescription_(**TableDescription)

    def __init__(self, table_name):
        super(UpdateTable, self).__init__("UpdateTable")
        self.__table_name = table_name
        self.__read_capacity_units = None
        self.__write_capacity_units = None
        self.__gsis = {}

    def build(self):
        data = {"TableName": self.__table_name}
        throughput = {}
        if self.__read_capacity_units:
            throughput["ReadCapacityUnits"] = self.__read_capacity_units
        if self.__write_capacity_units:
            throughput["WriteCapacityUnits"] = self.__write_capacity_units
        if throughput:
            data["ProvisionedThroughput"] = throughput
        if self.__gsis:
            data["GlobalSecondaryIndexUpdates"] = [{"Update": i._build()} for i in self.__gsis.itervalues()]
        return data

    class _IndexWithThroughput(ActionProxy):
        def __init__(self, action, name):
            super(UpdateTable._IndexWithThroughput, self).__init__(action)
            self.__name = name
            self.__read_capacity_units = None
            self.__write_capacity_units = None

        def table(self):
            return self._action

        def provisioned_throughput(self, read_capacity_units, write_capacity_units):
            self.__read_capacity_units = read_capacity_units
            self.__write_capacity_units = write_capacity_units
            return self

        def _build(self):
            data = {"IndexName": self.__name}
            throughput = {}
            if self.__read_capacity_units:
                throughput["ReadCapacityUnits"] = self.__read_capacity_units
            if self.__write_capacity_units:
                throughput["WriteCapacityUnits"] = self.__write_capacity_units
            if throughput:
                data["ProvisionedThroughput"] = throughput
            return data

    def provisioned_throughput(self, read_capacity_units, write_capacity_units):
        self.__read_capacity_units = read_capacity_units
        self.__write_capacity_units = write_capacity_units
        return self

    def global_secondary_index(self, name):
        if name not in self.__gsis:
            self.__gsis[name] = self._IndexWithThroughput(self, name)
        return self.__gsis[name]


class UpdateTableUnitTests(_tst.UnitTests):
    def testName(self):
        self.assertEqual(UpdateTable("Foo").name, "UpdateTable")

    def testNoArguments(self):
        self.assertEqual(UpdateTable("Foo").build(), {"TableName": "Foo"})

    def testThroughput(self):
        self.assertEqual(
            UpdateTable("Foo").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43},
            }
        )

    def testGsi(self):
        self.assertEqual(
            UpdateTable("Foo").global_secondary_index("the_gsi").build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi"}},
                ],
            }
        )

    def testGsiprovisioned_Throughput(self):
        self.assertEqual(
            UpdateTable("Foo").global_secondary_index("the_gsi").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
                ],
            }
        )

    def testBackToGsiAfterBackToTable(self):
        self.assertEqual(
            UpdateTable("Foo").global_secondary_index("the_gsi").table().provisioned_throughput(12, 13).global_secondary_index("the_gsi").provisioned_throughput(42, 43).build(),
            {
                "TableName": "Foo",
                "GlobalSecondaryIndexUpdates": [
                    {"Update": {"IndexName": "the_gsi", "ProvisionedThroughput": {"ReadCapacityUnits": 42, "WriteCapacityUnits": 43}}},
                ],
                "ProvisionedThroughput": {"ReadCapacityUnits": 12, "WriteCapacityUnits": 13},
            }
        )


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

    def testThroughput(self):
        r = self.connection(
            _lv.UpdateTable("Aaa").provisioned_throughput(2, 4)
        )

        _tst.fix_table_description(r.table_description)

        with _tst.cover("r", r) as r:
            self.assertDateTimeIsReasonable(r.table_description.creation_date_time)
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_name, "h")
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_type, "S")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_name, "hh")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_type, "S")
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
            self.assertEqual(r.table_description.item_count, 0)
            self.assertEqual(r.table_description.key_schema[0].attribute_name, "h")
            self.assertEqual(r.table_description.key_schema[0].key_type, "HASH")
            self.assertEqual(r.table_description.local_secondary_indexes, None)
            self.assertEqual(r.table_description.provisioned_throughput.last_decrease_date_time, datetime.datetime(1970, 1, 1))
            self.assertEqual(r.table_description.provisioned_throughput.last_increase_date_time, datetime.datetime(1970, 1, 1))
            self.assertEqual(r.table_description.provisioned_throughput.number_of_decreases_today, 0)
            self.assertEqual(r.table_description.provisioned_throughput.read_capacity_units, 2)
            self.assertEqual(r.table_description.provisioned_throughput.write_capacity_units, 4)
            self.assertEqual(r.table_description.table_name, "Aaa")
            self.assertEqual(r.table_description.table_size_bytes, 0)
            self.assertEqual(r.table_description.table_status, "ACTIVE")

    def testGsiprovisioned_Throughput(self):
        r = self.connection(
            _lv.UpdateTable("Aaa").global_secondary_index("the_gsi").provisioned_throughput(6, 8)
        )

        _tst.fix_table_description(r.table_description)

        with _tst.cover("r", r) as r:
            self.assertDateTimeIsReasonable(r.table_description.creation_date_time)
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_name, "h")
            self.assertEqual(r.table_description.attribute_definitions[0].attribute_type, "S")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_name, "hh")
            self.assertEqual(r.table_description.attribute_definitions[1].attribute_type, "S")
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
            self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.read_capacity_units, 6)
            self.assertEqual(r.table_description.global_secondary_indexes[0].provisioned_throughput.write_capacity_units, 8)
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
