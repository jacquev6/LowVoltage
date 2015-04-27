# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class UpdateItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_set(self):
        r = self.connection(
            _lv.UpdateItem("Aaa", {"h": u"set"})
                .set("a", ":v")
                .set("#p", ":w")
                .expression_attribute_value("v", "aaa")
                .expression_attribute_value("w", "bbb")
                .expression_attribute_name("p", "b")
        )

        self.assertEqual(
            self.connection(_lv.GetItem("Aaa", {"h": u"set"})).item,
            {"h": "set", "a": "aaa", "b": "bbb"}
        )

    def test_complex_update(self):
        self.connection(
            _lv.PutItem(
                "Aaa",
                {
                    "h": u"complex",
                    "a": "a",
                    "b": "b",
                    "c": "c",
                    "d": set([41, 43]),
                    "e": 42,
                    "f": set([41, 42, 43]),
                    "g": set([39, 40]),
                }
            )
        )

        r = self.connection(
            _lv.UpdateItem("Aaa", {"h": u"complex"})
                .set("a", ":s")
                .set("b", ":i")
                .remove("c")
                .add("d", "s")
                .add("e", "i")
                .delete("f", "s")
                .delete("g", "s")
                .expression_attribute_value("s", set([42, 43]))
                .expression_attribute_value("i", 52)
                .return_values_all_new()
        )

        self.assertEqual(
            r.attributes,
            {
                "h": u"complex",
                "a": set([42, 43]),
                "b": 52,
                "d": set([41, 42, 43]),
                "e": 94,
                "f": set([41]),
                "g": set([39, 40]),
            }
        )

    def test_condition_expression(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"expr", "a": 42, "b": 42}))

        r = self.connection(
            _lv.UpdateItem("Aaa", {"h": u"expr"})
                .set("checked", ":true")
                .expression_attribute_value("true", True)
                .condition_expression("a=b")
                .return_values_all_new()
        )

        self.assertEqual(
            r.attributes,
            {"h": u"expr", "a": 42, "b": 42, "checked": True}
        )

    def test_add_and_delete_from_same_set(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"expr", "a": {1, 2, 3}}))

        # A bit sad: you can't add to and delete from the same set
        with self.assertRaises(_lv.ValidationException):
            self.connection(
                _lv.UpdateItem("Aaa", {"h": u"expr"})
                    .delete("a", "three")
                    .add("a", "four")
                    .expression_attribute_value("three", {3})
                    .expression_attribute_value("four", {4})
                    .return_values_all_new()
            )


class UpdateItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(UpdateItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_indexes_without_indexed_attribute(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("a", ":a").expression_attribute_value("a", "a")
                .return_consumed_capacity_indexes()
        )

        self.assertEqual(r.consumed_capacity.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_consumed_capacity_indexes(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("gsi_h", ":gsi_h").expression_attribute_value("gsi_h", u"1")
                .set("gsi_r", ":gsi_r").expression_attribute_value("gsi_r", 1)
                .set("lsi_r", ":lsi_r").expression_attribute_value("lsi_r", 2)
                .return_consumed_capacity_indexes()
        )

        self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.capacity_units, 3.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_consumed_capacity_indexes_with_locally_indexed_attribute_only(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("lsi_r", ":lsi_r").expression_attribute_value("lsi_r", 2)
                .return_consumed_capacity_indexes()
        )

        self.assertEqual(r.consumed_capacity.capacity_units, 2.0)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_consumed_capacity_indexes_with_globally_indexed_attribute_only(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("gsi_h", ":gsi_h").expression_attribute_value("gsi_h", u"1")
                .set("gsi_r", ":gsi_r").expression_attribute_value("gsi_r", 1)
                .return_consumed_capacity_indexes()
        )

        self.assertEqual(r.consumed_capacity.capacity_units, 2.0)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_item_collection_metrics_size(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("a", ":a").expression_attribute_value("a", "a")
                .return_item_collection_metrics_size()
        )

        self.assertEqual(r.item_collection_metrics.item_collection_key, {"tab_h": u"0"})
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[0], 0.0)
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[1], 1.0)
