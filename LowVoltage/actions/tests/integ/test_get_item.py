# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class GetItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_simple_get(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"get", "a": "yyy"}))

        r = self.connection(_lv.GetItem("Aaa", {"h": u"get"}))

        self.assertEqual(r.item, {"h": "get", "a": "yyy"})

    def test_get_with_projections(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"attrs", "a": "yyy", "b": {"c": ["d1", "d2", "d3"]}, "e": 42, "f": "nope"}))

        r = self.connection(_lv.GetItem("Aaa", {"h": u"attrs"}).project("b.c[1]", "e"))

        self.assertEqual(r.item, {"b": {"c": ["d2"]}, "e": 42})

    def test_unexisting_table(self):
        with self.assertRaises(_lv.ResourceNotFoundException):
            self.connection(_lv.GetItem("Bbb", {}))

    def test_bad_key_type(self):
        with self.assertRaises(_lv.ValidationException):
            self.connection(_lv.GetItem("Aaa", {"h": 42}))


class GetItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(GetItemConnectedIntegTests, self).setUp()
        self.connection(_lv.PutItem(self.table, self.item))

    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(GetItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_total(self):
        r = self.connection(_lv.GetItem(self.table, self.tab_key).return_consumed_capacity_total())

        self.assertEqual(r.consumed_capacity.capacity_units, 0.5)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.table, None)
        self.assertEqual(r.consumed_capacity.table_name, self.table)
