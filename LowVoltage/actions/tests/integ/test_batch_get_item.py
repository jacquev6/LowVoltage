# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class BatchGetItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_simple_batch_get(self):
        self.connection(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"1", "a": "xxx"},
            {"h": u"2", "a": "yyy"},
            {"h": u"3", "a": "zzz"},
        ))

        r = self.connection(_lv.BatchGetItem().table("Aaa").keys({"h": u"1"}, {"h": u"2"}, {"h": u"3"}))

        self.assertEqual(r.responses.keys(), ["Aaa"])
        self.assertEqual(
            sorted(r.responses["Aaa"], key=lambda i: i["h"]),
            [{"h": u"1", "a": "xxx"}, {"h": u"2", "a": "yyy"}, {"h": u"3", "a": "zzz"}]
        )

    def test_batch_get_with_projections(self):
        self.connection(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"1", "a": "a1", "b": "b1", "c": "c1"},
            {"h": u"2", "a": "a2", "b": "b2", "c": "c2"},
            {"h": u"3", "a": "a3", "b": "b3", "c": "c3"},
        ))

        r = self.connection(
            _lv.BatchGetItem().table("Aaa").keys({"h": u"1"}, {"h": u"2"}, {"h": u"3"}).expression_attribute_name("p", "b").project("h").project("a", ["#p"])
        )
        self.assertEqual(
            sorted(r.responses["Aaa"], key=lambda i: i["h"]),
            [{"h": u"1", "a": "a1", "b": "b1"}, {"h": u"2", "a": "a2", "b": "b2"}, {"h": u"3", "a": "a3", "b": "b3"}]
        )

    def test_get_unexisting_keys(self):
        self.connection(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"1", "a": "xxx"},
            {"h": u"2", "a": "yyy"},
        ))

        r = self.connection(_lv.BatchGetItem().table("Aaa").keys({"h": u"1"}, {"h": u"2"}, {"h": u"3"}))

        self.assertEqual(
            sorted(r.responses["Aaa"], key=lambda i: i["h"]),
            [{"h": u"1", "a": "xxx"}, {"h": u"2", "a": "yyy"}]
        )
        self.assertEqual(r.unprocessed_keys, {})

    def test_get_without_unprocessed_keys(self):
        _lv.batch_put_item(self.connection, "Aaa", [{"h": unicode(i)} for i in range(100)])

        r = self.connection(_lv.BatchGetItem().table("Aaa").keys({"h": unicode(i)} for i in range(100)))

        self.assertEqual(r.unprocessed_keys, {})
        self.assertEqual(len(r.responses["Aaa"]), 100)

    def test_get_with_unprocessed_keys(self):
        _lv.batch_put_item(self.connection, "Aaa", [{"h": unicode(i), "xs": "x" * 300000} for i in range(100)])  # 300kB items ensure a single BatchGetItem will return at most 55 items

        r1 = self.connection(_lv.BatchGetItem().table("Aaa").keys({"h": unicode(i)} for i in range(100)))

        self.assertEqual(len(r1.unprocessed_keys["Aaa"]["Keys"]), 45)
        self.assertEqual(len(r1.responses["Aaa"]), 55)


class BatchGetItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(BatchGetItemConnectedIntegTests, self).setUp()
        self.connection(_lv.PutItem(self.table, self.item))

    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(BatchGetItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_total(self):
        r = self.connection(_lv.BatchGetItem().table(self.table).keys(self.tab_key).return_consumed_capacity_total())

        self.assertEqual(r.consumed_capacity[0].capacity_units, 0.5)
        self.assertEqual(r.consumed_capacity[0].global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity[0].local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity[0].table, None)
        self.assertEqual(r.consumed_capacity[0].table_name, self.table)
