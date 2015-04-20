# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_db_to_dict
from .expression_mixins import ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ConditionExpressionMixin
from .return_mixins import ReturnValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin
from .return_types import ConsumedCapacity_, ItemCollectionMetrics_, _is_dict


class UpdateItem(
    Action,
    ReturnValuesMixin,
    ReturnConsumedCapacityMixin,
    ReturnItemCollectionMetricsMixin,
    ExpressionAttributeNamesMixin,
    ExpressionAttributeValuesMixin,
    ConditionExpressionMixin,
):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_RequestParameters"""

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_ResponseElements"""

        def __init__(
            self,
            Attributes=None,
            ConsumedCapacity=None,
            ItemCollectionMetrics=None,
            **dummy
        ):
            self.attributes = None
            if _is_dict(Attributes):  # pragma no branch (Defensive code)
                self.attributes = _convert_db_to_dict(Attributes)

            self.consumed_capacity = None
            if _is_dict(ConsumedCapacity):  # pragma no branch (Defensive code)
                self.consumed_capacity = ConsumedCapacity_(**ConsumedCapacity)

            self.item_collection_metrics = None
            if _is_dict(ItemCollectionMetrics):  # pragma no branch (Defensive code)
                self.item_collection_metrics = ItemCollectionMetrics_(**ItemCollectionMetrics)

    def __init__(self, table_name, key):
        super(UpdateItem, self).__init__("UpdateItem")
        self.__table_name = table_name
        self.__key = key
        self.__set = {}
        self.__remove = []
        self.__add = {}
        self.__delete = {}
        ReturnValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ConditionExpressionMixin.__init__(self)

    def build(self):
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self._build_return_values())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_return_item_collection_metrics())
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_condition_expression())
        update = []
        if self.__set:
            # http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.SET
            update.append("SET {}".format(", ".join("{}=:{}".format(n, v) for n, v in self.__set.iteritems())))
        if self.__remove:
            # http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.REMOVE
            update.append("REMOVE {}".format(", ".join(self.__remove)))
        if self.__add:
            # http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.ADD
            update.append("ADD {}".format(", ".join("{} :{}".format(n, v) for n, v in self.__add.iteritems())))
        if self.__delete:
            # http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.DELETE
            update.append("DELETE {}".format(", ".join("{} :{}".format(n, v) for n, v in self.__delete.iteritems())))
        if update:
            data["UpdateExpression"] = " ".join(update)
        return data

    def set(self, attribute_name, value_name):
        self.__set[attribute_name] = value_name
        return self

    def remove(self, path):
        self.__remove.append(path)
        return self

    def add(self, attribute_name, value_name):
        self.__add[attribute_name] = value_name
        return self

    def delete(self, attribute_name, value_name):
        self.__delete[attribute_name] = value_name
        return self


class UpdateItemUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(UpdateItem("Table", {"hash": 42}).name, "UpdateItem")

    def testKey(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def testSet(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).set("a", "v").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "SET a=:v",
            }
        )

    def testSeveralSets(self):
        self.assertIn(
            UpdateItem("Table", {"hash": 42}).set("a", "v").set("b", "w").build(),
            [
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "SET a=:v, b=:w",
                },
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "SET b=:w, a=:v",
                }
            ]
        )

    def testRemove(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).remove("a").remove("b").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "REMOVE a, b",
            }
        )

    def testAdd(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).add("a", "v").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "ADD a :v",
            }
        )

    def testSeveralAdds(self):
        self.assertIn(
            UpdateItem("Table", {"hash": 42}).add("a", "v").add("b", "w").build(),
            [
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "ADD a :v, b :w",
                },
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "ADD b :w, a :v",
                }
            ]
        )

    def testDelete(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).delete("a", "v").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "DELETE a :v",
            }
        )

    def testSeveralDeletes(self):
        self.assertIn(
            UpdateItem("Table", {"hash": 42}).delete("a", "v").delete("b", "w").build(),
            [
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "DELETE a :v, b :w",
                },
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "DELETE b :w, a :v",
                }
            ]
        )

    def testExpressionAttributeValue(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).expression_attribute_value("v", u"value").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def testExpressionAttributeName(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).expression_attribute_name("n", "path").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def testConditionExpression(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).condition_expression("a=b").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
            }
        )

    def testReturnValuesNone(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnItemCollectionMetricsNone(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_item_collection_metrics_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


class UpdateItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def testSet(self):
        r = self.connection.request(
            _lv.UpdateItem("Aaa", {"h": u"set"})
                .set("a", "v")
                .set("#p", "w")
                .expression_attribute_value("v", "aaa")
                .expression_attribute_value("w", "bbb")
                .expression_attribute_name("p", "b")
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)

        self.assertEqual(
            self.connection.request(_lv.GetItem("Aaa", {"h": u"set"})).item,
            {"h": "set", "a": "aaa", "b": "bbb"}
        )

    def testComplexUpdate(self):
        self.connection.request(
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

        r = self.connection.request(
            _lv.UpdateItem("Aaa", {"h": u"complex"})
                .set("a", "s")
                .set("b", "i")
                .remove("c")
                .add("d", "s")
                .add("e", "i")
                .delete("f", "s")
                .delete("g", "s")
                .expression_attribute_value("s", set([42, 43]))
                .expression_attribute_value("i", 52)
                .return_values_all_new()
        )

        with _tst.cover("r", r) as r:
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
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)

    def testConditionExpression(self):
        self.connection.request(_lv.PutItem("Aaa", {"h": u"expr", "a": 42, "b": 42}))

        r = self.connection.request(
            _lv.UpdateItem("Aaa", {"h": u"expr"})
                .set("checked", "true")
                .expression_attribute_value("true", True)
                .condition_expression("a=b")
                .return_values_all_new()
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(
                r.attributes,
                {"h": u"expr", "a": 42, "b": 42, "checked": True}
            )
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)


class UpdateItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def tearDown(self):
        self.connection.request(_lv.DeleteItem(self.table, self.tab_key))
        super(UpdateItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_indexes_without_indexed_attribute(self):
        r = self.connection.request(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("a", "a").expression_attribute_value("a", "a")
                .return_consumed_capacity_indexes()
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity.capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.table_name, self.table)
            self.assertEqual(r.item_collection_metrics, None)

    def test_return_consumed_capacity_indexes(self):
        r = self.connection.request(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("gsi_h", "gsi_h").expression_attribute_value("gsi_h", u"1")
                .set("gsi_r", "gsi_r").expression_attribute_value("gsi_r", 1)
                .set("lsi_r", "lsi_r").expression_attribute_value("lsi_r", 2)
                .return_consumed_capacity_indexes()
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.capacity_units, 3.0)
            self.assertEqual(r.consumed_capacity.table_name, self.table)
            self.assertEqual(r.item_collection_metrics, None)

    def test_return_consumed_capacity_indexes_with_locally_indexed_attribute_only(self):
        r = self.connection.request(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("lsi_r", "lsi_r").expression_attribute_value("lsi_r", 2)
                .return_consumed_capacity_indexes()
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity.capacity_units, 2.0)
            self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.table_name, self.table)
            self.assertEqual(r.item_collection_metrics, None)

    def test_return_consumed_capacity_indexes_with_globally_indexed_attribute_only(self):
        r = self.connection.request(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("gsi_h", "gsi_h").expression_attribute_value("gsi_h", u"1")
                .set("gsi_r", "gsi_r").expression_attribute_value("gsi_r", 1)
                .return_consumed_capacity_indexes()
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity.capacity_units, 2.0)
            self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
            self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.table_name, self.table)
            self.assertEqual(r.item_collection_metrics, None)

    def test_return_item_collection_metrics_size(self):
        r = self.connection.request(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("a", "a").expression_attribute_value("a", "a")
                .return_item_collection_metrics_size()
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics.item_collection_key, {"tab_h": u"0"})
            self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[0], 0.0)
            self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[1], 1.0)
