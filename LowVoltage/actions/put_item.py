# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_db_to_dict
from .expression_mixins import ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ConditionExpressionMixin
from .return_mixins import ReturnOldValuesMixin, ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin
from .return_types import ItemCollectionMetrics_, ConsumedCapacity_, _is_dict


class PutItem(
    Action,
    ReturnOldValuesMixin,
    ReturnConsumedCapacityMixin,
    ReturnItemCollectionMetricsMixin,
    ExpressionAttributeNamesMixin,
    ExpressionAttributeValuesMixin,
    ConditionExpressionMixin,
):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_RequestParameters"""

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_ResponseElements"""

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

    def __init__(self, table_name, item):
        super(PutItem, self).__init__("PutItem")
        self.__table_name = table_name
        self.__item = item
        ReturnOldValuesMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ConditionExpressionMixin.__init__(self)

    def build(self):
        data = {
            "TableName": self.__table_name,
            "Item": _convert_dict_to_db(self.__item),
        }
        data.update(self._build_return_values())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_return_item_collection_metrics())
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_condition_expression())
        return data


class PutItemUnitTests(_tst.UnitTests):
    def testName(self):
        self.assertEqual(PutItem("Table", {"hash": 42}).name, "PutItem")

    def testItem(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"value"}).build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "value"}},
            }
        )

    def testReturnValuesNone(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnItemCollectionMetricsNone(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_item_collection_metrics_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )

    def testExpressionAttributeValue(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).expression_attribute_value("v", u"value").build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def testExpressionAttributeName(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).expression_attribute_name("n", "path").build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def testConditionExpression(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).condition_expression("a=b").build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
            }
        )


class PutItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def testSimplePut(self):
        r = self.connection(_lv.PutItem("Aaa", {"h": u"simple"}))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)

        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"simple"})).item, {"h": u"simple"})

    def testPutAllTypes(self):
        self.connection(_lv.PutItem("Aaa", {
            "h": u"all",
            "number": 42,
            "string": u"àoé",
            "binary": b"\xFF\x00\xFF",
            "bool 1": True,
            "bool 2": False,
            "null": None,
            "number set": set([42, 43]),
            "string set": set([u"éoà", u"bar"]),
            "binary set": set([b"\xFF", b"\xAB"]),
            "list": [True, 42],
            "map": {"a": True, "b": 42},
        }))

        self.assertEqual(
            self.connection(_lv.GetItem("Aaa", {"h": u"all"})).item,
            {
                "h": u"all",
                "number": 42,
                "string": u"àoé",
                "binary": b"\xFF\x00\xFF",
                "bool 1": True,
                "bool 2": False,
                "null": None,
                "number set": set([42, 43]),
                "string set": set([u"éoà", u"bar"]),
                "binary set": set([b"\xFF", b"\xAB"]),
                "list": [True, 42],
                "map": {"a": True, "b": 42},
            }
        )

    def testReturnOldValues(self):
        self.connection(PutItem("Aaa", {"h": u"return", "a": b"yyy"}))

        r = self.connection(
            PutItem("Aaa", {"h": u"return", "b": b"xxx"}).return_values_all_old()
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, {"h": u"return", "a": b"yyy"})
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)


class PutItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(PutItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_indexes(self):
        r = self.connection(_lv.PutItem(self.table, self.item).return_consumed_capacity_indexes())

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity.capacity_units, 3.0)
            self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.table_name, self.table)
            self.assertEqual(r.item_collection_metrics, None)

    def test_return_item_collection_metrics_size(self):
        r = self.connection(_lv.PutItem(self.table, self.item).return_item_collection_metrics_size())

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics.item_collection_key, {"tab_h": "0"})
            self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[0], 0.0)
            self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[1], 1.0)
