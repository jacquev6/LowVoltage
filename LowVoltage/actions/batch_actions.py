# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.actions.action import Action as _Action, ActionProxy as _ActionProxy
from LowVoltage.actions.return_mixins import ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin
from LowVoltage.actions.expression_mixins import (
    ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin,
    ProjectionExpressionMixin, FilterExpressionMixin,
)
from LowVoltage.actions.conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value
import LowVoltage.tests.dynamodb_local
import LowVoltage.actions.admin_actions
import LowVoltage.actions.item_actions
import LowVoltage.return_types as _rtyp
import LowVoltage.attribute_types as _atyp
import LowVoltage.exceptions as _exn
from LowVoltage.tests.cover import cover


class BatchGetItem(_Action,
    ReturnConsumedCapacityMixin,
):
    class Result(object):
        def __init__(
            self,
            ConsumedCapacity=None,
            Responses=None,
            UnprocessedKeys=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_ResponseElements
            # - ConsumedCapacity: done
            # - Responses: done
            # - UnprocessedKeys: @todo
            self.consumed_capacity = None if ConsumedCapacity is None else _rtyp.ConsumedCapacity(**ConsumedCapacity)
            self.responses = None if Responses is None else {t: [_convert_db_to_dict(v) for v in vs] for t, vs in Responses.iteritems()}
            self.unprocessed_keys = UnprocessedKeys

    def __init__(self):
        super(BatchGetItem, self).__init__("BatchGetItem")
        ReturnConsumedCapacityMixin.__init__(self)
        self.__tables = {}

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_RequestParameters
        # - RequestItems:
        #   - Keys: done
        #   - AttributesToGet: deprecated
        #   - ConsistentRead: done
        #   - ExpressionAttributeNames: done
        #   - ProjectionExpression: done
        # - ReturnConsumedCapacity: done
        data = {}
        data.update(self._build_return_consumed_capacity())
        if self.__tables:
            data["RequestItems"] = {n: t._build() for n, t in self.__tables.iteritems()}
        return data

    class _Table(_ActionProxy, ExpressionAttributeNamesMixin, ProjectionExpressionMixin):
        def __init__(self, action, name):
            super(BatchGetItem._Table, self).__init__(action)
            ExpressionAttributeNamesMixin.__init__(self)
            ProjectionExpressionMixin.__init__(self)
            self.__consistent_read = None
            self.__keys = []

        def _build(self):
            data = {}
            data.update(self._build_expression_attribute_names())
            data.update(self._build_projection_expression())
            if self.__consistent_read is not None:
                data["ConsistentRead"] = self.__consistent_read
            if self.__keys:
                data["Keys"] = [_convert_dict_to_db(k) for k in self.__keys]
            return data

        def keys(self, *keys):
            for key in keys:
                if isinstance(key, dict):
                    key = [key]
                self.__keys.extend(key)
            return self

        def consistent_read_true(self):
            self.__consistent_read = True
            return self

        def consistent_read_false(self):
            self.__consistent_read = False
            return self

    def table(self, name):
        if name not in self.__tables:
            self.__tables[name] = self._Table(self, name)
        return self.__tables[name]


class BatchGetItemUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(BatchGetItem().name, "BatchGetItem")

    def testEmpty(self):
        self.assertEqual(
            BatchGetItem().build(),
            {}
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            BatchGetItem().return_consumed_capacity_none().build(),
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testKeys(self):
        self.assertEqual(
            BatchGetItem().table("Table2").keys({"hash": u"h21"}).table("Table1").keys({"hash": u"h11"}, {"hash": u"h12"}).table("Table2").keys([{"hash": u"h22"}, {"hash": u"h23"}]).build(),
            {
                "RequestItems": {
                    "Table1": {
                        "Keys": [
                            {"hash": {"S": "h11"}},
                            {"hash": {"S": "h12"}},
                        ]
                    },
                    "Table2": {
                        "Keys": [
                            {"hash": {"S": "h21"}},
                            {"hash": {"S": "h22"}},
                            {"hash": {"S": "h23"}},
                        ]
                    },
                }
            }
        )

    def testConsistentRead(self):
        self.assertEqual(
            BatchGetItem().table("Table1").consistent_read_true().table("Table2").consistent_read_false().build(),
            {
                "RequestItems": {
                    "Table1": {
                        "ConsistentRead": True,
                    },
                    "Table2": {
                        "ConsistentRead": False,
                    },
                }
            }
        )

    def testProject(self):
        self.assertEqual(
            BatchGetItem().table("Table1").project("a").build(),
            {
                "RequestItems": {
                    "Table1": {
                        "ProjectionExpression": "a",
                    },
                }
            }
        )

    def testExpressionAttributeName(self):
        self.assertEqual(
            BatchGetItem().table("Table1").expression_attribute_name("a", "p").build(),
            {
                "RequestItems": {
                    "Table1": {
                        "ExpressionAttributeNames": {"#a": "p"},
                    },
                }
            }
        )


class BatchGetItemIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.actions.admin_actions.CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(LowVoltage.actions.admin_actions.DeleteTable("Aaa"))

    def testSimpleBatchGet(self):
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"1", "a": "xxx"}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"2", "a": "yyy"}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"3", "a": "zzz"}))

        r = self.connection.request(BatchGetItem().table("Aaa").keys({"h": u"1"}, {"h": u"2"}, {"h": u"3"}))

        with cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.responses.keys(), ["Aaa"])
            self.assertEqual(
                sorted(r.responses["Aaa"], key=lambda i: i["h"]),
                [{"h": u"1", "a": "xxx"}, {"h": u"2", "a": "yyy"}, {"h": u"3", "a": "zzz"}]
            )
            self.assertEqual(r.unprocessed_keys, {})

    def testBatchGetWithProjections(self):
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"1", "a": "a1", "b": "b1", "c": "c1"}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"2", "a": "a2", "b": "b2", "c": "c2"}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"3", "a": "a3", "b": "b3", "c": "c3"}))

        r = self.connection.request(
            BatchGetItem().table("Aaa").keys({"h": u"1"}, {"h": u"2"}, {"h": u"3"}).expression_attribute_name("p", "b").project("h").project("a", ["#p"])
        )

        with cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.responses.keys(), ["Aaa"])
            self.assertEqual(
                sorted(r.responses["Aaa"], key=lambda i: i["h"]),
                [{"h": u"1", "a": "a1", "b": "b1"}, {"h": u"2", "a": "a2", "b": "b2"}, {"h": u"3", "a": "a3", "b": "b3"}]
            )
            self.assertEqual(r.unprocessed_keys, {})


class BatchWriteItem(_Action,
    ReturnConsumedCapacityMixin, ReturnItemCollectionMetricsMixin,
):
    class Result(object):
        def __init__(
            self,
            ConsumedCapacity=None,
            ItemCollectionMetrics=None,
            UnprocessedItems=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_ResponseElements
            # - ConsumedCapacity: done
            # - ItemCollectionMetrics: done
            # - UnprocessedItems: @todo
            self.consumed_capacity = None if ConsumedCapacity is None else _rtyp.ConsumedCapacity(**ConsumedCapacity)
            self.item_collection_metrics = None if ItemCollectionMetrics is None else _rtyp.ItemCollectionMetrics(**ItemCollectionMetrics)
            self.unprocessed_items = UnprocessedItems

    def __init__(self):
        super(BatchWriteItem, self).__init__("BatchWriteItem")
        ReturnConsumedCapacityMixin.__init__(self)
        ReturnItemCollectionMetricsMixin.__init__(self)
        self.__tables = {}

    def build(self):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_RequestParameters
        # - RequestItems
        #   - DeleteRequest.Key: done
        #   - PutRequest.Item: done
        # - ReturnConsumedCapacity: done
        # - ReturnItemCollectionMetrics: done
        data = {}
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_return_item_collection_metrics())
        if self.__tables:
            data["RequestItems"] = {n: t._build() for n, t in self.__tables.iteritems()}
        return data

    class _Table(_ActionProxy):
        def __init__(self, action, name):
            super(BatchWriteItem._Table, self).__init__(action)
            self.__delete = []
            self.__put = []

        def _build(self):
            items = []
            if self.__delete:
                items.extend({"DeleteRequest": {"Key": _convert_dict_to_db(k)}} for k in self.__delete)
            if self.__put:
                items.extend({"PutRequest": {"Item": _convert_dict_to_db(i)}} for i in self.__put)
            return items

        def delete(self, *keys):
            for key in keys:
                if isinstance(key, dict):
                    key = [key]
                self.__delete.extend(key)
            return self

        def put(self, *items):
            for item in items:
                if isinstance(item, dict):
                    item = [item]
                self.__put.extend(item)
            return self

    def table(self, name):
        if name not in self.__tables:
            self.__tables[name] = self._Table(self, name)
        return self.__tables[name]


class BatchWriteItemUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(BatchWriteItem().name, "BatchWriteItem")

    def testEmpty(self):
        self.assertEqual(
            BatchWriteItem().build(),
            {}
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_none().build(),
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnItemCollectionMetricsNone(self):
        self.assertEqual(
            BatchWriteItem().return_item_collection_metrics_none().build(),
            {
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


    def testDelete(self):
        self.assertEqual(
            BatchWriteItem().table("Table").delete({"hash": u"h1"}).table("Table").delete([{"hash": u"h2"}]).build(),
            {
                "RequestItems": {
                    "Table": [
                        {"DeleteRequest": {"Key": {"hash": {"S": "h1"}}}},
                        {"DeleteRequest": {"Key": {"hash": {"S": "h2"}}}},
                    ]
                },
            }
        )

    def testPut(self):
        self.assertEqual(
            BatchWriteItem().table("Table").put({"hash": u"h1"}, [{"hash": u"h2"}]).build(),
            {
                "RequestItems": {
                    "Table": [
                        {"PutRequest": {"Item": {"hash": {"S": "h1"}}}},
                        {"PutRequest": {"Item": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )


class BatchWriteItemIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.actions.admin_actions.CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(LowVoltage.actions.admin_actions.DeleteTable("Aaa"))

    def testSimpleBatchPut(self):
        r = self.connection.request(
            BatchWriteItem().table("Aaa")
                .put({"h": u"1", "a": "xxx"}, {"h": u"2", "a": "yyy"}, {"h": u"3", "a": "zzz"})
        )

        with cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection.request(LowVoltage.actions.item_actions.GetItem("Aaa", {"h": u"1"})).item,
            {"h": "1", "a": "xxx"}
        )

    def testSimpleBatchDelete(self):
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"1", "a": "xxx"}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"2", "a": "yyy"}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"3", "a": "zzz"}))

        r = self.connection.request(BatchWriteItem().table("Aaa").delete({"h": u"1"}, {"h": u"2"}, {"h": u"3"}))

        with cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection.request(LowVoltage.actions.item_actions.GetItem("Aaa", {"h": u"1"})).item,
            None
        )


class Query(_Action,
    ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ProjectionExpressionMixin, FilterExpressionMixin,
    ReturnConsumedCapacityMixin,
):
    class Result(object):
        def __init__(
            self,
            ConsumedCapacity=None,
            Count=None,
            Items=None,
            LastEvaluatedKey=None,
            ScannedCount=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_ResponseElements
            # - ConsumedCapacity: done
            # - Count: done
            # - Items: done
            # - LastEvaluatedKey: done
            # - ScannedCount: done
            self.consumed_capacity = None if ConsumedCapacity is None else _rtyp.ConsumedCapacity(**ConsumedCapacity)
            self.count = None if Count is None else long(Count)
            self.items = None if Items is None else [_convert_db_to_dict(i) for i in Items]
            self.last_evaluated_key = None if LastEvaluatedKey is None else _convert_db_to_dict(LastEvaluatedKey)
            self.scanned_count = None if ScannedCount is None else long(ScannedCount)

    def __init__(self, table_name):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestParameters
        # - KeyConditions: done
        # - TableName: done
        # - AttributesToGet: deprecated
        # - ConditionalOperator: deprecated
        # - ConsistentRead: done
        # - ExclusiveStartKey: done
        # - ExpressionAttributeNames: done
        # - ExpressionAttributeValues: done
        # - FilterExpression: done
        # - IndexName: done
        # - Limit: done
        # - ProjectionExpression: done
        # - QueryFilter: deprecated
        # - ReturnConsumedCapacity: done
        # - ScanIndexForward: done
        # - Select: done
        super(Query, self).__init__("Query")
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ProjectionExpressionMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        FilterExpressionMixin.__init__(self)
        self.__table_name = table_name
        self.__conditions = {}
        self.__exclusive_start_key = None
        self.__limit = None
        self.__select = None
        self.__consistent_read = None
        self.__index_name = None
        self.__scan_index_forward = None

    def build(self):
        data = {"TableName": self.__table_name}
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_projection_expression())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_filter_expression())
        if self.__conditions:
            data["KeyConditions"] = self.__conditions
        if self.__exclusive_start_key:
            data["ExclusiveStartKey"] = _convert_dict_to_db(self.__exclusive_start_key)
        if self.__limit:
            data["Limit"] = self.__limit
        if self.__select:
            data["Select"] = self.__select
        if self.__consistent_read is not None:
            data["ConsistentRead"] = self.__consistent_read
        if self.__index_name:
            data["IndexName"] = self.__index_name
        if self.__scan_index_forward is not None:
            data["ScanIndexForward"] = self.__scan_index_forward
        return data

    def key_eq(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "EQ", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_le(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "LE", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_lt(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "LT", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_ge(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "GE", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_gt(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "GT", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_begins_with(self, name, value):
        self.__conditions[name] = {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [_convert_value_to_db(value)]}
        return self

    def key_between(self, name, lo, hi):
        self.__conditions[name] = {"ComparisonOperator": "BETWEEN", "AttributeValueList": [_convert_value_to_db(lo), _convert_value_to_db(hi)]}
        return self

    def exclusive_start_key(self, key):
        self.__exclusive_start_key = key
        return self

    def limit(self, limit):
        self.__limit = limit
        return self

    def select_count(self):
        self.__select = "COUNT"
        return self

    def select_all_attributes(self):
        self.__select = "ALL_ATTRIBUTES"
        return self

    def select_all_projected_attributes(self):
        self.__select = "ALL_PROJECTED_ATTRIBUTES"
        return self

    def select_specific_attributes(self):
        self.__select = "SPECIFIC_ATTRIBUTES"
        return self

    def consistent_read_true(self):
        self.__consistent_read = True
        return self

    def consistent_read_false(self):
        self.__consistent_read = False
        return self

    def index_name(self, name):
        self.__index_name = name
        return self

    def scan_index_forward_true(self):
        self.__scan_index_forward = True
        return self

    def scan_index_forward_false(self):
        self.__scan_index_forward = False
        return self


class QueryUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(Query("Aaa").name, "Query")

    def testTableName(self):
        self.assertEqual(Query("Aaa").build(), {"TableName": "Aaa"})

    def testKeyEq(self):
        self.assertEqual(
            Query("Aaa").key_eq("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "EQ", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyLe(self):
        self.assertEqual(
            Query("Aaa").key_le("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "LE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyLt(self):
        self.assertEqual(
            Query("Aaa").key_lt("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "LT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyGe(self):
        self.assertEqual(
            Query("Aaa").key_ge("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "GE", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyGt(self):
        self.assertEqual(
            Query("Aaa").key_gt("name", 42).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "GT", "AttributeValueList": [{"N": "42"}]}},
            }
        )

    def testKeyBeginsWith(self):
        self.assertEqual(
            Query("Aaa").key_begins_with("name", u"prefix").build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "BEGINS_WITH", "AttributeValueList": [{"S": "prefix"}]}},
            }
        )

    def testKeyBetween(self):
        self.assertEqual(
            Query("Aaa").key_between("name", 42, 44).build(),
            {
                "TableName": "Aaa",
                "KeyConditions": {"name": {"ComparisonOperator": "BETWEEN", "AttributeValueList": [{"N": "42"}, {"N": "44"}]}},
            }
        )

    def testExclusiveStartKey(self):
        self.assertEqual(Query("Aaa").exclusive_start_key({"h": u"v"}).build(), {"TableName": "Aaa", "ExclusiveStartKey": {"h": {"S": "v"}}})

    def testLimit(self):
        self.assertEqual(Query("Aaa").limit(4).build(), {"TableName": "Aaa", "Limit": 4})

    def testSelect(self):
        self.assertEqual(Query("Aaa").select_all_attributes().build(), {"TableName": "Aaa", "Select": "ALL_ATTRIBUTES"})
        self.assertEqual(Query("Aaa").select_all_projected_attributes().build(), {"TableName": "Aaa", "Select": "ALL_PROJECTED_ATTRIBUTES"})
        self.assertEqual(Query("Aaa").select_count().build(), {"TableName": "Aaa", "Select": "COUNT"})
        self.assertEqual(Query("Aaa").select_specific_attributes().build(), {"TableName": "Aaa", "Select": "SPECIFIC_ATTRIBUTES"})

    def testExpressionAttributeName(self):
        self.assertEqual(Query("Aaa").expression_attribute_name("n", "p").build(), {"TableName": "Aaa", "ExpressionAttributeNames": {"#n": "p"}})

    def testExpressionAttributeValue(self):
        self.assertEqual(Query("Aaa").expression_attribute_value("n", u"p").build(), {"TableName": "Aaa", "ExpressionAttributeValues": {":n": {"S": "p"}}})

    def testProject(self):
        self.assertEqual(Query("Aaa").project("a").build(), {"TableName": "Aaa", "ProjectionExpression": "a"})

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(Query("Aaa").return_consumed_capacity_none().build(), {"TableName": "Aaa", "ReturnConsumedCapacity": "NONE"})

    def testFilterExpression(self):
        self.assertEqual(Query("Aaa").filter_expression("a=b").build(), {"TableName": "Aaa", "FilterExpression": "a=b"})

    def testConsistentRead(self):
        self.assertEqual(Query("Aaa").consistent_read_true().build(), {"TableName": "Aaa", "ConsistentRead": True})
        self.assertEqual(Query("Aaa").consistent_read_false().build(), {"TableName": "Aaa", "ConsistentRead": False})

    def testIndexName(self):
        self.assertEqual(Query("Aaa").index_name("foo").build(), {"TableName": "Aaa", "IndexName": "foo"})

    def testScanIndexForward(self):
        self.assertEqual(Query("Aaa").scan_index_forward_true().build(), {"TableName": "Aaa", "ScanIndexForward": True})
        self.assertEqual(Query("Aaa").scan_index_forward_false().build(), {"TableName": "Aaa", "ScanIndexForward": False})


class QueryIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.actions.admin_actions.CreateTable("Aaa")
                .hash_key("h", _atyp.STRING)
                .range_key("r", _atyp.NUMBER)
                .provisioned_throughput(1, 2)
        )

        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"0", "r": 41, "v": 0}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"0", "r": 42, "v": 1}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"0", "r": 43, "v": 2}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"0", "r": 44, "v": 3}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"0", "r": 45, "v": 4}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"1", "r": 42, "v": 2}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"2", "r": 42, "v": 3}))

    def tearDown(self):
        self.connection.request(LowVoltage.actions.admin_actions.DeleteTable("Aaa"))

    def testSimpleQuery(self):
        r = self.connection.request(
            Query("Aaa").key_eq("h", u"1")
        )

        with cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": "1", "r": 42, "v": 2})
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 1)

    def testComplexQuery(self):
        r = self.connection.request(
            Query("Aaa").key_eq("h", u"0").key_between("r", 42, 44)
                .scan_index_forward_false()
                .project("r", "v")
                .filter_expression("#p<>:v")
                .expression_attribute_name("p", "v")
                .expression_attribute_value("v", 2)
                .limit(2)
        )

        with cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"r": 44, "v": 3})
            self.assertEqual(r.last_evaluated_key, {"h": u"0", "r": 43})
            self.assertEqual(r.scanned_count, 2)


class Scan(_Action,
    ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ProjectionExpressionMixin, FilterExpressionMixin,
    ReturnConsumedCapacityMixin,
):
    class Result(object):
        def __init__(
            self,
            ConsumedCapacity=None,
            Count=None,
            Items=None,
            LastEvaluatedKey=None,
            ScannedCount=None,
            **dummy
        ):
            # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_ResponseElements
            # - ConsumedCapacity: done
            # - Count: done
            # - Items: done
            # - LastEvaluatedKey: done
            # - ScannedCount: done
            self.consumed_capacity = None if ConsumedCapacity is None else _rtyp.ConsumedCapacity(**ConsumedCapacity)
            self.count = None if Count is None else long(Count)
            self.items = None if Items is None else [_convert_db_to_dict(i) for i in Items]
            self.last_evaluated_key = None if LastEvaluatedKey is None else _convert_db_to_dict(LastEvaluatedKey)
            self.scanned_count = None if ScannedCount is None else long(ScannedCount)

    def __init__(self, table_name):
        # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_RequestParameters
        # - TableName: done
        # - AttributesToGet: deprecated
        # - ConditionalOperator: deprecated
        # - ExclusiveStartKey: done
        # - ExpressionAttributeNames: done
        # - ExpressionAttributeValues: done
        # - FilterExpression: done
        # - Limit: done
        # - ProjectionExpression: done
        # - ReturnConsumedCapacity: done
        # - ScanFilter: deprecated
        # - Segment: done
        # - Select: done
        # - TotalSegments: done
        super(Scan, self).__init__("Scan")
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ProjectionExpressionMixin.__init__(self)
        ReturnConsumedCapacityMixin.__init__(self)
        FilterExpressionMixin.__init__(self)
        self.__table_name = table_name
        self.__exclusive_start_key = None
        self.__limit = None
        self.__select = None
        self.__segment = None
        self.__total_segments = None

    def build(self):
        data = {"TableName": self.__table_name}
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_projection_expression())
        data.update(self._build_return_consumed_capacity())
        data.update(self._build_filter_expression())
        if self.__segment is not None:
            data["Segment"] = self.__segment
        if self.__total_segments:
            data["TotalSegments"] = self.__total_segments
        if self.__exclusive_start_key:
            data["ExclusiveStartKey"] = _convert_dict_to_db(self.__exclusive_start_key)
        if self.__limit:
            data["Limit"] = self.__limit
        if self.__select:
            data["Select"] = self.__select
        return data

    def segment(self, segment, total_segments):
        self.__segment = segment
        self.__total_segments = total_segments
        return self

    def exclusive_start_key(self, key):
        self.__exclusive_start_key = key
        return self

    def limit(self, limit):
        self.__limit = limit
        return self

    def select_count(self):
        self.__select = "COUNT"
        return self

    def select_all_attributes(self):
        self.__select = "ALL_ATTRIBUTES"
        return self

    def select_specific_attributes(self):
        self.__select = "SPECIFIC_ATTRIBUTES"
        return self


class ScanUnitTests(unittest.TestCase):
    def testName(self):
        self.assertEqual(Scan("Aaa").name, "Scan")

    def testTableName(self):
        self.assertEqual(Scan("Aaa").build(), {"TableName": "Aaa"})

    def testSegment(self):
        self.assertEqual(Scan("Aaa").segment(0, 2).build(), {"TableName": "Aaa", "Segment": 0, "TotalSegments": 2})
        self.assertEqual(Scan("Aaa").segment(1, 2).build(), {"TableName": "Aaa", "Segment": 1, "TotalSegments": 2})

    def testExclusiveStartKey(self):
        self.assertEqual(Scan("Aaa").exclusive_start_key({"h": u"v"}).build(), {"TableName": "Aaa", "ExclusiveStartKey": {"h": {"S": "v"}}})

    def testLimit(self):
        self.assertEqual(Scan("Aaa").limit(4).build(), {"TableName": "Aaa", "Limit": 4})

    def testSelect(self):
        self.assertEqual(Scan("Aaa").select_all_attributes().build(), {"TableName": "Aaa", "Select": "ALL_ATTRIBUTES"})
        self.assertEqual(Scan("Aaa").select_count().build(), {"TableName": "Aaa", "Select": "COUNT"})
        self.assertEqual(Scan("Aaa").select_specific_attributes().build(), {"TableName": "Aaa", "Select": "SPECIFIC_ATTRIBUTES"})

    def testExpressionAttributeName(self):
        self.assertEqual(Scan("Aaa").expression_attribute_name("n", "p").build(), {"TableName": "Aaa", "ExpressionAttributeNames": {"#n": "p"}})

    def testExpressionAttributeValue(self):
        self.assertEqual(Scan("Aaa").expression_attribute_value("n", u"p").build(), {"TableName": "Aaa", "ExpressionAttributeValues": {":n": {"S": "p"}}})

    def testProject(self):
        self.assertEqual(Scan("Aaa").project("a").build(), {"TableName": "Aaa", "ProjectionExpression": "a"})

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(Scan("Aaa").return_consumed_capacity_none().build(), {"TableName": "Aaa", "ReturnConsumedCapacity": "NONE"})

    def testFilterExpression(self):
        self.assertEqual(Scan("Aaa").filter_expression("a=b").build(), {"TableName": "Aaa", "FilterExpression": "a=b"})


class ScanIntegTests(LowVoltage.tests.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            LowVoltage.actions.admin_actions.CreateTable("Aaa").hash_key("h", _atyp.STRING).provisioned_throughput(1, 2)
        )

        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"0", "v": 0}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"1", "v": 1}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"2", "v": 2}))
        self.connection.request(LowVoltage.actions.item_actions.PutItem("Aaa", {"h": u"3", "v": 3}))

    def tearDown(self):
        self.connection.request(LowVoltage.actions.admin_actions.DeleteTable("Aaa"))

    def testSimpleScan(self):
        r = self.connection.request(
            Scan("Aaa")
        )

        with cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 4)
            items = sorted((r.items[i] for i in range(4)), key=lambda i: i["h"])
            self.assertEqual(items, [{"h": u"0", "v": 0}, {"h": u"1", "v": 1}, {"h": u"2", "v": 2}, {"h": u"3", "v": 3}])
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 4)

    def testPaginatedSegmentedScan(self):
        # If this test fails randomly, change it to assert on the sum and union of the results
        r01 = self.connection.request(
            Scan("Aaa").segment(0, 2).limit(1)
        )
        r02 = self.connection.request(
            Scan("Aaa").segment(0, 2).exclusive_start_key({"h": u"1"})
        )
        r11 = self.connection.request(
            Scan("Aaa").segment(1, 2).limit(1)
        )
        r12 = self.connection.request(
            Scan("Aaa").segment(1, 2).exclusive_start_key({"h": u"0"})
        )

        with cover("r01", r01) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": u"1", "v": 1})
            self.assertEqual(r.last_evaluated_key, {"h": u"1"})
            self.assertEqual(r.scanned_count, 1)

        with cover("r02", r02) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": u"3", "v": 3})
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 1)

        with cover("r11", r11) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": u"0", "v": 0})
            self.assertEqual(r.last_evaluated_key, {"h": u"0"})
            self.assertEqual(r.scanned_count, 1)

        with cover("r12", r12) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 1)
            self.assertEqual(r.items[0], {"h": u"2", "v": 2})
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 1)

    def testFilteredScan(self):
        r = self.connection.request(
            Scan("Aaa").filter_expression("v>:v").expression_attribute_value("v", 1).project("h")
        )

        with cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.count, 2)
            self.assertEqual(r.items[0], {"h": u"3"})
            self.assertEqual(r.items[1], {"h": u"2"})
            self.assertEqual(r.last_evaluated_key, None)
            self.assertEqual(r.scanned_count, 4)


if __name__ == "__main__":
    LowVoltage.tests.dynamodb_local.main()  # pragma no cover (Test code)
