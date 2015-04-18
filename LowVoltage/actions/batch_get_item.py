# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import MockMockMock

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import CompletableAction, ActionProxy
from .conversion import _convert_dict_to_db, _convert_db_to_dict
from .expression_mixins import ExpressionAttributeNamesMixin, ProjectionExpressionMixin
from .return_mixins import ReturnConsumedCapacityMixin
from .return_types import ConsumedCapacity_, _is_dict


class BatchGetItem(
    CompletableAction,
    ReturnConsumedCapacityMixin,
):
    """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_RequestParameters"""

    class Result(object):
        """http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_ResponseElements"""

        def __init__(
            self,
            ConsumedCapacity=None,
            Responses=None,
            UnprocessedKeys=None,
            **dummy
        ):
            self.consumed_capacity = None
            if _is_dict(ConsumedCapacity):  # pragma no branch (Defensive code)
                self.consumed_capacity = ConsumedCapacity_(**ConsumedCapacity)

            self.responses = None
            if _is_dict(Responses):  # pragma no branch (Defensive code)
                self.responses = {t: [_convert_db_to_dict(v) for v in vs] for t, vs in Responses.iteritems()}

            self.unprocessed_keys = UnprocessedKeys

    def __init__(self, previous_unprocessed_keys=None):
        super(BatchGetItem, self).__init__("BatchGetItem")
        ReturnConsumedCapacityMixin.__init__(self)
        self.__previous_unprocessed_keys = previous_unprocessed_keys
        self.__tables = {}

    def build(self):
        data = {}
        data.update(self._build_return_consumed_capacity())
        if self.__previous_unprocessed_keys:
            data["RequestItems"] = self.__previous_unprocessed_keys
        if self.__tables:
            data["RequestItems"] = {n: t._build() for n, t in self.__tables.iteritems()}
        return data

    class _Table(ActionProxy, ExpressionAttributeNamesMixin, ProjectionExpressionMixin):
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

    def get_completion_action(self, r):
        if _is_dict(r.unprocessed_keys) and len(r.unprocessed_keys) != 0:
            return BatchGetItem(r.unprocessed_keys)
        else:
            return None

    def complete_response(self, r1, r2):
        r1.consumed_capacity = r2.consumed_capacity  # @todo Should we merge those? (Maybe add them?)
        for table, items in r2.responses.iteritems():
            l = r1.responses.setdefault(table, [])
            l += items
        r1.unprocessed_keys = r2.unprocessed_keys


class BatchGetItemUnitTests(unittest.TestCase):
    def setUp(self):
        self.mocks = MockMockMock.Engine()

    def tearDown(self):
        self.mocks.tearDown()

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

    def test_no_completion_action(self):
        self.assertIsNone(
            BatchGetItem().get_completion_action(BatchGetItem.Result(UnprocessedKeys={}))
        )

    def test_completion_action(self):
        a = BatchGetItem().get_completion_action(BatchGetItem.Result(UnprocessedKeys={"Foo": {}}))
        self.assertEqual(a.build(), {"RequestItems": {"Foo": {}}})

    def test_complete_response(self):
        r = BatchGetItem.Result(Responses={"A": [{"h": {"S": "0"}}]})
        r2 = self.mocks.create("r2")
        r2.expect.consumed_capacity.andReturn(1)
        r2.expect.responses.andReturn({"A": [1, 2], "B": [3, 4]})
        r2.expect.unprocessed_keys.andReturn(2)
        BatchGetItem().complete_response(r, r2.object)
        self.assertEqual(r.consumed_capacity, 1)
        self.assertEqual(r.responses, {"A": [{"h": "0"}, 1, 2], "B": [3, 4]})
        self.assertEqual(r.unprocessed_keys, 2)


class BatchGetItemLocalIntegTests(_tst.dynamodb_local.TestCase):
    def setUp(self):
        self.connection.request(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 2)
        )

    def tearDown(self):
        self.connection.request(_lv.DeleteTable("Aaa"))

    def testSimpleBatchGet(self):
        self.connection.request(_lv.PutItem("Aaa", {"h": u"1", "a": "xxx"}))
        self.connection.request(_lv.PutItem("Aaa", {"h": u"2", "a": "yyy"}))
        self.connection.request(_lv.PutItem("Aaa", {"h": u"3", "a": "zzz"}))

        r = self.connection.request(_lv.BatchGetItem().table("Aaa").keys({"h": u"1"}, {"h": u"2"}, {"h": u"3"}))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.responses.keys(), ["Aaa"])
            self.assertEqual(
                sorted(r.responses["Aaa"], key=lambda i: i["h"]),
                [{"h": u"1", "a": "xxx"}, {"h": u"2", "a": "yyy"}, {"h": u"3", "a": "zzz"}]
            )
            self.assertEqual(r.unprocessed_keys, {})

    def testBatchGetWithProjections(self):
        self.connection.request(_lv.PutItem("Aaa", {"h": u"1", "a": "a1", "b": "b1", "c": "c1"}))
        self.connection.request(_lv.PutItem("Aaa", {"h": u"2", "a": "a2", "b": "b2", "c": "c2"}))
        self.connection.request(_lv.PutItem("Aaa", {"h": u"3", "a": "a3", "b": "b3", "c": "c3"}))

        r = self.connection.request(
            _lv.BatchGetItem().table("Aaa").keys({"h": u"1"}, {"h": u"2"}, {"h": u"3"}).expression_attribute_name("p", "b").project("h").project("a", ["#p"])
        )

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.responses.keys(), ["Aaa"])
            self.assertEqual(
                sorted(r.responses["Aaa"], key=lambda i: i["h"]),
                [{"h": u"1", "a": "a1", "b": "b1"}, {"h": u"2", "a": "a2", "b": "b2"}, {"h": u"3", "a": "a3", "b": "b3"}]
            )
            self.assertEqual(r.unprocessed_keys, {})

    def test_get_without_unprocessed_keys(self):
        for i in range(100):
            self.connection.request(_lv.PutItem("Aaa", {"h": unicode(i)}))

        action = _lv.BatchGetItem().table("Aaa").keys({"h": unicode(i)} for i in range(100))
        r = self.connection.request(action)
        self.assertEqual(r.unprocessed_keys, {})
        self.assertEqual(len(r.responses["Aaa"]), 100)

        self.assertEqual(action.is_completable, True)
        self.assertIsNone(action.get_completion_action(r))

    def test_get_with_unprocessed_keys(self):
        for i in range(100):
            self.connection.request(_lv.PutItem("Aaa", {"h": unicode(i), "xs": "x" * 300000}))

        main_action = _lv.BatchGetItem().table("Aaa").keys({"h": unicode(i)} for i in range(100))
        r1 = self.connection.request(main_action)
        self.assertEqual(len(r1.unprocessed_keys["Aaa"]["Keys"]), 45)
        self.assertEqual(len(r1.responses["Aaa"]), 55)

        second_action = main_action.get_completion_action(r1)
        r2 = self.connection.request(second_action)
        self.assertEqual(r2.unprocessed_keys, {})
        self.assertEqual(len(r2.responses["Aaa"]), 45)

        main_action.complete_response(r1, r2)
        self.assertEqual(r1.unprocessed_keys, {})
        self.assertEqual(len(r1.responses["Aaa"]), 100)

        third_action = main_action.get_completion_action(r1)
        self.assertIsNone(third_action)
