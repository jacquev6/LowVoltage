# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`BatchGetItem`, the connection will return a :class:`BatchGetItemResponse`:

>>> connection(BatchGetItem().table(table).keys({"h": 0}, {"h": 1}))
<LowVoltage.actions.batch_get_item.BatchGetItemResponse ...>

Responses are accessed like this:

>>> connection(
...   BatchGetItem().table(table).keys({"h": 0})
... ).responses[table]
[{u'h': 0, u'gr': 0, u'gh': 0}]

Note that responses are in an undefined order.
"""

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_db_to_dict
from .next_gen_mixins import proxy
from .next_gen_mixins import (
    ConsistentRead,
    ExpressionAttributeNames,
    ProjectionExpression,
    ReturnConsumedCapacity,
)
from .return_types import ConsumedCapacity, _is_dict, _is_list_of_dict


class BatchGetItemResponse(object):
    """
    The `BatchGetItem response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_ResponseElements>`__
    """

    def __init__(
        self,
        ConsumedCapacity=None,
        Responses=None,
        UnprocessedKeys=None,
        **dummy
    ):
        self.__consumed_capacity = ConsumedCapacity
        self.__responses = Responses
        self.__unprocessed_keys = UnprocessedKeys

    @property
    def consumed_capacity(self):
        """
        The capacity consumed by the request. If you used :meth:`~BatchGetItem.return_consumed_capacity_total`.

        :type: ``None`` or list of :class:`.ConsumedCapacity`
        """
        if _is_list_of_dict(self.__consumed_capacity):  # pragma no branch (Defensive code)
            return [ConsumedCapacity(**c) for c in self.__consumed_capacity]

    @property
    def responses(self):
        """
        The items you just got.

        :type: ``None`` or dict of string (table name) to list of dict
        """
        if _is_dict(self.__responses):  # pragma no branch (Defensive code)
            return {t: [_convert_db_to_dict(v) for v in vs] for t, vs in self.__responses.iteritems()}

    @property
    def unprocessed_keys(self):
        """
        Keys that were not processed during this request. If not None, you should give this back to the constructor of a subsequent :class:`BatchGetItem`.

        :type: ``None`` or exactly as returned by DynamoDB
        """
        return self.__unprocessed_keys


class BatchGetItem(Action):
    """
    The `BatchGetItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_RequestParameters>`__
    """

    def __init__(self, previous_unprocessed_keys=None):
        super(BatchGetItem, self).__init__("BatchGetItem", BatchGetItemResponse)
        self.__previous_unprocessed_keys = previous_unprocessed_keys
        self.__tables = {}
        self.__active_table = None
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)

    @property
    def payload(self):
        data = {}
        if self.__previous_unprocessed_keys:
            data["RequestItems"] = self.__previous_unprocessed_keys
        if self.__tables:
            data["RequestItems"] = {n: t.payload for n, t in self.__tables.iteritems()}
        data.update(self.__return_consumed_capacity.payload)
        return data

    class _Table:
        def __init__(self, action):
            self.keys = []
            self.consistent_read = ConsistentRead(action)
            self.expression_attribute_names = ExpressionAttributeNames(action)
            self.projection_expression = ProjectionExpression(action)

        @property
        def payload(self):
            data = {}
            if self.keys:
                data["Keys"] = [_convert_dict_to_db(k) for k in self.keys]
            data.update(self.consistent_read.payload)
            data.update(self.expression_attribute_names.payload)
            data.update(self.projection_expression.payload)
            return data

    def table(self, name):
        """
        Set the active table. Calls to methods like :meth:`keys` or :meth:`consistent_read_true` will apply to this table.
        """
        if name not in self.__tables:
            self.__tables[name] = self._Table(self)
        self.__active_table = self.__tables[name]
        return self

    # @todo Add unit tests about calling keys (and others) without an active table. Same in other multi-table actions. Same in CreateTable for active index.

    def keys(self, *keys):
        """
        Add keys to get from the active table.
        This method accepts a variable number of keys or iterables.

        :raise: :exc:`.BuilderError` if called when no table is active.

        >>> connection(
        ...   BatchGetItem()
        ...     .table(table)
        ...     .keys({"h": 1})
        ...     .keys({"h": 2}, {"h": 3})
        ...     .keys([{"h": 4}, {"h": 5}])
        ...     .keys({"h": h} for h in range(6, 10))
        ... )
        <LowVoltage.actions.batch_get_item.BatchGetItemResponse ...>
        """
        self.__check_active_table()
        for key in keys:
            if isinstance(key, dict):
                key = [key]
            self.__active_table.keys.extend(key)
        return self

    @proxy
    def consistent_read_true(self):
        """
        :raise: :exc:`.BuilderError` if called when no table is active.

        >>> c = connection(
        ...   BatchGetItem()
        ...     .table(table).keys({"h": 0})
        ...     .consistent_read_true()
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity
        >>> c[0].table_name
        u'LowVoltage.Tests.Doc.1'
        >>> c[0].capacity_units
        1.0
        """
        self.__check_active_table()
        return self.__active_table.consistent_read.true()

    @proxy
    def consistent_read_false(self):
        """
        :raise: :exc:`.BuilderError` if called when no table is active.

        >>> c = connection(
        ...   BatchGetItem()
        ...     .table(table).keys({"h": 0})
        ...     .consistent_read_false()
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity
        >>> c[0].table_name
        u'LowVoltage.Tests.Doc.1'
        >>> c[0].capacity_units
        0.5
        """
        self.__check_active_table()
        return self.__active_table.consistent_read.false()

    @proxy
    def project(self, *names):
        """
        :raise: :exc:`.BuilderError` if called when no table is active.

        >>> connection(
        ...   BatchGetItem()
        ...     .table(table)
        ...     .keys({"h": 0})
        ...     .project("h", "gr")
        ... ).responses[table]
        [{u'h': 0, u'gr': 0}]
        """
        self.__check_active_table()
        self.__active_table.projection_expression.add(*names)
        return self

    @proxy
    def expression_attribute_name(self, name, path):
        """
        :raise: :exc:`.BuilderError` if called when no table is active.

        >>> connection(
        ...   BatchGetItem()
        ...     .table(table)
        ...     .keys({"h": 0})
        ...     .expression_attribute_name("syn", "h")
        ...     .project("#syn")
        ... ).responses[table]
        [{u'h': 0}]
        """
        self.__check_active_table()
        self.__active_table.expression_attribute_names.add(name, path)
        return self

    @proxy
    def return_consumed_capacity_total(self):
        """
        >>> c = connection(
        ...   BatchGetItem()
        ...     .table(table).keys({"h": 0}, {"h": 1}, {"h": 2})
        ...     .table(table2).keys({"h": 0, "r1": 0}, {"h": 1, "r1": 0})
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity
        >>> c[0].table_name
        u'LowVoltage.Tests.Doc.1'
        >>> c[0].capacity_units
        1.5
        >>> c[1].table_name
        u'LowVoltage.Tests.Doc.2'
        >>> c[1].capacity_units
        1.0
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_none(self):
        """
        >>> print connection(
        ...   BatchGetItem()
        ...     .table(table)
        ...     .keys({"h": 0}, {"h": 1}, {"h": 2})
        ...     .return_consumed_capacity_none()
        ... ).consumed_capacity
        None
        """
        return self.__return_consumed_capacity.none()

    def __check_active_table(self):
        if self.__active_table is None:
            raise _lv.BuilderError("No active table.")


class BatchGetItemUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(BatchGetItem().name, "BatchGetItem")

    def test_empty(self):
        self.assertEqual(
            BatchGetItem().payload,
            {}
        )

    def test_return_consumed_capacity_none(self):
        self.assertEqual(
            BatchGetItem().return_consumed_capacity_none().payload,
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def test_return_consumed_capacity_total(self):
        self.assertEqual(
            BatchGetItem().return_consumed_capacity_total().payload,
            {
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def test_keys(self):
        self.assertEqual(
            BatchGetItem().table("Table2").keys({"hash": u"h21"}).table("Table1").keys({"hash": u"h11"}, {"hash": u"h12"}).table("Table2").keys([{"hash": u"h22"}, {"hash": u"h23"}]).payload,
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

    def test_consistent_read(self):
        self.assertEqual(
            BatchGetItem().table("Table1").consistent_read_true().table("Table2").consistent_read_false().payload,
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

    def test_project(self):
        self.assertEqual(
            BatchGetItem().table("Table1").project("a").payload,
            {
                "RequestItems": {
                    "Table1": {
                        "ProjectionExpression": "a",
                    },
                }
            }
        )

    def test_expression_attribute_name(self):
        self.assertEqual(
            BatchGetItem().table("Table1").expression_attribute_name("a", "p").payload,
            {
                "RequestItems": {
                    "Table1": {
                        "ExpressionAttributeNames": {"#a": "p"},
                    },
                }
            }
        )

    def test_keys_without_active_table(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            BatchGetItem().keys({"h": 0})
        self.assertEqual(catcher.exception.args, ("No active table.",))

    def test_consistent_read_true_without_active_table(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            BatchGetItem().consistent_read_true()
        self.assertEqual(catcher.exception.args, ("No active table.",))

    def test_consistent_read_false_without_active_table(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            BatchGetItem().consistent_read_false()
        self.assertEqual(catcher.exception.args, ("No active table.",))

    def test_project_without_active_table(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            BatchGetItem().project("a")
        self.assertEqual(catcher.exception.args, ("No active table.",))

    def test_expression_attribute_name_without_active_table(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            BatchGetItem().expression_attribute_name("a", "b")
        self.assertEqual(catcher.exception.args, ("No active table.",))


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
        _lv.BatchPutItem(self.connection, "Aaa", [{"h": unicode(i)} for i in range(100)])

        r = self.connection(_lv.BatchGetItem().table("Aaa").keys({"h": unicode(i)} for i in range(100)))

        self.assertEqual(r.unprocessed_keys, {})
        self.assertEqual(len(r.responses["Aaa"]), 100)

    def test_get_with_unprocessed_keys(self):
        _lv.BatchPutItem(self.connection, "Aaa", [{"h": unicode(i), "xs": "x" * 300000} for i in range(100)])  # 300kB items ensure a single BatchGetItem will return at most 55 items

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
