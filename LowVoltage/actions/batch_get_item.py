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
[{u'h': 0, u'gr': 10, u'gh': 0}]

Note that responses are in an undefined order.

See also the :func:`.iterate_batch_get_item` compound. And :ref:`actions-vs-compounds` in the user guide.
"""

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_db_to_dict
from .next_gen_mixins import proxy, variadic
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
        if _is_list_of_dict(self.__consumed_capacity):
            return [ConsumedCapacity(**c) for c in self.__consumed_capacity]

    @property
    def responses(self):
        """
        The items you just got.

        :type: ``None`` or dict of string (table name) to list of dict
        """
        if _is_dict(self.__responses):
            return {t: [_convert_db_to_dict(v) for v in vs] for t, vs in self.__responses.iteritems()}

    @property
    def unprocessed_keys(self):
        """
        Keys that were not processed during this request.
        If not None, you should give this back to :meth:`~BatchGetItem.previous_unprocessed_keys`
        in a subsequent :class:`BatchGetItem`.

        The :func:`.iterate_batch_get_item` compound does that for you.

        :type: ``None`` or exactly as returned by DynamoDB
        """
        return self.__unprocessed_keys


class BatchGetItem(Action):
    """
    The `BatchGetItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_RequestParameters>`__
    """

    @variadic(dict)
    def __init__(self, table=None, keys=[]):
        """
        Passing ``table`` (and ``keys``) to the constructor is like calling :meth:`table` on the new instance.
        """
        super(BatchGetItem, self).__init__("BatchGetItem", BatchGetItemResponse)
        self.__previous_unprocessed_keys = None
        self.__tables = {}
        self.__active_table = None
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        if table is not None:
            self.table(table, keys)

    @property
    def payload(self):
        # @todo Simplify, make more linear
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

    @variadic(dict)
    def table(self, name, keys):
        """
        Set the active table. Calls to methods like :meth:`keys` or :meth:`consistent_read_true` will apply to this table.

        >>> connection(
        ...   BatchGetItem()
        ...     .table(table)
        ...     .keys({"h": 1}, {"h": 2}, {"h": 3})
        ... )
        <LowVoltage.actions.batch_get_item.BatchGetItemResponse ...>

        If some keys are provided, they'll be added to the keys to get from the table.

        >>> connection(
        ...   BatchGetItem()
        ...     .table(table, {"h": 1}, {"h": 2}, {"h": 3})
        ... )
        <LowVoltage.actions.batch_get_item.BatchGetItemResponse ...>
        """
        if name not in self.__tables:
            self.__tables[name] = self._Table(self)
        self.__active_table = self.__tables[name]
        self.keys(keys)
        return self

    @variadic(dict)
    def keys(self, keys):
        """
        Add keys to get from the active table.

        :raise: :exc:`.BuilderError` if called when no table is active.

        >>> connection(
        ...   BatchGetItem()
        ...     .table(table)
        ...     .keys({"h": 1}, {"h": 2}, {"h": 3})
        ... )
        <LowVoltage.actions.batch_get_item.BatchGetItemResponse ...>
        """
        self.__check_active_table()
        self.__active_table.keys.extend(keys)
        return self

    def previous_unprocessed_keys(self, previous_unprocessed_keys):
        """
        Set Table and Keys to retry previous :attr:`~BatchGetItemResponse.unprocessed_keys`.

        The :func:`.iterate_batch_get_item` compound does that for you.

        Note that using this method is incompatible with using :meth:`table` or :meth:`keys`
        or passing a ``table`` or ``keys`` in the constructor.
        """
        self.__previous_unprocessed_keys = previous_unprocessed_keys
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
        [{u'h': 0, u'gr': 10}]
        """
        self.__check_active_table()
        self.__active_table.projection_expression.add(*names)
        return self

    @proxy
    def expression_attribute_name(self, synonym, name):
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
        self.__active_table.expression_attribute_names.add(synonym, name)
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

    def test_constructor_with_table(self):
        self.assertEqual(
            BatchGetItem("Table").keys({"h": u"1"}, {"h": u"2"}).payload,
            {
                "RequestItems": {
                    "Table": {
                        "Keys": [
                            {"h": {"S": "1"}},
                            {"h": {"S": "2"}},
                        ]
                    },
                }
            }
        )

    def test_constructor_with_keys(self):
        self.assertEqual(
            BatchGetItem("Table", {"h": u"1"}, {"h": u"2"}).payload,
            {
                "RequestItems": {
                    "Table": {
                        "Keys": [
                            {"h": {"S": "1"}},
                            {"h": {"S": "2"}},
                        ]
                    },
                }
            }
        )

    def test_constructor_with_keys_in_list(self):
        self.assertEqual(
            BatchGetItem("Table", [{"h": u"1"}, {"h": u"2"}]).payload,
            {
                "RequestItems": {
                    "Table": {
                        "Keys": [
                            {"h": {"S": "1"}},
                            {"h": {"S": "2"}},
                        ]
                    },
                }
            }
        )

    def test_table_keys(self):
        self.assertEqual(
            BatchGetItem().table("Table", {"h": u"1"}, {"h": u"2"}).payload,
            {
                "RequestItems": {
                    "Table": {
                        "Keys": [
                            {"h": {"S": "1"}},
                            {"h": {"S": "2"}},
                        ]
                    },
                }
            }
        )

    def test_table_keys_twice(self):
        self.assertEqual(
            BatchGetItem().table("Table", {"h": u"1"}).table("Table", {"h": u"2"}).payload,
            {
                "RequestItems": {
                    "Table": {
                        "Keys": [
                            {"h": {"S": "1"}},
                            {"h": {"S": "2"}},
                        ]
                    },
                }
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


class BatchGetItemResponseUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = BatchGetItemResponse()
        self.assertIsNone(r.consumed_capacity)
        self.assertIsNone(r.responses)
        self.assertIsNone(r.unprocessed_keys)

    def test_all_set(self):
        unprocessed_keys = object()
        r = BatchGetItemResponse(ConsumedCapacity=[{}], Responses={"A": [{"h": {"S": "a"}}]}, UnprocessedKeys=unprocessed_keys)
        self.assertIsInstance(r.consumed_capacity[0], ConsumedCapacity)
        self.assertEqual(r.responses, {"A": [{"h": u"a"}]})
        self.assertIs(r.unprocessed_keys, unprocessed_keys)
