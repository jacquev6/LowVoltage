# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`BatchWriteItem`, the connection will return a :class:`BatchWriteItemResponse`:

>>> connection(
...   BatchWriteItem().table(table).delete({"h": 0}, {"h": 1})
... )
<LowVoltage.actions.batch_write_item.BatchWriteItemResponse ...>

See also the :func:`.batch_put_item` and :func:`.batch_delete_item` compounds. And :ref:`actions-vs-compounds` in the user guide.
"""

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db
from .next_gen_mixins import proxy, variadic
from .next_gen_mixins import ReturnConsumedCapacity, ReturnItemCollectionMetrics
from .return_types import ConsumedCapacity, ItemCollectionMetrics, _is_dict, _is_list_of_dict


class BatchWriteItemResponse(object):
    """
    The `BatchWriteItem response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_ResponseElements>`__.
    """

    def __init__(
        self,
        ConsumedCapacity=None,
        ItemCollectionMetrics=None,
        UnprocessedItems=None,
        **dummy
    ):
        self.__consumed_capacity = ConsumedCapacity

        self.__item_collection_metrics = ItemCollectionMetrics

        self.__unprocessed_items = UnprocessedItems

    @property
    def consumed_capacity(self):
        """
        The capacity consumed by the request. If you used :meth:`~BatchWriteItem.return_consumed_capacity_total` or :meth:`~BatchWriteItem.return_consumed_capacity_indexes`.

        :type: ``None`` or list of :class:`.ConsumedCapacity`
        """
        if _is_list_of_dict(self.__consumed_capacity):
            return [ConsumedCapacity(**c) for c in self.__consumed_capacity]

    @property
    def item_collection_metrics(self):
        """
        Metrics about the collection of the items you just updated. If a LSI was touched and you used :meth:`~BatchWriteItem.return_item_collection_metrics_size`.

        :type: ``None`` or dict of string (table name) to list of :class:`.ItemCollectionMetrics`
        """
        if _is_dict(self.__item_collection_metrics):
            return {n: [ItemCollectionMetrics(**m) for m in v] for n, v in self.__item_collection_metrics.iteritems()}

    @property
    def unprocessed_items(self):
        """
        Items that were not processed during this request.
        If not None, you should give this back to:meth:`~BatchWriteItem.previous_unprocessed_items`
        in a subsequent :class:`BatchWriteItem`.

        The :func:`.batch_put_item` and :func:`.batch_delete_item` compounds do that for you.

        :type: ``None`` or exactly as returned by DynamoDB
        """
        return self.__unprocessed_items


class BatchWriteItem(Action):
    """
    The `BatchWriteItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_RequestParameters>`__.
    """

    def __init__(self, table=None, put=[], delete=[]):
        """
        Passing ``table`` (and ``put`` and ``delete``) to the constructor is like calling :meth:`table` on the new instance.
        """
        super(BatchWriteItem, self).__init__("BatchWriteItem", BatchWriteItemResponse)
        self.__previous_unprocessed_items = None
        self.__tables = {}
        self.__active_table = None
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__return_item_collection_metrics = ReturnItemCollectionMetrics(self)
        if table is not None:
            self.table(table, put, delete)

    @property
    def payload(self):
        # @todo Simplify, make more linear
        data = {}
        if self.__previous_unprocessed_items:
            data["RequestItems"] = self.__previous_unprocessed_items
        if self.__tables:
            data["RequestItems"] = {n: t.payload for n, t in self.__tables.iteritems()}
        data.update(self.__return_consumed_capacity.payload)
        data.update(self.__return_item_collection_metrics.payload)
        return data

    class _Table:
        def __init__(self, action):
            self.delete = []
            self.put = []

        @property
        def payload(self):
            items = []
            if self.delete:
                items.extend({"DeleteRequest": {"Key": _convert_dict_to_db(k)}} for k in self.delete)
            if self.put:
                items.extend({"PutRequest": {"Item": _convert_dict_to_db(i)}} for i in self.put)
            return items

    def table(self, name, put=[], delete=[]):
        """
        Set the active table. Calls to methods like :meth:`delete` or :meth:`put` will apply to this table.

        >>> connection(
        ...   BatchWriteItem().table(table)
        ...     .put({"h": 12}, {"h": 13})
        ... )
        <LowVoltage.actions.batch_write_item.BatchWriteItemResponse ...>

        If you pass a list of items as ``put``, they'll be added to the items to put in the table.

        >>> connection(
        ...   BatchWriteItem().table(table, put=[{"h": 12}, {"h": 13}])
        ... )
        <LowVoltage.actions.batch_write_item.BatchWriteItemResponse ...>

        If you pass a list of keys as ``delete``, they'll be added to the keys to delete from the table.

        >>> connection(
        ...   BatchWriteItem().table(table, delete=[{"h": 12}, {"h": 13}])
        ... )
        <LowVoltage.actions.batch_write_item.BatchWriteItemResponse ...>
        """
        if name not in self.__tables:
            self.__tables[name] = self._Table(self)
        self.__active_table = self.__tables[name]
        self.put(put)
        self.delete(delete)
        return self

    @variadic(dict)
    def put(self, items):
        """
        Add items to put in the active table.

        :raise: :exc:`.BuilderError` if called when no table is active.

        >>> connection(
        ...   BatchWriteItem().table(table)
        ...     .put({"h": 12}, {"h": 13})
        ... )
        <LowVoltage.actions.batch_write_item.BatchWriteItemResponse ...>
        """
        self.__check_active_table()
        self.__active_table.put.extend(items)
        return self

    @variadic(dict)
    def delete(self, keys):
        """
        Add keys to delete from the active table.

        :raise: :exc:`.BuilderError` if called when no table is active.

        >>> connection(
        ...   BatchWriteItem().table(table)
        ...     .delete({"h": 12}, {"h": 13})
        ... )
        <LowVoltage.actions.batch_write_item.BatchWriteItemResponse ...>
        """
        self.__check_active_table()
        self.__active_table.delete.extend(keys)
        return self

    def previous_unprocessed_items(self, previous_unprocessed_items):
        """
        Set Table and items to retry previous :attr:`~BatchWriteItemResponse.unprocessed_items`.

        The :func:`.batch_put_item` and :func:`.batch_delete_item` compounds do that for you.

        Note that using this method is incompatible with using :meth:`table`, :meth:`put` or :meth:`delete`
        or passing a ``table`` or ``put`` or ``delete`` in the constructor.
        """
        self.__previous_unprocessed_items = previous_unprocessed_items
        return self

    @proxy
    def return_consumed_capacity_total(self):
        """
        >>> c = connection(
        ...   BatchWriteItem().table(table)
        ...     .delete({"h": 3})
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity
        >>> c[0].table_name
        u'LowVoltage.Tests.Doc.1'
        >>> c[0].capacity_units
        2.0
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_indexes(self):
        """
        >>> c = connection(
        ...   BatchWriteItem().table(table)
        ...     .delete({"h": 4})
        ...     .return_consumed_capacity_indexes()
        ... ).consumed_capacity
        >>> c[0].capacity_units
        2.0
        >>> c[0].table.capacity_units
        1.0
        >>> c[0].global_secondary_indexes["gsi"].capacity_units
        1.0
        """
        return self.__return_consumed_capacity.indexes()

    @proxy
    def return_consumed_capacity_none(self):
        """
        >>> print connection(
        ...   BatchWriteItem().table(table).delete({"h": 5})
        ...     .return_consumed_capacity_none()
        ... ).consumed_capacity
        None
        """
        return self.__return_consumed_capacity.none()

    @proxy
    def return_item_collection_metrics_size(self):
        """
        >>> m = connection(
        ...   BatchWriteItem().table(table2)
        ...     .put({"h": 0, "r1": 0, "r2": 0})
        ...     .return_item_collection_metrics_size()
        ... ).item_collection_metrics
        >>> m[table2][0].item_collection_key
        {u'h': 0}
        >>> m[table2][0].size_estimate_range_gb
        [0.0, 1.0]
        """
        return self.__return_item_collection_metrics.size()

    @proxy
    def return_item_collection_metrics_none(self):
        """
        >>> print connection(
        ...   BatchWriteItem().table(table2)
        ...     .put({"h": 1, "r1": 0, "r2": 0})
        ...     .return_item_collection_metrics_none()
        ... ).item_collection_metrics
        None
        """
        return self.__return_item_collection_metrics.none()

    def __check_active_table(self):
        if self.__active_table is None:
            raise _lv.BuilderError("No active table.")


class BatchWriteItemUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(BatchWriteItem().name, "BatchWriteItem")

    def test_empty(self):
        self.assertEqual(
            BatchWriteItem().payload,
            {}
        )

    def test_return_consumed_capacity_none(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_none().payload,
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def test_return_consumed_capacity_indexes(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_indexes().payload,
            {
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def test_return_consumed_capacity_total(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_total().payload,
            {
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def test_return_item_collection_metrics_none(self):
        self.assertEqual(
            BatchWriteItem().return_item_collection_metrics_none().payload,
            {
                "ReturnItemCollectionMetrics": "NONE",
            }
        )

    def test_return_item_collection_metrics_size(self):
        self.assertEqual(
            BatchWriteItem().return_item_collection_metrics_size().payload,
            {
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def test_table(self):
        self.assertEqual(
            BatchWriteItem().table("Table").payload,
            {
                "RequestItems": {
                    "Table": [
                    ],
                },
            }
        )

    def test_constuctor_with_table(self):
        self.assertEqual(
            BatchWriteItem("Table").delete({"hash": u"h1"}).payload,
            {
                "RequestItems": {
                    "Table": [
                        {"DeleteRequest": {"Key": {"hash": {"S": "h1"}}}},
                    ],
                },
            }
        )

    def test_constuctor_with_table_and_delete(self):
        self.assertEqual(
            BatchWriteItem("Table", delete=[{"hash": u"h1"}]).payload,
            {
                "RequestItems": {
                    "Table": [
                        {"DeleteRequest": {"Key": {"hash": {"S": "h1"}}}},
                    ],
                },
            }
        )

    def test_constuctor_with_table_and_put(self):
        self.assertEqual(
            BatchWriteItem("Table", put=[{"hash": u"h1"}]).payload,
            {
                "RequestItems": {
                    "Table": [
                        {"PutRequest": {"Item": {"hash": {"S": "h1"}}}},
                    ],
                },
            }
        )

    def test_table_with_delete(self):
        self.assertEqual(
            BatchWriteItem().table("Table", delete=[{"hash": u"h1"}, {"hash": u"h2"}]).payload,
            {
                "RequestItems": {
                    "Table": [
                        {"DeleteRequest": {"Key": {"hash": {"S": "h1"}}}},
                        {"DeleteRequest": {"Key": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )

    def test_table_with_delete_twice(self):
        self.assertEqual(
            BatchWriteItem().table("Table", delete=[{"hash": u"h1"}]).table("Table", delete=[{"hash": u"h2"}]).payload,
            {
                "RequestItems": {
                    "Table": [
                        {"DeleteRequest": {"Key": {"hash": {"S": "h1"}}}},
                        {"DeleteRequest": {"Key": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )

    def test_table_with_put(self):
        self.assertEqual(
            BatchWriteItem().table("Table", put=[{"hash": u"h1"}, {"hash": u"h2"}]).payload,
            {
                "RequestItems": {
                    "Table": [
                        {"PutRequest": {"Item": {"hash": {"S": "h1"}}}},
                        {"PutRequest": {"Item": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )

    def test_table_with_put_twice(self):
        self.assertEqual(
            BatchWriteItem().table("Table", put=[{"hash": u"h1"}]).table("Table", put=[{"hash": u"h2"}]).payload,
            {
                "RequestItems": {
                    "Table": [
                        {"PutRequest": {"Item": {"hash": {"S": "h1"}}}},
                        {"PutRequest": {"Item": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )

    def test_delete(self):
        self.assertEqual(
            BatchWriteItem().table("Table").delete({"hash": u"h1"}).table("Table").delete([{"hash": u"h2"}]).payload,
            {
                "RequestItems": {
                    "Table": [
                        {"DeleteRequest": {"Key": {"hash": {"S": "h1"}}}},
                        {"DeleteRequest": {"Key": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )

    def test_put(self):
        self.assertEqual(
            BatchWriteItem().table("Table").put({"hash": u"h1"}, [{"hash": u"h2"}]).payload,
            {
                "RequestItems": {
                    "Table": [
                        {"PutRequest": {"Item": {"hash": {"S": "h1"}}}},
                        {"PutRequest": {"Item": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )

    def test_alternate_between_tables_and_put_delete(self):
        self.assertEqual(
            BatchWriteItem()
                .table("Table1").delete({"hash": u"h1"})
                .table("Table2").put([{"hash": u"h2"}])
                .table("Table1").put({"hash": u"h11"})
                .table("Table2").delete({"hash": u"h22"})
                .payload,
            {
                "RequestItems": {
                    "Table1": [
                        {"DeleteRequest": {"Key": {"hash": {"S": "h1"}}}},
                        {"PutRequest": {"Item": {"hash": {"S": "h11"}}}},
                    ],
                    "Table2": [
                        {"DeleteRequest": {"Key": {"hash": {"S": "h22"}}}},
                        {"PutRequest": {"Item": {"hash": {"S": "h2"}}}},
                    ],
                },
            }
        )

    def test_put_without_active_table(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            BatchWriteItem().put({"h": 0})
        self.assertEqual(catcher.exception.args, ("No active table.",))

    def test_delete_without_active_table(self):
        with self.assertRaises(_lv.BuilderError) as catcher:
            BatchWriteItem().delete({"h": 0})
        self.assertEqual(catcher.exception.args, ("No active table.",))


class BatchWriteItemResponseUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = BatchWriteItemResponse()
        self.assertIsNone(r.consumed_capacity)
        self.assertIsNone(r.item_collection_metrics)
        self.assertIsNone(r.unprocessed_items)

    def test_all_set(self):
        unprocessed_items = object()
        r = BatchWriteItemResponse(ConsumedCapacity=[{}], ItemCollectionMetrics={"A": [{}]}, UnprocessedItems=unprocessed_items)
        self.assertIsInstance(r.consumed_capacity[0], ConsumedCapacity)
        self.assertIsInstance(r.item_collection_metrics["A"][0], ItemCollectionMetrics)
        self.assertIs(r.unprocessed_items, unprocessed_items)
