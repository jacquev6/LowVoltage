# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db
from .next_gen_mixins import proxy, ReturnConsumedCapacity, ReturnItemCollectionMetrics
from .return_types import ConsumedCapacity_, ItemCollectionMetrics_, _is_dict, _is_list_of_dict


class BatchWriteItem(Action):
    """
    The `BatchWriteItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_RequestParameters>`__.
    """

    class Result(object):
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
            self.consumed_capacity = None
            if _is_list_of_dict(ConsumedCapacity):  # pragma no branch (Defensive code)
                self.consumed_capacity = [ConsumedCapacity_(**c) for c in ConsumedCapacity]

            self.item_collection_metrics = None
            if _is_dict(ItemCollectionMetrics):  # pragma no branch (Defensive code)
                self.item_collection_metrics = {n: [ItemCollectionMetrics_(**m) for m in v] for n, v in ItemCollectionMetrics.iteritems()}

            self.unprocessed_items = UnprocessedItems

    def __init__(self, previous_unprocessed_items=None):
        super(BatchWriteItem, self).__init__("BatchWriteItem")
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__return_item_collection_metrics = ReturnItemCollectionMetrics(self)
        self.__previous_unprocessed_items = previous_unprocessed_items
        self.__tables = {}
        self.__active_table = None

    def build(self):
        data = {}
        data.update(self.__return_consumed_capacity.build())
        data.update(self.__return_item_collection_metrics.build())
        if self.__previous_unprocessed_items:
            data["RequestItems"] = self.__previous_unprocessed_items
        if self.__tables:
            data["RequestItems"] = {n: t.build() for n, t in self.__tables.iteritems()}
        return data

    class _Table:
        def __init__(self, action, name):
            self.delete = []
            self.put = []

        def build(self):
            items = []
            if self.delete:
                items.extend({"DeleteRequest": {"Key": _convert_dict_to_db(k)}} for k in self.delete)
            if self.put:
                items.extend({"PutRequest": {"Item": _convert_dict_to_db(i)}} for i in self.put)
            return items

    def table(self, name):
        """
        Set the active table. Calls to methods like :meth:`delete` or :meth:`put` will apply to this table.
        """
        if name not in self.__tables:
            self.__tables[name] = self._Table(self, name)
        self.__active_table = self.__tables[name]
        return self

    def delete(self, *keys):
        """
        Add keys to delete from the active table.
        """
        for key in keys:
            if isinstance(key, dict):
                key = [key]
            self.__active_table.delete.extend(key)
        return self

    def put(self, *items):
        """
        Add items to put in the active table.
        """
        for item in items:
            if isinstance(item, dict):
                item = [item]
            self.__active_table.put.extend(item)
        return self

    @proxy
    def return_consumed_capacity_total(self):
        """
        >>> c = connection(
        ...   BatchWriteItem().table(table).delete({"h": 3})
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity
        >>> c[0].table_name
        u'LowVoltage.DocTests'
        >>> c[0].capacity_units
        2.0
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_indexes(self):
        """
        >>> c = connection(
        ...   BatchWriteItem().table(table).delete({"h": 4})
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
        @todo doctest (We need a table with a LSI)
        """
        return self.__return_item_collection_metrics.size()

    @proxy
    def return_item_collection_metrics_none(self):
        """
        @todo doctest (We need a table with a LSI)
        """
        return self.__return_item_collection_metrics.none()


class BatchWriteItemUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(BatchWriteItem().name, "BatchWriteItem")

    def test_empty(self):
        self.assertEqual(
            BatchWriteItem().build(),
            {}
        )

    def test_return_consumed_capacity_none(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_none().build(),
            {
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def test_return_consumed_capacity_indexes(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_indexes().build(),
            {
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def test_return_consumed_capacity_total(self):
        self.assertEqual(
            BatchWriteItem().return_consumed_capacity_total().build(),
            {
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def test_return_item_collection_metrics_none(self):
        self.assertEqual(
            BatchWriteItem().return_item_collection_metrics_none().build(),
            {
                "ReturnItemCollectionMetrics": "NONE",
            }
        )

    def test_return_item_collection_metrics_size(self):
        self.assertEqual(
            BatchWriteItem().return_item_collection_metrics_size().build(),
            {
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def test_table(self):
        self.assertEqual(
            BatchWriteItem().table("Table").build(),
            {
                "RequestItems": {
                    "Table": [
                    ],
                },
            }
        )

    def test_delete(self):
        self.assertEqual(
            BatchWriteItem().table("Table").delete({"hash": u"h1"}).table("Table").delete([{"hash": u"h2"}]).build(),
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

    def test_alternate_between_tables_and_put_delete(self):
        self.assertEqual(
            BatchWriteItem()
                .table("Table1").delete({"hash": u"h1"})
                .table("Table2").put([{"hash": u"h2"}])
                .table("Table1").put({"hash": u"h11"})
                .table("Table2").delete({"hash": u"h22"})
                .build(),
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


class BatchWriteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_simple_batch_put(self):
        r = self.connection(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"1", "a": "xxx"},
            {"h": u"2", "a": "yyy"},
            {"h": u"3", "a": "zzz"},
        ))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection(_lv.GetItem("Aaa", {"h": u"1"})).item,
            {"h": "1", "a": "xxx"}
        )

    def test_simple_batch_delete(self):
        self.connection(_lv.BatchWriteItem().table("Aaa").put(
            {"h": u"1", "a": "xxx"},
            {"h": u"2", "a": "yyy"},
            {"h": u"3", "a": "zzz"},
        ))

        r = self.connection(_lv.BatchWriteItem().table("Aaa").delete(
            {"h": u"1"},
            {"h": u"2"},
            {"h": u"3"}
        ))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

        self.assertEqual(
            self.connection(_lv.GetItem("Aaa", {"h": u"1"})).item,
            None
        )


class BatchWriteItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(BatchWriteItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_indexes(self):
        r = self.connection(_lv.BatchWriteItem().table(self.table).put(self.item).return_consumed_capacity_indexes())

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity[0].capacity_units, 3.0)
            self.assertEqual(r.consumed_capacity[0].global_secondary_indexes["gsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity[0].local_secondary_indexes["lsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity[0].table.capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity[0].table_name, self.table)
            self.assertEqual(r.item_collection_metrics, None)
            self.assertEqual(r.unprocessed_items, {})

    def test_return_item_collection_metrics_size(self):
        r = self.connection(_lv.BatchWriteItem().table(self.table).put(self.item).return_item_collection_metrics_size())

        with _tst.cover("r", r) as r:
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics[self.table][0].item_collection_key, {"tab_h": "0"})
            self.assertEqual(r.item_collection_metrics[self.table][0].size_estimate_range_gb[0], 0.0)
            self.assertEqual(r.item_collection_metrics[self.table][0].size_estimate_range_gb[1], 1.0)
            self.assertEqual(r.unprocessed_items, {})
