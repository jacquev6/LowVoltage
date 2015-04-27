# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>
"""
When given a :class:`PutItem`, the connection will return a :class:`PutItemResponse`:

>>> connection(PutItem(table, {"h": 0}))
<LowVoltage.actions.put_item.PutItemResponse ...>
"""

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_db_to_dict
from .next_gen_mixins import proxy
from .next_gen_mixins import (
    ConditionExpression,
    ExpressionAttributeNames,
    ExpressionAttributeValues,
    Item,
    ReturnConsumedCapacity,
    ReturnItemCollectionMetrics,
    ReturnValues,
    TableName,
)
from .return_types import ItemCollectionMetrics, ConsumedCapacity, _is_dict


class PutItemResponse(object):
    """
    The `PutItem response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_ResponseElements>`__.
    """

    def __init__(
        self,
        Attributes=None,
        ConsumedCapacity=None,
        ItemCollectionMetrics=None,
        **dummy
    ):
        self.__attributes = Attributes
        self.__consumed_capacity = ConsumedCapacity
        self.__item_collection_metrics = ItemCollectionMetrics

    @property
    def attributes(self):
        """
        The previous attributes of the item you just put. If you used :meth:`~PutItem.return_values_all_old`.

        :type: ``None`` or dict
        """
        if _is_dict(self.__attributes):
            return _convert_db_to_dict(self.__attributes)

    @property
    def consumed_capacity(self):
        """
        The capacity consumed by the request. If you used :meth:`~PutItem.return_consumed_capacity_total` or :meth:`~PutItem.return_consumed_capacity_indexes`.

        :type: ``None`` or :class:`.ConsumedCapacity`
        """
        if _is_dict(self.__consumed_capacity):
            return ConsumedCapacity(**self.__consumed_capacity)

    @property
    def item_collection_metrics(self):
        """
        Metrics about the collection of the item you just put. If a LSI was touched and you used :meth:`~PutItem.return_item_collection_metrics_size`.

        :type: ``None`` or :class:`.ItemCollectionMetrics`
        """
        if _is_dict(self.__item_collection_metrics):
            return ItemCollectionMetrics(**self.__item_collection_metrics)


class PutItem(Action):
    """
    The `PutItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_RequestParameters>`__.
    """

    def __init__(self, table_name=None, item=None):
        """
        Passing ``table_name`` to the constructor is like calling :meth:`table_name` on the new instance.
        Passing ``item`` to the constructor is like calling :meth:`item` on the new instance.
        """
        super(PutItem, self).__init__("PutItem", PutItemResponse)
        self.__condition_expression = ConditionExpression(self)
        self.__expression_attribute_names = ExpressionAttributeNames(self)
        self.__expression_attribute_values = ExpressionAttributeValues(self)
        self.__item = Item(self, item)
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__return_item_collection_metrics = ReturnItemCollectionMetrics(self)
        self.__return_values = ReturnValues(self)
        self.__table_name = TableName(self, table_name)

    @property
    def payload(self):
        data = {}
        data.update(self.__condition_expression.payload)
        data.update(self.__expression_attribute_names.payload)
        data.update(self.__expression_attribute_values.payload)
        data.update(self.__item.payload)
        data.update(self.__return_consumed_capacity.payload)
        data.update(self.__return_item_collection_metrics.payload)
        data.update(self.__return_values.payload)
        data.update(self.__table_name.payload)
        return data

    @proxy
    def item(self, item):
        """
        >>> connection(
        ...   PutItem(table_name=table)
        ...     .item({"h": 0})
        ... )
        <LowVoltage.actions.put_item.PutItemResponse ...>
        """
        return self.__item.set(item)

    @proxy
    def table_name(self, table_name):
        """
        >>> connection(
        ...   PutItem(item={"h": 0})
        ...     .table_name(table)
        ... )
        <LowVoltage.actions.put_item.PutItemResponse ...>
        """
        return self.__table_name.set(table_name)

    @proxy
    def condition_expression(self, expression):
        """
        >>> connection(
        ...   PutItem(table, {"h": 1})
        ...     .condition_expression("#syn=:val")
        ...     .expression_attribute_name("syn", "gr")
        ...     .expression_attribute_value("val", 8)
        ... )
        <LowVoltage.actions.put_item.PutItemResponse ...>
        """
        return self.__condition_expression.set(expression)

    @proxy
    def expression_attribute_name(self, synonym, name):
        """
        See :meth:`condition_expression` for an example.
        """
        return self.__expression_attribute_names.add(synonym, name)

    @proxy
    def expression_attribute_value(self, name, value):
        """
        See :meth:`condition_expression` for an example.
        """
        return self.__expression_attribute_values.add(name, value)

    @proxy
    def return_consumed_capacity_indexes(self):
        """
        >>> c = connection(
        ...   PutItem(table, {"h": 5, "gh": 5, "gr": 5})
        ...     .return_consumed_capacity_indexes()
        ... ).consumed_capacity
        >>> c.capacity_units
        3.0
        >>> c.table.capacity_units
        1.0
        >>> c.global_secondary_indexes["gsi"].capacity_units
        2.0
        """
        return self.__return_consumed_capacity.indexes()

    @proxy
    def return_consumed_capacity_total(self):
        """
        >>> connection(
        ...   PutItem(table, {"h": 4, "gh": 4, "gr": 4})
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        3.0
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_none(self):
        """
        >>> print connection(
        ...   PutItem(table, {"h": 6})
        ...     .return_consumed_capacity_none()
        ... ).consumed_capacity
        None
        """
        return self.__return_consumed_capacity.none()

    @proxy
    def return_item_collection_metrics_size(self):
        """
        >>> m = connection(
        ...   PutItem(table2, {"h": 0, "r1": 0, "r2": 1})
        ...     .return_item_collection_metrics_size()
        ... ).item_collection_metrics
        >>> m.item_collection_key
        {u'h': 0}
        >>> m.size_estimate_range_gb
        [0.0, 1.0]
        """
        return self.__return_item_collection_metrics.size()

    @proxy
    def return_item_collection_metrics_none(self):
        """
        >>> print connection(
        ...   PutItem(table2, {"h": 1, "r1": 0, "r2": 1})
        ...     .return_item_collection_metrics_none()
        ... ).item_collection_metrics
        None
        """
        return self.__return_item_collection_metrics.none()

    @proxy
    def return_values_all_old(self):
        """
        >>> connection(
        ...   PutItem(table, {"h": 2})
        ...     .return_values_all_old()
        ... ).attributes
        {u'h': 2, u'gr': 6, u'gh': 4}
        """
        return self.__return_values.all_old()

    @proxy
    def return_values_none(self):
        """
        >>> print connection(
        ...   PutItem(table, {"h": 3})
        ...     .return_values_none()
        ... ).attributes
        None
        """
        return self.__return_values.none()


class PutItemUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(PutItem("Table", {"hash": 42}).name, "PutItem")

    def test_table_name_and_item(self):
        self.assertEqual(
            PutItem().table_name("Table").item({"hash": 42}).payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
            }
        )

    def test_constructor(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"value"}).payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "value"}},
            }
        )

    def test_return_values_none(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_values_none().payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def test_return_values_all_old(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_values_all_old().payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def test_return_consumed_capacity_total(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_consumed_capacity_total().payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def test_return_consumed_capacity_indexes(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_consumed_capacity_indexes().payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def test_return_consumed_capacity_none(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_consumed_capacity_none().payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def test_return_item_collection_metrics_size(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_item_collection_metrics_size().payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def test_return_item_collection_metrics_none(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_item_collection_metrics_none().payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )

    def test_expression_attribute_value(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).expression_attribute_value("v", u"value").payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def test_expression_attribute_name(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).expression_attribute_name("n", "path").payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def test_condition_expression(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).condition_expression("a=b").payload,
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
            }
        )


class PutItemResponseUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = PutItemResponse()
        self.assertIsNone(r.attributes)
        self.assertIsNone(r.consumed_capacity)
        self.assertIsNone(r.item_collection_metrics)

    def test_all_set(self):
        unprocessed_keys = object()
        r = PutItemResponse(Attributes={"h": {"S": "a"}}, ConsumedCapacity={}, ItemCollectionMetrics={})
        self.assertEqual(r.attributes, {"h": "a"})
        self.assertIsInstance(r.consumed_capacity, ConsumedCapacity)
        self.assertIsInstance(r.item_collection_metrics, ItemCollectionMetrics)
