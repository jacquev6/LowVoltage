# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`DeleteItem`, the connection will return a :class:`DeleteItemResponse`:

>>> connection(DeleteItem(table, {"h": 0}))
<LowVoltage.actions.delete_item.DeleteItemResponse object at ...>

Note that deleting the same item twice is not an error (deleting is idempotent). To know if an item was actually deleted, use :meth:`~DeleteItem.return_values_all_old`:

>>> connection(
...   DeleteItem(table, {"h": 1})
...     .return_values_all_old()
... ).attributes
{u'h': 1, u'gr': 0, u'gh': 0}
>>> print connection(
...   DeleteItem(table, {"h": 1})
...     .return_values_all_old()
... ).attributes
None
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
    ReturnConsumedCapacity,
    ReturnItemCollectionMetrics,
    ReturnValues,
)
from .return_types import ConsumedCapacity, ItemCollectionMetrics, _is_dict


class DeleteItemResponse(object):
    """
    The `DeleteItem response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html#API_DeleteItem_ResponseElements>`__
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
        The previous attributes of the item you just deleted. If you used :meth:`~DeleteItem.return_values_all_old`.

        :type: ``None`` or dict
        """
        if _is_dict(self.__attributes):  # pragma no branch (Defensive code)
            return _convert_db_to_dict(self.__attributes)

    @property
    def consumed_capacity(self):
        """
        The capacity consumed by the request. If you used :meth:`~DeleteItem.return_consumed_capacity_total` or :meth:`~DeleteItem.return_consumed_capacity_indexes`.

        :type: ``None`` or :class:`.ConsumedCapacity`
        """
        if _is_dict(self.__consumed_capacity):  # pragma no branch (Defensive code)
            return ConsumedCapacity(**self.__consumed_capacity)

    @property
    def item_collection_metrics(self):
        """
        Metrics about the collection of the item you just deleted. If a LSI was touched and you used :meth:`~DeleteItem.return_item_collection_metrics_size`.

        :type: ``None`` or :class:`.ItemCollectionMetrics`
        """
        if _is_dict(self.__item_collection_metrics):  # pragma no branch (Defensive code)
            return ItemCollectionMetrics(**self.__item_collection_metrics)


class DeleteItem(Action):
    """
    The `DeleteItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html#API_DeleteItem_RequestParameters>`__
    """

    def __init__(self, table_name, key):
        super(DeleteItem, self).__init__("DeleteItem", DeleteItemResponse)
        self.__table_name = table_name
        self.__key = key
        self.__condition_expression = ConditionExpression(self)
        self.__expression_attribute_names = ExpressionAttributeNames(self)
        self.__expression_attribute_values = ExpressionAttributeValues(self)
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__return_item_collection_metrics = ReturnItemCollectionMetrics(self)
        self.__return_values = ReturnValues(self)

    @property
    def payload(self):
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self.__condition_expression.payload)
        data.update(self.__expression_attribute_names.payload)
        data.update(self.__expression_attribute_values.payload)
        data.update(self.__return_consumed_capacity.payload)
        data.update(self.__return_item_collection_metrics.payload)
        data.update(self.__return_values.payload)
        return data

    @proxy
    def condition_expression(self, expression):
        """
        >>> connection(
        ...   DeleteItem(table, {"h": 2})
        ...     .condition_expression("#syn=:val")
        ...     .expression_attribute_name("syn", "gr")
        ...     .expression_attribute_value("val", 0)
        ... )
        <LowVoltage.actions.delete_item.DeleteItemResponse object at ...>
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
    def return_values_all_old(self):
        """
        >>> connection(
        ...   DeleteItem(table, {"h": 3})
        ...     .return_values_all_old()
        ... ).attributes
        {u'h': 3, u'gr': 0, u'gh': 0}
        """
        return self.__return_values.all_old()

    @proxy
    def return_values_none(self):
        """
        >>> print connection(
        ...   DeleteItem(table, {"h": 4})
        ...     .return_values_none()
        ... ).attributes
        None
        """
        return self.__return_values.none()

    @proxy
    def return_consumed_capacity_total(self):
        """
        >>> connection(
        ...   DeleteItem(table, {"h": 5})
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        2.0
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_indexes(self):
        """
        >>> c = connection(
        ...   DeleteItem(table, {"h": 6})
        ...     .return_consumed_capacity_indexes()
        ... ).consumed_capacity
        >>> c.capacity_units
        2.0
        >>> c.table.capacity_units
        1.0
        >>> c.global_secondary_indexes["gsi"].capacity_units
        1.0
        """
        return self.__return_consumed_capacity.indexes()

    @proxy
    def return_consumed_capacity_none(self):
        """
        >>> print connection(
        ...   DeleteItem(table, {"h": 7})
        ...     .return_consumed_capacity_none()
        ... ).consumed_capacity
        None
        """
        return self.__return_consumed_capacity.none()

    @proxy
    def return_item_collection_metrics_size(self):
        """
        >>> m = connection(
        ...   DeleteItem(table2, {"h": 0, "r1": 0})
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
        ...   DeleteItem(table2, {"h": 1, "r1": 0})
        ...     .return_item_collection_metrics_none()
        ... ).item_collection_metrics
        None
        """
        return self.__return_item_collection_metrics.none()


class DeleteItemUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(DeleteItem("Table", {"hash": 42}).name, "DeleteItem")

    def test_key(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def test_return_values_none(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_values_none().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def test_return_values_all_old(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_values_all_old().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def test_return_consumed_capacity_total(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_consumed_capacity_total().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def test_return_consumed_capacity_indexes(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_consumed_capacity_indexes().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def test_return_consumed_capacity_none(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_consumed_capacity_none().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def test_return_item_collection_metrics_size(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_item_collection_metrics_size().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def test_return_item_collection_metrics_none(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_item_collection_metrics_none().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )

    def test_expression_attribute_value(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).expression_attribute_value("v", u"value").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def test_expression_attribute_name(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).expression_attribute_name("n", "path").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def test_condition_expression(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).condition_expression("a=b").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
            }
        )


class DeleteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_simple_delete(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"simple", "a": "yyy"}))

        self.connection(_lv.DeleteItem("Aaa", {"h": u"simple"}))

        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"simple"})).item, None)

    def test_return_old_values(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"return", "a": "yyy"}))

        r = self.connection(_lv.DeleteItem("Aaa", {"h": u"return"}).return_values_all_old())

        self.assertEqual(r.attributes, {"h": "return", "a": "yyy"})


class DeleteItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(DeleteItemConnectedIntegTests, self).setUp()
        self.connection(_lv.PutItem(self.table, self.item))

    def test_return_consumed_capacity_indexes(self):
        r = self.connection(_lv.DeleteItem(self.table, self.tab_key).return_consumed_capacity_indexes())

        self.assertEqual(r.consumed_capacity.capacity_units, 3.0)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_item_collection_metrics_size(self):
        r = self.connection(_lv.DeleteItem(self.table, self.tab_key).return_item_collection_metrics_size())

        self.assertEqual(r.item_collection_metrics.item_collection_key, {"tab_h": u"0"})
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[0], 0.0)
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[1], 1.0)
