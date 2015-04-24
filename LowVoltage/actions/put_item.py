# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>
"""
When given a :class:`PutItem`, the connection will return a :class:`PutItemResponse`:

>>> connection(PutItem(table, {"h": 0}))
<LowVoltage.actions.put_item.PutItemResponse object at ...>
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
        The previous attributes of the item you just put. If you used :meth:`~.PutItem.return_values_all_old`.

        :type: None or dict
        """
        if _is_dict(self.__attributes):  # pragma no branch (Defensive code)
            return _convert_db_to_dict(self.__attributes)

    @property
    def consumed_capacity(self):
        """
        The capacity consumed by the request. If you used :meth:`~.PutItem.return_consumed_capacity_total` or :meth:`~.PutItem.return_consumed_capacity_indexes`.

        :type: None or :class:`.ConsumedCapacity`
        """
        if _is_dict(self.__consumed_capacity):  # pragma no branch (Defensive code)
            return ConsumedCapacity(**self.__consumed_capacity)

    @property
    def item_collection_metrics(self):
        """
        Metrics about the collection of the item you just put. If a LSI was touched and you used :meth:`~.PutItem.return_item_collection_metrics_size`.

        :type: None or :class:`.ItemCollectionMetrics`
        """
        if _is_dict(self.__item_collection_metrics):  # pragma no branch (Defensive code)
            return ItemCollectionMetrics(**self.__item_collection_metrics)


class PutItem(Action):
    """
    The `PutItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html#API_PutItem_RequestParameters>`__.
    """

    def __init__(self, table_name, item):
        super(PutItem, self).__init__("PutItem")
        self.__table_name = table_name
        self.__item = item
        self.__condition_expression = ConditionExpression(self)
        self.__expression_attribute_names = ExpressionAttributeNames(self)
        self.__expression_attribute_values = ExpressionAttributeValues(self)
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__return_item_collection_metrics = ReturnItemCollectionMetrics(self)
        self.__return_values = ReturnValues(self)

    def build(self):
        data = {
            "TableName": self.__table_name,
            "Item": _convert_dict_to_db(self.__item),
        }
        data.update(self.__condition_expression.build())
        data.update(self.__expression_attribute_names.build())
        data.update(self.__expression_attribute_values.build())
        data.update(self.__return_consumed_capacity.build())
        data.update(self.__return_item_collection_metrics.build())
        data.update(self.__return_values.build())
        return data

    @staticmethod
    def Result(**kwds):
        return PutItemResponse(**kwds)

    @proxy
    def condition_expression(self, expression):
        """
        >>> connection(
        ...   PutItem(table, {"h": 1})
        ...     .condition_expression("#syn=:val")
        ...     .expression_attribute_name("syn", "gr")
        ...     .expression_attribute_value("val", 0)
        ... )
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
        """
        return self.__condition_expression.set(expression)

    @proxy
    def expression_attribute_name(self, synonym, name):
        """
        See :meth:`~.PutItem.condition_expression` for an example.
        """
        return self.__expression_attribute_names.add(synonym, name)

    @proxy
    def expression_attribute_value(self, name, value):
        """
        See :meth:`~.PutItem.condition_expression` for an example.
        """
        return self.__expression_attribute_values.add(name, value)

    @proxy
    def return_values_all_old(self):
        """
        >>> connection(
        ...   PutItem(table, {"h": 2})
        ...     .return_values_all_old()
        ... ).attributes
        {u'h': 2, u'gr': 0, u'gh': 0}
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
        @todo doctest (We need a table with a LSI)
        """
        return self.__return_item_collection_metrics.size()

    @proxy
    def return_item_collection_metrics_none(self):
        """
        @todo doctest (We need a table with a LSI)
        """
        return self.__return_item_collection_metrics.none()


class PutItemUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(PutItem("Table", {"hash": 42}).name, "PutItem")

    def test_item(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"value"}).build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "value"}},
            }
        )

    def test_return_values_none(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def test_return_values_all_old(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_values_all_old().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def test_return_consumed_capacity_total(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_consumed_capacity_total().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def test_return_consumed_capacity_indexes(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_consumed_capacity_indexes().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def test_return_consumed_capacity_none(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def test_return_item_collection_metrics_size(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_item_collection_metrics_size().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def test_return_item_collection_metrics_none(self):
        self.assertEqual(
            PutItem("Table", {"hash": u"h"}).return_item_collection_metrics_none().build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )

    def test_expression_attribute_value(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).expression_attribute_value("v", u"value").build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def test_expression_attribute_name(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).expression_attribute_name("n", "path").build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def test_condition_expression(self):
        self.assertEqual(
            PutItem("Table", {"hash": 42}).condition_expression("a=b").build(),
            {
                "TableName": "Table",
                "Item": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
            }
        )


class PutItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_simple_put(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"simple"}))

        self.assertEqual(self.connection(_lv.GetItem("Aaa", {"h": u"simple"})).item, {"h": u"simple"})

    def test_put_all_types(self):
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

    def test_return_old_values(self):
        self.connection(PutItem("Aaa", {"h": u"return", "a": b"yyy"}))

        r = self.connection(
            PutItem("Aaa", {"h": u"return", "b": b"xxx"}).return_values_all_old()
        )

        self.assertEqual(r.attributes, {"h": u"return", "a": b"yyy"})


class PutItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(PutItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_indexes(self):
        r = self.connection(_lv.PutItem(self.table, self.item).return_consumed_capacity_indexes())

        self.assertEqual(r.consumed_capacity.capacity_units, 3.0)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_item_collection_metrics_size(self):
        r = self.connection(_lv.PutItem(self.table, self.item).return_item_collection_metrics_size())

        self.assertEqual(r.item_collection_metrics.item_collection_key, {"tab_h": "0"})
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[0], 0.0)
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[1], 1.0)
