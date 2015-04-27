# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`UpdateItem`, the connection will return a :class:`UpdateItemResponse`:

>>> connection(UpdateItem(table, {"h": 0}).remove("a"))
<LowVoltage.actions.update_item.UpdateItemResponse ...>
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
    Key,
    ReturnConsumedCapacity,
    ReturnItemCollectionMetrics,
    ReturnValues,
    TableName
)
from .return_types import ConsumedCapacity, ItemCollectionMetrics, _is_dict


class UpdateItemResponse(object):
    """
    The `UpdateItem response <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_ResponseElements>`__.
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
        The (previous or new) attributes of the item you just updated. If you used :meth:`~UpdateItem.return_values_all_old`, :meth:`~UpdateItem.return_values_all_new`, :meth:`~UpdateItem.return_values_updated_old` or :meth:`~UpdateItem.return_values_updated_new`.

        :type: ``None`` or dict
        """
        if _is_dict(self.__attributes):
            return _convert_db_to_dict(self.__attributes)

    @property
    def consumed_capacity(self):
        """
        The capacity consumed by the request. If you used :meth:`~UpdateItem.return_consumed_capacity_total` or :meth:`~UpdateItem.return_consumed_capacity_indexes`.

        :type: ``None`` or :class:`.ConsumedCapacity`
        """
        if _is_dict(self.__consumed_capacity):
            return ConsumedCapacity(**self.__consumed_capacity)

    @property
    def item_collection_metrics(self):
        """
        Metrics about the collection of the item you just updated. If a LSI was touched and you used :meth:`~UpdateItem.return_item_collection_metrics_size`.

        :type: ``None`` or :class:`.ItemCollectionMetrics`
        """
        if _is_dict(self.__item_collection_metrics):
            return ItemCollectionMetrics(**self.__item_collection_metrics)


class UpdateItem(Action):
    """
    The `UpdateItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_RequestParameters>`__.
    """

    def __init__(self, table_name=None, key=None):
        """
        Passing ``table_name`` to the constructor is like calling :meth:`table_name` on the new instance.
        Passing ``key`` to the constructor is like calling :meth:`key` on the new instance.
        """
        super(UpdateItem, self).__init__("UpdateItem", UpdateItemResponse)
        self.__set = {}
        self.__remove = []
        self.__add = {}
        self.__delete = {}
        self.__condition_expression = ConditionExpression(self)
        self.__expression_attribute_names = ExpressionAttributeNames(self)
        self.__expression_attribute_values = ExpressionAttributeValues(self)
        self.__key = Key(self, key)
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__return_item_collection_metrics = ReturnItemCollectionMetrics(self)
        self.__return_values = ReturnValues(self)
        self.__table_name = TableName(self, table_name)

    @property
    def payload(self):
        data = {}
        update = []
        if self.__set:
            update.append("SET {}".format(", ".join("{}={}".format(n, v) for n, v in self.__set.iteritems())))
        if self.__remove:
            update.append("REMOVE {}".format(", ".join(self.__remove)))
        if self.__add:
            update.append("ADD {}".format(", ".join("{} :{}".format(n, v) for n, v in self.__add.iteritems())))
        if self.__delete:
            update.append("DELETE {}".format(", ".join("{} :{}".format(n, v) for n, v in self.__delete.iteritems())))
        if update:
            data["UpdateExpression"] = " ".join(update)
        data.update(self.__condition_expression.payload)
        data.update(self.__expression_attribute_names.payload)
        data.update(self.__expression_attribute_values.payload)
        data.update(self.__key.payload)
        data.update(self.__return_consumed_capacity.payload)
        data.update(self.__return_item_collection_metrics.payload)
        data.update(self.__return_values.payload)
        data.update(self.__table_name.payload)
        return data

    @proxy
    def table_name(self, table_name):
        """
        >>> connection(
        ...   UpdateItem(key={"h": 0})
        ...     .table_name(table)
        ...     .remove("a")
        ... )
        <LowVoltage.actions.update_item.UpdateItemResponse ...>
        """
        return self.__table_name.set(table_name)

    @proxy
    def key(self, key):
        """
        >>> connection(
        ...   UpdateItem(table_name=table)
        ...     .key({"h": 0})
        ...     .remove("a")
        ... )
        <LowVoltage.actions.update_item.UpdateItemResponse ...>
        """
        return self.__key.set(key)

    def set(self, attribute_name, value_name):
        """
        Add a value to SET as an attribute to UpdateExpression.
        As described in the `developer guide <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.SET>`__.

        >>> connection(PutItem(table, {"h": 0}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .set("a", ":forty_two")
        ...     .expression_attribute_value("forty_two", 42)
        ...     .return_values_all_new()
        ... ).attributes
        {u'a': 42, u'h': 0}
        """
        self.__set[attribute_name] = value_name
        return self

    def remove(self, path):
        """
        Add an attribute to REMOVE to UpdateExpression.
        As described in the `developer guide <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.REMOVE>`__.

        >>> connection(PutItem(table, {"h": 0, "a": 42}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .remove("a")
        ...     .return_values_all_new()
        ... ).attributes
        {u'h': 0}
        """
        self.__remove.append(path)
        return self

    def add(self, attribute_name, value_name):
        """
        Add a (set of) value(s) to ADD to a number (or a set) attribute to UpdateExpression.
        As described in the `developer guide <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.ADD>`__.

        >>> connection(PutItem(table, {"h": 0, "a": 42}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .add("a", "two")
        ...     .expression_attribute_value("two", 2)
        ...     .return_values_all_new()
        ... ).attributes
        {u'a': 44, u'h': 0}

        >>> connection(PutItem(table, {"h": 0, "a": {2, 3}}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .add("a", "vals")
        ...     .expression_attribute_value("vals", {1, 2})
        ...     .return_values_all_new()
        ... ).attributes
        {u'a': set([1, 2, 3]), u'h': 0}
        """
        self.__add[attribute_name] = value_name
        return self

    def delete(self, attribute_name, value_name):
        """
        Add a set of values to DELETE from a set attribute to UpdateExpression.
        As described in the `developer guide <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.DELETE>`__.

        >>> connection(PutItem(table, {"h": 0, "a": {1, 2, 3}}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .delete("a", "vals")
        ...     .expression_attribute_value("vals", {1, 2, 4})
        ...     .return_values_all_new()
        ... ).attributes
        {u'a': set([3]), u'h': 0}
        """
        self.__delete[attribute_name] = value_name
        return self

    @proxy
    def condition_expression(self, expression):
        """
        >>> connection(
        ...   UpdateItem(table, {"h": 1})
        ...     .remove("gh")
        ...     .condition_expression("#syn=:val")
        ...     .expression_attribute_name("syn", "gr")
        ...     .expression_attribute_value("val", 8)
        ... )
        <LowVoltage.actions.update_item.UpdateItemResponse ...>
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
        ...   UpdateItem(table, {"h": 5}).set("gh", "h").set("gr", "h")
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
        ...   UpdateItem(table, {"h": 4}).set("gh", "h").set("gr", "h")
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        3.0
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_none(self):
        """
        >>> print connection(
        ...   UpdateItem(table, {"h": 6}).set("gh", "h").set("gr", "h")
        ...     .return_consumed_capacity_none()
        ... ).consumed_capacity
        None
        """
        return self.__return_consumed_capacity.none()

    @proxy
    def return_item_collection_metrics_size(self):
        """
        >>> m = connection(
        ...   UpdateItem(table2, {"h": 0, "r1": 0}).set("a", "h")
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
        ...   UpdateItem(table2, {"h": 1, "r1": 0}).set("a", "h")
        ...     .return_item_collection_metrics_none()
        ... ).item_collection_metrics
        None
        """
        return self.__return_item_collection_metrics.none()

    @proxy
    def return_values_all_old(self):
        """
        >>> connection(PutItem(table, {"h": 0, "a": 1, "b": 2}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .set("a", ":v")
        ...     .expression_attribute_value("v", 2)
        ...     .return_values_all_old()
        ... ).attributes
        {u'a': 1, u'h': 0, u'b': 2}
        """
        return self.__return_values.all_old()

    @proxy
    def return_values_all_new(self):
        """
        >>> connection(PutItem(table, {"h": 0, "a": 1, "b": 2}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .set("a", ":v")
        ...     .expression_attribute_value("v", 2)
        ...     .return_values_all_new()
        ... ).attributes
        {u'a': 2, u'h': 0, u'b': 2}
        """
        return self.__return_values.all_new()

    @proxy
    def return_values_updated_old(self):
        """
        >>> connection(PutItem(table, {"h": 0, "a": 1, "b": 2}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .set("a", ":v")
        ...     .expression_attribute_value("v", 2)
        ...     .return_values_updated_old()
        ... ).attributes
        {u'a': 1}
        """
        return self.__return_values.updated_old()

    @proxy
    def return_values_updated_new(self):
        """
        >>> connection(PutItem(table, {"h": 0, "a": 1, "b": 2}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .set("a", ":v")
        ...     .expression_attribute_value("v", 2)
        ...     .return_values_updated_new()
        ... ).attributes
        {u'a': 2}
        """
        return self.__return_values.updated_new()

    @proxy
    def return_values_none(self):
        """
        >>> connection(PutItem(table, {"h": 0, "a": 1, "b": 2}))
        <LowVoltage.actions.put_item.PutItemResponse ...>
        >>> print connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .set("a", ":v")
        ...     .expression_attribute_value("v", 2)
        ...     .return_values_none()
        ... ).attributes
        None
        """
        return self.__return_values.none()


class UpdateItemUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(UpdateItem("Table", {"hash": 42}).name, "UpdateItem")

    def test_table_name_and_key(self):
        self.assertEqual(
            UpdateItem().table_name("Table").key({"hash": 42}).payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def test_constructor(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def test_set(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).set("a", ":v").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "SET a=:v",
            }
        )

    def test_several_sets(self):
        self.assertIn(
            UpdateItem("Table", {"hash": 42}).set("a", ":v").set("b", ":w").payload,
            [
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "SET a=:v, b=:w",
                },
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "SET b=:w, a=:v",
                }
            ]
        )

    def test_remove(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).remove("a").remove("b").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "REMOVE a, b",
            }
        )

    def test_add(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).add("a", "v").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "ADD a :v",
            }
        )

    def test_several_adds(self):
        self.assertIn(
            UpdateItem("Table", {"hash": 42}).add("a", "v").add("b", "w").payload,
            [
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "ADD a :v, b :w",
                },
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "ADD b :w, a :v",
                }
            ]
        )

    def test_delete(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).delete("a", "v").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "UpdateExpression": "DELETE a :v",
            }
        )

    def test_several_deletes(self):
        self.assertIn(
            UpdateItem("Table", {"hash": 42}).delete("a", "v").delete("b", "w").payload,
            [
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "DELETE a :v, b :w",
                },
                {
                    "TableName": "Table",
                    "Key": {"hash": {"N": "42"}},
                    "UpdateExpression": "DELETE b :w, a :v",
                }
            ]
        )

    def test_expression_attribute_value(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).expression_attribute_value("v", u"value").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def test_expression_attribute_name(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).expression_attribute_name("n", "path").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def test_condition_expression(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": 42}).condition_expression("a=b").payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
            }
        )

    def test_return_values_all_new(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_values_all_new().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_NEW",
            }
        )

    def test_return_values_all_old(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_values_all_old().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "ALL_OLD",
            }
        )

    def test_return_values_updated_new(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_values_updated_new().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_NEW",
            }
        )

    def test_return_values_updated_old(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_values_updated_old().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "UPDATED_OLD",
            }
        )

    def test_return_values_none(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_values_none().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def test_return_consumed_capacity_total(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_consumed_capacity_total().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "TOTAL",
            }
        )

    def test_return_consumed_capacity_indexes(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_consumed_capacity_indexes().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "INDEXES",
            }
        )

    def test_return_consumed_capacity_none(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_consumed_capacity_none().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def test_return_item_collection_metrics_size(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_item_collection_metrics_size().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "SIZE",
            }
        )

    def test_return_item_collection_metrics_none(self):
        self.assertEqual(
            UpdateItem("Table", {"hash": u"h"}).return_item_collection_metrics_none().payload,
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )


class UpdateItemResponseUnitTests(_tst.UnitTests):
    def test_all_none(self):
        r = UpdateItemResponse()
        self.assertIsNone(r.attributes)
        self.assertIsNone(r.consumed_capacity)
        self.assertIsNone(r.item_collection_metrics)

    def test_all_set(self):
        unprocessed_keys = object()
        r = UpdateItemResponse(Attributes={"h": {"S": "a"}}, ConsumedCapacity={}, ItemCollectionMetrics={})
        self.assertEqual(r.attributes, {"h": "a"})
        self.assertIsInstance(r.consumed_capacity, ConsumedCapacity)
        self.assertIsInstance(r.item_collection_metrics, ItemCollectionMetrics)
