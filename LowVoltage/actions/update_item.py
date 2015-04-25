# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
When given a :class:`UpdateItem`, the connection will return a :class:`UpdateItemResponse`:

>>> connection(UpdateItem(table, {"h": 0}).remove("a"))
<LowVoltage.actions.update_item.UpdateItemResponse object at ...>
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
        if _is_dict(self.__attributes):  # pragma no branch (Defensive code)
            return _convert_db_to_dict(self.__attributes)

    # @todo Remove '_' prefix from classes in .return_types.

    @property
    def consumed_capacity(self):
        """
        The capacity consumed by the request. If you used :meth:`~UpdateItem.return_consumed_capacity_total` or :meth:`~UpdateItem.return_consumed_capacity_indexes`.

        :type: ``None`` or :class:`.ConsumedCapacity`
        """
        if _is_dict(self.__consumed_capacity):  # pragma no branch (Defensive code)
            return ConsumedCapacity(**self.__consumed_capacity)

    @property
    def item_collection_metrics(self):
        """
        Metrics about the collection of the item you just updated. If a LSI was touched and you used :meth:`~UpdateItem.return_item_collection_metrics_size`.

        :type: ``None`` or :class:`.ItemCollectionMetrics`
        """
        if _is_dict(self.__item_collection_metrics):  # pragma no branch (Defensive code)
            return ItemCollectionMetrics(**self.__item_collection_metrics)


class UpdateItem(Action):
    """
    The `UpdateItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#API_UpdateItem_RequestParameters>`__.
    """

    def __init__(self, table_name, key):
        super(UpdateItem, self).__init__("UpdateItem", UpdateItemResponse)
        self.__table_name = table_name
        self.__key = key
        self.__set = {}
        self.__remove = []
        self.__add = {}
        self.__delete = {}
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
        data.update(self.__return_consumed_capacity.payload)
        data.update(self.__return_item_collection_metrics.payload)
        data.update(self.__return_values.payload)
        return data

    # @todo should we provide bundle methods for set, add, delete that do an implicit expression_attribute_value with a generated value_name?
    # @todo should we provide add_to_int (accepting an int), add_to_set and delete_from_set (accepting several ints, strs or binaries)?

    def set(self, attribute_name, value_name):
        """
        Add a value to SET as an attribute to UpdateExpression.
        As described in the `developer guide <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.SET>`__.

        >>> connection(PutItem(table, {"h": 0}))
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
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
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
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
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
        >>> connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .add("a", "two")
        ...     .expression_attribute_value("two", 2)
        ...     .return_values_all_new()
        ... ).attributes
        {u'a': 44, u'h': 0}

        >>> connection(PutItem(table, {"h": 0, "a": {2, 3}}))
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
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

    # @todo What happens if you add and delete from the same set? The same values?
    # @todo What happens if you add twice to the same number?
    # @todo What happens if you add twice to the same set?
    # @todo What happens if you delete twice from the same set?
    # @todo What happens if you set the same attribute twice?
    # @todo What happens if you remove the same attribute twice?
    # @todo What happens if you set and remove the same attribute?
    # @todo Should we handle those cases in a "the last call wins" manner like we do for return_values_xxx?

    def delete(self, attribute_name, value_name):
        """
        Add a set of values to DELETE from a set attribute to UpdateExpression.
        As described in the `developer guide <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Modifying.html#Expressions.Modifying.UpdateExpressions.DELETE>`__.

        >>> connection(PutItem(table, {"h": 0, "a": {1, 2, 3}}))
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
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
        ...     .expression_attribute_value("val", 0)
        ... )
        <LowVoltage.actions.update_item.UpdateItemResponse object at ...>
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
        >>> connection(PutItem(table, {"h": 0, "a": 1, "b": 2}))
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
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
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
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
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
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
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
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
        <LowVoltage.actions.put_item.PutItemResponse object at ...>
        >>> print connection(
        ...   UpdateItem(table, {"h": 0})
        ...     .set("a", ":v")
        ...     .expression_attribute_value("v", 2)
        ...     .return_values_none()
        ... ).attributes
        None
        """
        return self.__return_values.none()

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


class UpdateItemUnitTests(_tst.UnitTests):
    def test_name(self):
        self.assertEqual(UpdateItem("Table", {"hash": 42}).name, "UpdateItem")

    def test_key(self):
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


class UpdateItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def test_set(self):
        r = self.connection(
            _lv.UpdateItem("Aaa", {"h": u"set"})
                .set("a", ":v")
                .set("#p", ":w")
                .expression_attribute_value("v", "aaa")
                .expression_attribute_value("w", "bbb")
                .expression_attribute_name("p", "b")
        )

        self.assertEqual(
            self.connection(_lv.GetItem("Aaa", {"h": u"set"})).item,
            {"h": "set", "a": "aaa", "b": "bbb"}
        )

    def test_complex_update(self):
        self.connection(
            _lv.PutItem(
                "Aaa",
                {
                    "h": u"complex",
                    "a": "a",
                    "b": "b",
                    "c": "c",
                    "d": set([41, 43]),
                    "e": 42,
                    "f": set([41, 42, 43]),
                    "g": set([39, 40]),
                }
            )
        )

        r = self.connection(
            _lv.UpdateItem("Aaa", {"h": u"complex"})
                .set("a", ":s")
                .set("b", ":i")
                .remove("c")
                .add("d", "s")
                .add("e", "i")
                .delete("f", "s")
                .delete("g", "s")
                .expression_attribute_value("s", set([42, 43]))
                .expression_attribute_value("i", 52)
                .return_values_all_new()
        )

        self.assertEqual(
            r.attributes,
            {
                "h": u"complex",
                "a": set([42, 43]),
                "b": 52,
                "d": set([41, 42, 43]),
                "e": 94,
                "f": set([41]),
                "g": set([39, 40]),
            }
        )

    def test_condition_expression(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"expr", "a": 42, "b": 42}))

        r = self.connection(
            _lv.UpdateItem("Aaa", {"h": u"expr"})
                .set("checked", ":true")
                .expression_attribute_value("true", True)
                .condition_expression("a=b")
                .return_values_all_new()
        )

        self.assertEqual(
            r.attributes,
            {"h": u"expr", "a": 42, "b": 42, "checked": True}
        )


class UpdateItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def tearDown(self):
        self.connection(_lv.DeleteItem(self.table, self.tab_key))
        super(UpdateItemConnectedIntegTests, self).tearDown()

    def test_return_consumed_capacity_indexes_without_indexed_attribute(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("a", ":a").expression_attribute_value("a", "a")
                .return_consumed_capacity_indexes()
        )

        self.assertEqual(r.consumed_capacity.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_consumed_capacity_indexes(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("gsi_h", ":gsi_h").expression_attribute_value("gsi_h", u"1")
                .set("gsi_r", ":gsi_r").expression_attribute_value("gsi_r", 1)
                .set("lsi_r", ":lsi_r").expression_attribute_value("lsi_r", 2)
                .return_consumed_capacity_indexes()
        )

        self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.capacity_units, 3.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_consumed_capacity_indexes_with_locally_indexed_attribute_only(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("lsi_r", ":lsi_r").expression_attribute_value("lsi_r", 2)
                .return_consumed_capacity_indexes()
        )

        self.assertEqual(r.consumed_capacity.capacity_units, 2.0)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_consumed_capacity_indexes_with_globally_indexed_attribute_only(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("gsi_h", ":gsi_h").expression_attribute_value("gsi_h", u"1")
                .set("gsi_r", ":gsi_r").expression_attribute_value("gsi_r", 1)
                .return_consumed_capacity_indexes()
        )

        self.assertEqual(r.consumed_capacity.capacity_units, 2.0)
        self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.local_secondary_indexes, None)
        self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
        self.assertEqual(r.consumed_capacity.table_name, self.table)

    def test_return_item_collection_metrics_size(self):
        r = self.connection(
            _lv.UpdateItem(self.table, self.tab_key)
                .set("a", ":a").expression_attribute_value("a", "a")
                .return_item_collection_metrics_size()
        )

        self.assertEqual(r.item_collection_metrics.item_collection_key, {"tab_h": u"0"})
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[0], 0.0)
        self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[1], 1.0)
