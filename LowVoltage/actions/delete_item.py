# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .action import Action
from .conversion import _convert_dict_to_db, _convert_db_to_dict
from .expression_mixins import ExpressionAttributeNamesMixin, ExpressionAttributeValuesMixin, ConditionExpressionMixin
from .next_gen_mixins import proxy, ReturnValues, ReturnConsumedCapacity, ReturnItemCollectionMetrics
from .return_types import ConsumedCapacity_, ItemCollectionMetrics_, _is_dict


class DeleteItem(
    Action,
    ExpressionAttributeNamesMixin,
    ExpressionAttributeValuesMixin,
    ConditionExpressionMixin,
):
    """
    The `DeleteItem request <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteItem.html#API_DeleteItem_RequestParameters>`__

    Note that deleting the same item twice is not an error (deleting is idempotent). To know if an item was actually deleted, use :meth:`return_values_all_old`:

        >>> connection(
        ...   DeleteItem(table, {"h": 0})
        ...     .return_values_all_old()
        ... ).attributes
        {u'h': 0, u'gr': 0, u'gh': 0}
        >>> print connection(
        ...   DeleteItem(table, {"h": 0})
        ...     .return_values_all_old()
        ... ).attributes
        None
    """

    class Result(object):
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
            self.attributes = None
            "@todo Document"
            if _is_dict(Attributes):  # pragma no branch (Defensive code)
                self.attributes = _convert_db_to_dict(Attributes)

            self.consumed_capacity = None
            "@todo Document"
            if _is_dict(ConsumedCapacity):  # pragma no branch (Defensive code)
                self.consumed_capacity = ConsumedCapacity_(**ConsumedCapacity)

            self.item_collection_metrics = None
            "@todo Document"
            if _is_dict(ItemCollectionMetrics):  # pragma no branch (Defensive code)
                self.item_collection_metrics = ItemCollectionMetrics_(**ItemCollectionMetrics)

    def __init__(self, table_name, key):
        super(DeleteItem, self).__init__("DeleteItem")
        self.__table_name = table_name
        self.__key = key
        self.__return_values = ReturnValues(self)
        self.__return_consumed_capacity = ReturnConsumedCapacity(self)
        self.__return_item_collection_metrics = ReturnItemCollectionMetrics(self)
        ExpressionAttributeNamesMixin.__init__(self)
        ExpressionAttributeValuesMixin.__init__(self)
        ConditionExpressionMixin.__init__(self)

    def build(self):
        data = {
            "TableName": self.__table_name,
            "Key": _convert_dict_to_db(self.__key),
        }
        data.update(self.__return_values.build())
        data.update(self.__return_consumed_capacity.build())
        data.update(self.__return_item_collection_metrics.build())
        data.update(self._build_expression_attribute_names())
        data.update(self._build_expression_attribute_values())
        data.update(self._build_condition_expression())
        return data

    @proxy
    def return_values_all_old(self):
        """
        >>> connection(
        ...   DeleteItem(table, {"h": 1})
        ...     .return_values_all_old()
        ... ).attributes
        {u'h': 1, u'gr': 0, u'gh': 0}
        """
        return self.__return_values.all_old()

    @proxy
    def return_values_none(self):
        """
        >>> print connection(
        ...   DeleteItem(table, {"h": 2})
        ...     .return_values_none()
        ... ).attributes
        None
        """
        return self.__return_values.none()

    @proxy
    def return_consumed_capacity_total(self):
        """
        >>> connection(
        ...   DeleteItem(table, {"h": 3})
        ...     .return_consumed_capacity_total()
        ... ).consumed_capacity.capacity_units
        2.0
        """
        return self.__return_consumed_capacity.total()

    @proxy
    def return_consumed_capacity_indexes(self):
        """
        >>> c = connection(
        ...   DeleteItem(table, {"h": 4})
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
        ...   DeleteItem(table, {"h": 5})
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


class DeleteItemUnitTests(_tst.UnitTests):
    def testName(self):
        self.assertEqual(DeleteItem("Table", {"hash": 42}).name, "DeleteItem")

    def testKey(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
            }
        )

    def testReturnValuesNone(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_values_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnValues": "NONE",
            }
        )

    def testReturnConsumedCapacityNone(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_consumed_capacity_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnConsumedCapacity": "NONE",
            }
        )

    def testReturnItemCollectionMetricsNone(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": u"h"}).return_item_collection_metrics_none().build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"S": "h"}},
                "ReturnItemCollectionMetrics": "NONE",
            }
        )

    def testExpressionAttributeValue(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).expression_attribute_value("v", u"value").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeValues": {":v": {"S": "value"}},
            }
        )

    def testExpressionAttributeName(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).expression_attribute_name("n", "path").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ExpressionAttributeNames": {"#n": "path"},
            }
        )

    def testConditionExpression(self):
        self.assertEqual(
            DeleteItem("Table", {"hash": 42}).condition_expression("a=b").build(),
            {
                "TableName": "Table",
                "Key": {"hash": {"N": "42"}},
                "ConditionExpression": "a=b",
            }
        )


class DeleteItemLocalIntegTests(_tst.LocalIntegTestsWithTableH):
    def testSimpleDelete(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"simple", "a": "yyy"}))

        r = self.connection(_lv.DeleteItem("Aaa", {"h": u"simple"}))

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)

    def testReturnOldValues(self):
        self.connection(_lv.PutItem("Aaa", {"h": u"return", "a": "yyy"}))

        r = self.connection(_lv.DeleteItem("Aaa", {"h": u"return"}).return_values_all_old())

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, {"h": "return", "a": "yyy"})
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics, None)


class DeleteItemConnectedIntegTests(_tst.ConnectedIntegTestsWithTable):
    def setUp(self):
        super(DeleteItemConnectedIntegTests, self).setUp()
        self.connection(_lv.PutItem(self.table, self.item))

    def test_return_consumed_capacity_indexes(self):
        r = self.connection(_lv.DeleteItem(self.table, self.tab_key).return_consumed_capacity_indexes())

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity.capacity_units, 3.0)
            self.assertEqual(r.consumed_capacity.global_secondary_indexes["gsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.local_secondary_indexes["lsi"].capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.table.capacity_units, 1.0)
            self.assertEqual(r.consumed_capacity.table_name, self.table)
            self.assertEqual(r.item_collection_metrics, None)

    def test_return_item_collection_metrics_size(self):
        r = self.connection(_lv.DeleteItem(self.table, self.tab_key).return_item_collection_metrics_size())

        with _tst.cover("r", r) as r:
            self.assertEqual(r.attributes, None)
            self.assertEqual(r.consumed_capacity, None)
            self.assertEqual(r.item_collection_metrics.item_collection_key, {"tab_h": u"0"})
            self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[0], 0.0)
            self.assertEqual(r.item_collection_metrics.size_estimate_range_gb[1], 1.0)
