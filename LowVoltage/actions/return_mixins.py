# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage.testing as _tst


class ReturnOldValuesMixin(object):
    def __init__(self):
        self.__return_values = None

    def _build_return_values(self):
        data = {}
        if self.__return_values:
            data["ReturnValues"] = self.__return_values
        return data

    def return_values_all_old(self):
        """
        Set ReturnValues to ALL_OLD.
        The result will contain all the attributes of the item in its previous state.

            >>> connection(PutItem(table, {"h": 0, "a": 1}))
            <LowVoltage.actions.put_item.Result object at ...>
            >>> connection(
            ...   DeleteItem(table, {"h": 0})
            ...     .return_values_all_old()
            ... ).attributes
            {u'a': 1, u'h': 0}
        """
        return self._set_return_values("ALL_OLD")

    def return_values_none(self):
        """
        Set ReturnValues to NONE.
        The result will not include the attributes of the item.

            >>> connection(PutItem(table, {"h": 0, "a": 1}))
            <LowVoltage.actions.put_item.Result object at ...>
            >>> print connection(
            ...   DeleteItem(table, {"h": 0})
            ...     .return_values_none()
            ... ).attributes
            None
        """
        return self._set_return_values("NONE")

    def _set_return_values(self, value):
        self.__return_values = value
        return self


class ReturnOldValuesMixinUnitTests(_tst.UnitTests):
    def testDefault(self):
        self.assertEqual(
            ReturnOldValuesMixin()._build_return_values(),
            {}
        )

    def testReturnValuesAllOld(self):
        self.assertEqual(
            ReturnOldValuesMixin().return_values_all_old()._build_return_values(),
            {"ReturnValues": "ALL_OLD"}
        )

    def testReturnValuesNone(self):
        self.assertEqual(
            ReturnOldValuesMixin().return_values_none()._build_return_values(),
            {"ReturnValues": "NONE"}
        )


class ReturnValuesMixin(ReturnOldValuesMixin):
    def return_values_all_new(self):
        """
        Set ReturnValues to ALL_NEW.
        The result will contain all the attributes of the item in its new state.

            >>> connection(PutItem(table, {"h": 0, "a": 1}))
            <LowVoltage.actions.put_item.Result object at ...>
            >>> connection(
            ...   UpdateItem(table, {"h": 0})
            ...     .add("a", "one")
            ...     .expression_attribute_value("one", 1)
            ...     .return_values_all_new()
            ... ).attributes
            {u'a': 2, u'h': 0}
        """
        return self._set_return_values("ALL_NEW")

    def return_values_updated_new(self):
        """
        Set ReturnValues to UPDATED_NEW.
        The result will contain the just-updated attributes of the item in its new state.

            >>> connection(PutItem(table, {"h": 0, "a": 1}))
            <LowVoltage.actions.put_item.Result object at ...>
            >>> connection(
            ...   UpdateItem(table, {"h": 0})
            ...     .add("a", "one")
            ...     .expression_attribute_value("one", 1)
            ...     .return_values_updated_new()
            ... ).attributes
            {u'a': 2}
        """
        return self._set_return_values("UPDATED_NEW")

    def return_values_updated_old(self):
        """
        Set ReturnValues to UPDATED_OLD.
        The result will contain the just-updated attributes of the item in its previous state.

            >>> connection(PutItem(table, {"h": 0, "a": 1}))
            <LowVoltage.actions.put_item.Result object at ...>
            >>> connection(
            ...   UpdateItem(table, {"h": 0})
            ...     .add("a", "one")
            ...     .expression_attribute_value("one", 1)
            ...     .return_values_updated_old()
            ... ).attributes
            {u'a': 1}
        """
        return self._set_return_values("UPDATED_OLD")


class ReturnValuesMixinUnitTests(_tst.UnitTests):
    def testDefault(self):
        self.assertEqual(
            ReturnValuesMixin()._build_return_values(),
            {}
        )

    def testReturnValuesAllNew(self):
        self.assertEqual(
            ReturnValuesMixin().return_values_all_new()._build_return_values(),
            {"ReturnValues": "ALL_NEW"}
        )

    def testReturnValuesUpdatedNew(self):
        self.assertEqual(
            ReturnValuesMixin().return_values_updated_new()._build_return_values(),
            {"ReturnValues": "UPDATED_NEW"}
        )

    def testReturnValuesAllOld(self):
        self.assertEqual(
            ReturnValuesMixin().return_values_all_old()._build_return_values(),
            {"ReturnValues": "ALL_OLD"}
        )

    def testReturnValuesUpdatedOld(self):
        self.assertEqual(
            ReturnValuesMixin().return_values_updated_old()._build_return_values(),
            {"ReturnValues": "UPDATED_OLD"}
        )

    def testReturnValuesNone(self):
        self.assertEqual(
            ReturnValuesMixin().return_values_none()._build_return_values(),
            {"ReturnValues": "NONE"}
        )


class ReturnConsumedCapacityMixin(object):
    def __init__(self):
        self.__return_consumed_capacity = None

    def _build_return_consumed_capacity(self):
        data = {}
        if self.__return_consumed_capacity:
            data["ReturnConsumedCapacity"] = self.__return_consumed_capacity
        return data

    def return_consumed_capacity_total(self):
        """
        Set ReturnConsumedCapacity to TOTAL.
        The result will contain the total capacity consumed by this request.

            >>> connection(
            ...   PutItem(table, {"h": 0})
            ...     .return_consumed_capacity_total()
            ... ).consumed_capacity.capacity_units
            1.0
        """
        return self._set_return_consumed_capacity("TOTAL")

    def return_consumed_capacity_indexes(self):
        """
        Set ReturnConsumedCapacity to INDEXES.
        The result will contain the capacity consumed by this request detailled on the table and the indexes.

            >>> c = connection(
            ...   PutItem(table, {"h": 0, "gh": 0, "gr": 0})
            ...     .return_consumed_capacity_indexes()
            ... ).consumed_capacity
            >>> c.capacity_units
            2.0
            >>> c.table.capacity_units
            1.0
            >>> c.global_secondary_indexes["gsi"].capacity_units
            1.0
        """
        return self._set_return_consumed_capacity("INDEXES")

    def return_consumed_capacity_none(self):
        """
        Set ReturnConsumedCapacity to NONE.
        The result will not contain the capacity consumed by this request.

            >>> print connection(
            ...   PutItem(table, {"h": 0})
            ...     .return_consumed_capacity_none()
            ... ).consumed_capacity
            None
        """
        return self._set_return_consumed_capacity("NONE")

    def _set_return_consumed_capacity(self, value):
        self.__return_consumed_capacity = value
        return self


class ReturnConsumedCapacityMixinUnitTests(_tst.UnitTests):
    def testDefault(self):
        self.assertEqual(
            ReturnConsumedCapacityMixin()._build_return_consumed_capacity(),
            {}
        )

    def testReturnIndexesConsumedCapacity(self):
        self.assertEqual(
            ReturnConsumedCapacityMixin().return_consumed_capacity_indexes()._build_return_consumed_capacity(),
            {"ReturnConsumedCapacity": "INDEXES"}
        )

    def testReturnTotalConsumedCapacity(self):
        self.assertEqual(
            ReturnConsumedCapacityMixin().return_consumed_capacity_total()._build_return_consumed_capacity(),
            {"ReturnConsumedCapacity": "TOTAL"}
        )

    def testReturnNoConsumedCapacity(self):
        self.assertEqual(
            ReturnConsumedCapacityMixin().return_consumed_capacity_none()._build_return_consumed_capacity(),
            {"ReturnConsumedCapacity": "NONE"}
        )


class ReturnItemCollectionMetricsMixin(object):
    def __init__(self):
        self.__return_item_collection_metrics = None

    def _build_return_item_collection_metrics(self):
        data = {}
        if self.__return_item_collection_metrics:
            data["ReturnItemCollectionMetrics"] = self.__return_item_collection_metrics
        return data

    def return_item_collection_metrics_size(self):
        """
        Set ReturnItemCollectionMetrics to SIZE.
        If the table has a local secondary index, the result will contain size item collection metrics.
        """
        return self._set_return_item_collection_metrics("SIZE")

    def return_item_collection_metrics_none(self):
        """
        Set ReturnItemCollectionMetrics to NONE.
        The result will not contain any item collection metrics.

            >>> print connection(
            ...   PutItem(table, {"h": 0})
            ...     .return_item_collection_metrics_none()
            ... ).item_collection_metrics
            None
        """
        return self._set_return_item_collection_metrics("NONE")

    def _set_return_item_collection_metrics(self, value):
        self.__return_item_collection_metrics = value
        return self


class ReturnItemCollectionMetricsMixinUnitTests(_tst.UnitTests):
    def testDefault(self):
        self.assertEqual(
            ReturnItemCollectionMetricsMixin()._build_return_item_collection_metrics(),
            {}
        )

    def testReturnSizeItemCollectionMetrics(self):
        self.assertEqual(
            ReturnItemCollectionMetricsMixin().return_item_collection_metrics_size()._build_return_item_collection_metrics(),
            {"ReturnItemCollectionMetrics": "SIZE"}
        )

    def testReturnItemCollectionMetricsNone(self):
        self.assertEqual(
            ReturnItemCollectionMetricsMixin().return_item_collection_metrics_none()._build_return_item_collection_metrics(),
            {"ReturnItemCollectionMetrics": "NONE"}
        )
