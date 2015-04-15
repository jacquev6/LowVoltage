# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest


class ReturnOldValuesMixin(object):
    def __init__(self):
        self.__return_values = None

    def _build_return_values(self):
        data = {}
        if self.__return_values:
            data["ReturnValues"] = self.__return_values
        return data

    def return_values_all_old(self):
        return self._set_return_values("ALL_OLD")

    def return_values_none(self):
        return self._set_return_values("NONE")

    def _set_return_values(self, value):
        self.__return_values = value
        return self


class ReturnOldValuesMixinUnitTests(unittest.TestCase):
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
        return self._set_return_values("ALL_NEW")

    def return_values_updated_new(self):
        return self._set_return_values("UPDATED_NEW")

    def return_values_updated_old(self):
        return self._set_return_values("UPDATED_OLD")


class ReturnValuesMixinUnitTests(unittest.TestCase):
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
        return self._set_return_consumed_capacity("TOTAL")

    def return_consumed_capacity_indexes(self):
        return self._set_return_consumed_capacity("INDEXES")

    def return_consumed_capacity_none(self):
        return self._set_return_consumed_capacity("NONE")

    def _set_return_consumed_capacity(self, value):
        self.__return_consumed_capacity = value
        return self


class ReturnConsumedCapacityMixinUnitTests(unittest.TestCase):
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
        return self._set_return_item_collection_metrics("SIZE")

    def return_item_collection_metrics_none(self):
        return self._set_return_item_collection_metrics("NONE")

    def _set_return_item_collection_metrics(self, value):
        self.__return_item_collection_metrics = value
        return self


class ReturnItemCollectionMetricsMixinUnitTests(unittest.TestCase):
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
