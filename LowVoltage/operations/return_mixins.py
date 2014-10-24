# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

class ReturnConsumedCapacityMixin(object):
    def __init__(self):
        self.__return_consumed_capacity = None

    def _build_return_consumed_capacity(self, data):
        if self.__return_consumed_capacity:
            data["ReturnConsumedCapacity"] = self.__return_consumed_capacity

    def return_consumed_capacity_total(self):
        return self._set_return_consumed_capacity("TOTAL")

    def return_consumed_capacity_indexes(self):
        return self._set_return_consumed_capacity("INDEXES")

    def return_consumed_capacity_none(self):
        return self._set_return_consumed_capacity("NONE")

    def _set_return_consumed_capacity(self, value):
        self.__return_consumed_capacity = value
        return self


class ReturnItemCollectionMetricsMixin(object):
    def __init__(self):
        self.__return_item_collection_metrics = None

    def _build_return_item_collection_metrics(self, data):
        if self.__return_item_collection_metrics:
            data["ReturnItemCollectionMetrics"] = self.__return_item_collection_metrics

    def return_item_collection_metrics_size(self):
        return self._set_return_item_collection_metrics("SIZE")

    def return_item_collection_metrics_none(self):
        return self._set_return_item_collection_metrics("NONE")

    def _set_return_item_collection_metrics(self, value):
        self.__return_item_collection_metrics = value
        return self
