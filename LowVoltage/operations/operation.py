# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

from LowVoltage.operations.conversion import _convert_dict_to_db, _convert_value_to_db, _convert_db_to_dict, _convert_db_to_value


class Operation(object):
    def __init__(self, operation):
        self.__operation = operation

    @property
    def name(self):
        return self.__operation


class OperationProxy(object):
    def __init__(self, operation):
        self._operation = operation

    def __getattr__(self, name):
        return getattr(self._operation, name)


class ReturnOldValuesMixin(object):
    def __init__(self):
        self.__return_values = None

    def _build_return_values(self, data):
        if self.__return_values:
            data["ReturnValues"] = self.__return_values

    def return_values_all_old(self):
        return self._set_return_values("ALL_OLD")

    def return_values_none(self):
        return self._set_return_values("NONE")

    def _set_return_values(self, value):
        self.__return_values = value
        return self


class ReturnValuesMixin(ReturnOldValuesMixin):
    def return_values_all_new(self):
        return self._set_return_values("ALL_NEW")

    def return_values_updated_new(self):
        return self._set_return_values("UPDATED_NEW")

    def return_values_updated_old(self):
        return self._set_return_values("UPDATED_OLD")


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
