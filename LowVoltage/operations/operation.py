# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import numbers
import unittest


class Operation(object):
    def __init__(self, operation):
        self.__operation = operation

    @property
    def name(self):
        return self.__operation

    def _convert_dict(self, attributes):
        return {
            key: self._convert_value(val)
            for key, val in attributes.iteritems()
        }

    def _convert_value(self, value):
        if isinstance(value, basestring):
            return {"S": value}
        elif isinstance(value, numbers.Integral):
            return {"N": str(value)}
        else:
            assert len(value) > 0
            if isinstance(value[0], basestring):
                return {"SS": value}
            elif isinstance(value[0], numbers.Integral):
                return {"NS": [str(n) for n in value]}
            else:
                assert False  # pragma no cover


class ExpectedMixin(object):
    def __init__(self):
        self.__conditional_operator = None
        self.__expected = {}

    def _build_expected(self, data):
        if self.__conditional_operator:
            data["ConditionalOperator"] = self.__conditional_operator
        if self.__expected:
            data["Expected"] = self.__expected

    def conditional_operator_and(self):
        return self._set_conditional_operator("AND")

    def conditional_operator_or(self):
        return self._set_conditional_operator("OR")

    def _set_conditional_operator(self, value):
        self.__conditional_operator = value
        return self

    def expect_eq(self, name, value):
        return self._add_expected(name, "EQ", [value])

    def expect_ne(self, name, value):
        return self._add_expected(name, "NE", [value])

    def expect_le(self, name, value):
        return self._add_expected(name, "LE", [value])

    def expect_lt(self, name, value):
        return self._add_expected(name, "LT", [value])

    def expect_ge(self, name, value):
        return self._add_expected(name, "GE", [value])

    def expect_gt(self, name, value):
        return self._add_expected(name, "GT", [value])

    def expect_not_null(self, name):
        return self._add_expected(name, "NOT_NULL")

    def expect_null(self, name):
        return self._add_expected(name, "NULL")

    def expect_contains(self, name, value):
        return self._add_expected(name, "CONTAINS", [value])

    def expect_not_contains(self, name, value):
        return self._add_expected(name, "NOT_CONTAINS", [value])

    def expect_begins_with(self, name, value):
        return self._add_expected(name, "BEGINS_WITH", [value])

    def expect_in(self, name, values):
        return self._add_expected(name, "IN", values)

    def expect_between(self, name, low, high):
        return self._add_expected(name, "BETWEEN", [low, high])

    def _add_expected(self, name, operator, value_list=None):
        data = {"ComparisonOperator": operator}
        if value_list:
            data["AttributeValueList"] = [self._convert_value(value) for value in value_list]
        self.__expected[name] = data
        return self


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
