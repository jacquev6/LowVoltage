# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage.testing as _tst
from .conversion import _convert_value_to_db, _convert_dict_to_db


def ScalarValue(name):
    class ScalarValue(object):
        def __init__(self, parent):
            self.__value = None
            self.__parent = parent

        def set(self, value):
            self.__value = value
            return self.__parent

        def build(self):
            data = {}
            if self.__value is not None:
                data[name] = self.__value
            return data

    return ScalarValue


def DictValue(name):
    class DictValue(object):
        def __init__(self, parent):
            self.__data = {}
            self.__parent = parent

        def add(self, name, value):
            self.__data[name] = value
            return self.__parent

        def build(self):
            data = {}
            if len(self.__data) != 0:
                data[name] = self.__data
            return data

    return DictValue


def CommaSeparatedStringsValue(name):
    class CommaSeparatedStringsValue(object):
        def __init__(self, parent):
            self.__values = []
            self.__parent = parent

        def add(self, *values):
            for value in values:
                if isinstance(value, basestring):
                    value = [value]
                self.__values.extend(value)
            return self.__parent

        def build(self):
            data = {}
            if len(self.__values) != 0:
                data[name] = ", ".join(self.__values)
            return data

    return CommaSeparatedStringsValue


class ConditionExpression(ScalarValue("ConditionExpression")):
    def set(self, expression):
        """
        Set the ConditionExpression, making the request conditional.
        It will raise a :class:`.ConditionalCheckFailedException` if the condition is not met.
        """
        return super(ConditionExpression, self).set(expression)


class ConsistentRead(ScalarValue("ConsistentRead")):
    def false(self):
        """
        Set ConsistentRead to False.
        The request will use eventually consistent reads.
        """
        return self.set(False)

    def true(self):
        """
        Set ConsistentRead to True.
        The request will use strong consistent reads.
        """
        return self.set(True)


class ExclusiveStartKey(ScalarValue("ExclusiveStartKey")):
    def set(self, key):
        """
        Set ExclusiveStartKey. The request will only scan items that are after this key.
        This is typically the :attr:`~.{}Response.last_evaluated_key` of a previous response.
        """
        return super(ExclusiveStartKey, self).set(_convert_dict_to_db(key))


class ExpressionAttributeNames(DictValue("ExpressionAttributeNames")):
    def add(self, synonym, name):
        """
        Add a synonym for an attribute name to ExpressionAttributeNames.
        Useful for attributes whose names don't play well with ProjectionExpression, ConditionExpression or UpdateExpression because they contain a dot or brackets.
        """
        return super(ExpressionAttributeNames, self).add("#" + synonym, name)


class ExpressionAttributeValues(DictValue("ExpressionAttributeValues")):
    def add(self, name, value):
        """
        Add a named value to ExpressionAttributeValues.
        """
        return super(ExpressionAttributeValues, self).add(":" + name, _convert_value_to_db(value))


class FilterExpression(ScalarValue("FilterExpression")):
    def set(self, expression):
        """
        Set the FilterExpression. The response will contain only items that match.
        """
        return super(FilterExpression, self).set(expression)


class Limit(ScalarValue("Limit")):
    def set(self, limit):
        """
        Set Limit. The request will scan at most this number of items.
        """
        return super(Limit, self).set(limit)


class ProjectionExpression(CommaSeparatedStringsValue("ProjectionExpression")):
    def add(self, *names):
        """
        Add name(s) to ProjectionExpression.
        The request will return only projected attributes.

        Note that this method accepts a variable number of names or iterables.  @todo A section in the user guide describing those methods.
        """
        return super(ProjectionExpression, self).add(*names)


class ReturnConsumedCapacity(ScalarValue("ReturnConsumedCapacity")):
    def indexes(self):
        """
        Set ReturnConsumedCapacity to INDEXES.
        The response will contain the capacity consumed by this request detailled on the table and the indexes.
        """
        return self.set("INDEXES")

    def none(self):
        """
        Set ReturnConsumedCapacity to NONE.
        The response will not contain the capacity consumed by this request.
        """
        return self.set("NONE")

    def total(self):
        """
        Set ReturnConsumedCapacity to TOTAL.
        The response will contain the total capacity consumed by this request.
        """
        return self.set("TOTAL")


class ReturnItemCollectionMetrics(ScalarValue("ReturnItemCollectionMetrics")):
    def none(self):
        """
        Set ReturnItemCollectionMetrics to NONE.
        The response will not contain any item collection metrics.
        """
        return self.set("NONE")

    def size(self):
        """
        Set ReturnItemCollectionMetrics to SIZE.
        If the table has a local secondary index, the response will contain metrics about the size of item collections that were touched.
        """
        return self.set("SIZE")


class ReturnValues(ScalarValue("ReturnValues")):
    def all_new(self):
        """
        Set ReturnValues to ALL_NEW.
        The response will contain all the attributes of the item in its new state.
        """
        return self.set("ALL_NEW")

    def all_old(self):
        """
        Set ReturnValues to ALL_OLD.
        The response will contain all the attributes of the item in its previous state.
        """
        return self.set("ALL_OLD")

    def none(self):
        """
        Set ReturnValues to NONE.
        The response will not include the attributes of the item.
        """
        return self.set("NONE")

    def updated_new(self):
        """
        Set ReturnValues to UPDATED_NEW.
        The response will contain the just-updated attributes of the item in its new state.
        """
        return self.set("UPDATED_NEW")

    def updated_old(self):
        """
        Set ReturnValues to UPDATED_OLD.
        The response will contain the just-updated attributes of the item in its previous state.
        """
        return self.set("UPDATED_OLD")


class Select(ScalarValue("Select")):
    def all_attributes(self):
        """
        Set Select to ALL_ATTRIBUTES. The response will contain all attributes of the matching items.
        """
        return self.set("ALL_ATTRIBUTES")

    def all_projected_attributes(self):
        """
        Set Select to ALL_PROJECTED_ATTRIBUTES. Usable only when querying an index.
        The response will contain the attributes of the matching items that are projected on the index.
        """
        return self.set("ALL_PROJECTED_ATTRIBUTES")

    def count(self):
        """
        Set Select to COUNT. The response will contain only the count of matching items.
        """
        return self.set("COUNT")


def proxy(class_or_method):
    def decorator(method):
        bases = {
            "condition_expression": ConditionExpression.set,
            "consistent_read_false": ConsistentRead.false,
            "consistent_read_true": ConsistentRead.true,
            "exclusive_start_key": ExclusiveStartKey.set,
            "expression_attribute_name": ExpressionAttributeNames.add,
            "expression_attribute_value": ExpressionAttributeValues.add,
            "filter_expression": FilterExpression.set,
            "limit": Limit.set,
            "project": ProjectionExpression.add,
            "return_consumed_capacity_indexes": ReturnConsumedCapacity.indexes,
            "return_consumed_capacity_none": ReturnConsumedCapacity.none,
            "return_consumed_capacity_total": ReturnConsumedCapacity.total,
            "return_item_collection_metrics_none": ReturnItemCollectionMetrics.none,
            "return_item_collection_metrics_size": ReturnItemCollectionMetrics.size,
            "return_values_all_new": ReturnValues.all_new,
            "return_values_all_old": ReturnValues.all_old,
            "return_values_none": ReturnValues.none,
            "return_values_updated_new": ReturnValues.updated_new,
            "return_values_updated_old": ReturnValues.updated_old,
            "select_all_attributes": Select.all_attributes,
            "select_all_projected_attributes": Select.all_projected_attributes,
            "select_count": Select.count,
        }

        args = ()

        if method.__name__ == "exclusive_start_key":
            args = class_or_method

        method.__doc__ = bases[method.__name__].__doc__.format(args) + "\n" + method.__doc__
        return method

    if isinstance(class_or_method, basestring):
        return decorator
    else:
        return decorator(class_or_method)
