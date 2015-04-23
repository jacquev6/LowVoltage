# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage.testing as _tst


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
        It will raise a :class:`ConditionalCheckFailedException` if the condition is not met.
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
        Set the FilterExpression.
        """
        return super(FilterExpression, self).set(expression)


class ProjectionExpression(CommaSeparatedStringsValue("ProjectionExpression")):
    def add(self, *names):
        """
        Add name(s) to ProjectionExpression.
        """
        return super(ProjectionExpression, self).add(*names)


class ReturnConsumedCapacity(ScalarValue("ReturnConsumedCapacity")):
    def indexes(self):
        """
        Set ReturnConsumedCapacity to INDEXES.
        The result will contain the capacity consumed by this request detailled on the table and the indexes.
        """
        return self.set("INDEXES")

    def none(self):
        """
        Set ReturnConsumedCapacity to NONE.
        The result will not contain the capacity consumed by this request.
        """
        return self.set("NONE")

    def total(self):
        """
        Set ReturnConsumedCapacity to TOTAL.
        The result will contain the total capacity consumed by this request.
        """
        return self.set("TOTAL")


class ReturnItemCollectionMetrics(ScalarValue("ReturnItemCollectionMetrics")):
    def none(self):
        """
        Set ReturnItemCollectionMetrics to NONE.
        The result will not contain any item collection metrics.
        """
        return self.set("NONE")

    def size(self):
        """
        Set ReturnItemCollectionMetrics to SIZE.
        If the table has a local secondary index, the result will contain size item collection metrics.
        """
        return self.set("SIZE")


class ReturnValues(ScalarValue("ReturnValues")):
    def all_new(self):
        """
        Set ReturnValues to ALL_NEW.
        The result will contain all the attributes of the item in its new state.
        """
        return self.set("ALL_NEW")

    def all_old(self):
        """
        Set ReturnValues to ALL_OLD.
        The result will contain all the attributes of the item in its previous state.
        """
        return self.set("ALL_OLD")

    def none(self):
        """
        Set ReturnValues to NONE.
        The result will not include the attributes of the item.
        """
        return self.set("NONE")

    def updated_new(self):
        """
        Set ReturnValues to UPDATED_NEW.
        The result will contain the just-updated attributes of the item in its new state.
        """
        return self.set("UPDATED_NEW")

    def updated_old(self):
        """
        Set ReturnValues to UPDATED_OLD.
        The result will contain the just-updated attributes of the item in its previous state.
        """
        return self.set("UPDATED_OLD")


def proxy(method):
    bases = {
        "condition_expression": ConditionExpression.set,
        "consistent_read_false": ConsistentRead.false,
        "consistent_read_true": ConsistentRead.true,
        "expression_attribute_name": ExpressionAttributeNames.add,
        "expression_attribute_value": ExpressionAttributeValues.add,
        "filter_expression": FilterExpression.set,
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
    }

    method.__doc__ = bases[method.__name__].__doc__ + "\n" + method.__doc__
    return method
