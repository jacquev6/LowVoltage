# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage.testing as _tst


def SimpleValue(name):
    class SimpleValue(object):
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
    return SimpleValue


class ReturnValues(SimpleValue("ReturnValues")):
    def all_old(self):
        """
        Set ReturnValues to ALL_OLD.
        The result will contain all the attributes of the item in its previous state.
        """
        return self.set("ALL_OLD")

    def all_new(self):
        """
        Set ReturnValues to ALL_NEW.
        The result will contain all the attributes of the item in its new state.
        """
        return self.set("ALL_NEW")

    def updated_old(self):
        """
        Set ReturnValues to UPDATED_OLD.
        The result will contain the just-updated attributes of the item in its previous state.
        """
        return self.set("UPDATED_OLD")

    def updated_new(self):
        """
        Set ReturnValues to UPDATED_NEW.
        The result will contain the just-updated attributes of the item in its new state.
        """
        return self.set("UPDATED_NEW")

    def none(self):
        """
        Set ReturnValues to NONE.
        The result will not include the attributes of the item.
        """
        return self.set("NONE")


class ReturnConsumedCapacity(SimpleValue("ReturnConsumedCapacity")):
    def total(self):
        """
        Set ReturnConsumedCapacity to TOTAL.
        The result will contain the total capacity consumed by this request.
        """
        return self.set("TOTAL")

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


class ReturnItemCollectionMetrics(SimpleValue("ReturnItemCollectionMetrics")):
    def size(self):
        """
        Set ReturnItemCollectionMetrics to SIZE.
        If the table has a local secondary index, the result will contain size item collection metrics.
        """
        return self.set("SIZE")

    def none(self):
        """
        Set ReturnItemCollectionMetrics to NONE.
        The result will not contain any item collection metrics.
        """
        return self.set("NONE")


class ConsistentRead(SimpleValue("ConsistentRead")):
    def true(self):
        """
        Set ConsistentRead to True.
        The request will use strong consistent reads.
        """
        return self.set(True)

    def false(self):
        """
        Set ConsistentRead to False.
        The request will use eventually consistent reads.
        """
        return self.set(False)


def proxy(method):
    bases = {
        "return_values_all_old": ReturnValues.all_old,
        "return_values_all_new": ReturnValues.all_new,
        "return_values_updated_old": ReturnValues.updated_old,
        "return_values_updated_new": ReturnValues.updated_new,
        "return_values_none": ReturnValues.none,

        "return_consumed_capacity_total": ReturnConsumedCapacity.total,
        "return_consumed_capacity_indexes": ReturnConsumedCapacity.indexes,
        "return_consumed_capacity_none": ReturnConsumedCapacity.none,

        "return_item_collection_metrics_size": ReturnItemCollectionMetrics.size,
        "return_item_collection_metrics_none": ReturnItemCollectionMetrics.none,

        "consistent_read_true": ConsistentRead.true,
        "consistent_read_false": ConsistentRead.false,
    }

    method.__doc__ = bases[method.__name__].__doc__ + "\n" + method.__doc__
    return method
