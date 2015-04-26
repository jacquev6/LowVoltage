# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import inspect
import functools

import LowVoltage as _lv
import LowVoltage.testing as _tst
from .conversion import _convert_value_to_db, _convert_dict_to_db


# @todo Put variadic somewhere else: it's used by compounds as well
def variadic(typ):
    def flatten(args):
        flat_args = []
        for arg in args:
            if isinstance(arg, typ):
                flat_args.append(arg)
            else:
                flat_args.extend(arg)
        return flat_args

    def decorator(wrapped):
        assert wrapped.__doc__ is not None
        spec = inspect.getargspec(wrapped)
        assert len(spec.args) >= 1
        assert spec.varargs is None
        assert spec.keywords is None
        assert spec.defaults is None

        def call_wrapped(*args):
            args = list(args)
            args[-1] = flatten(args[-1])
            return wrapped(*args)

        prototype = list(spec.args)
        prototype[-1] = "*" + prototype[-1]
        prototype = ", ".join(prototype)
        call = ", ".join(spec.args)
        wrapper_code = "def wrapper({}): return call_wrapped({})".format(prototype, call)

        exec_globals = {"call_wrapped": call_wrapped}
        exec wrapper_code in exec_globals
        wrapper = exec_globals["wrapper"]

        functools.update_wrapper(wrapper, wrapped)
        wrapper.__doc__ = "Note that this function is variadic. See :ref:`variadic-functions`.\n\n" + wrapper.__doc__
        return wrapper

    return decorator


class ScalarParameter(object):
    def __init__(self, name, parent, value):
        self._name = name
        self._parent = parent
        self.set(value)

    def set(self, value):
        if value is None:
            self._value = None
        else:
            self._value = self._convert(value)
        return self._parent


class MandatoryScalarParameter(ScalarParameter):
    @property
    def payload(self):
        if self._value is None:
            raise _lv.BuilderError("Mandatory parameter {} is missing.".format(self._name))
        else:
            return {self._name: self._value}


class OptionalScalarParameter(ScalarParameter):
    def __init__(self, name, parent):
        super(OptionalScalarParameter, self).__init__(name, parent, None)

    @property
    def payload(self):
        data = {}
        if self._value is not None:
            data[self._name] = self._value
        return data


class StringParameterMixin(object):
    def _convert(self, s):
        if isinstance(s, basestring):
            return s
        else:
            raise TypeError("Parameter {} must be a string.".format(self._name))


class ItemParameterMixin(object):
    def _convert(self, item):
        if isinstance(item, dict):
            return _convert_dict_to_db(item)
        else:
            raise TypeError("Parameter {} must be a dict.".format(self._name))


class MandatoryStringParameter(MandatoryScalarParameter, StringParameterMixin):
    pass


class MandatoryItemParameter(MandatoryScalarParameter, ItemParameterMixin):
    pass


TableName = functools.partial(MandatoryStringParameter, "TableName")
Key = functools.partial(MandatoryItemParameter, "Key")


def ScalarValue(name, parent=None, value=None):
    class ScalarValue(object):
        def __init__(self, parent, value=None):
            self.__value = value
            self.__parent = parent

        def set(self, value):
            self.__value = value
            return self.__parent

        @property
        def payload(self):
            data = {}
            if self.__value is not None:
                data[name] = self.__value
            return data

    if parent is None:
        return ScalarValue
    else:
        return ScalarValue(parent, value)


def DictValue(name, parent=None, data=None):
    class DictValue(object):
        def __init__(self, parent, data=None):
            self.__data = data or {}
            self.__parent = parent

        def add(self, name, value):
            self.__data[name] = value
            return self.__parent

        @property
        def payload(self):
            data = {}
            if len(self.__data) != 0:
                data[name] = self.__data
            return data

    if parent is None:
        return DictValue
    else:
        return DictValue(parent, data)


def CommaSeparatedStringsValue(name, parent=None, values=None):
    class CommaSeparatedStringsValue(object):
        def __init__(self, parent, values=None):
            self.__values = values or []
            self.__parent = parent

        def add(self, values):
            self.__values += values
            return self.__parent

        @property
        def payload(self):
            data = {}
            if len(self.__values) != 0:
                data[name] = ", ".join(self.__values)
            return data

    if parent is None:
        return CommaSeparatedStringsValue
    else:
        return CommaSeparatedStringsValue(parent, values)


class ConditionExpression(ScalarValue("ConditionExpression")):
    def set(self, expression):
        """
        Set the ConditionExpression, making the request conditional.
        It will raise a :exc:`.ConditionalCheckFailedException` if the condition is not met.
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
        This is typically the :attr:`~{}Response.last_evaluated_key` of a previous response.
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
    @variadic(basestring)
    def add(self, names):
        """
        Add name(s) to ProjectionExpression.
        The request will return only projected attributes.
        """
        return super(ProjectionExpression, self).add(names)


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


def proxy(*proxy_args):
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

    if len(proxy_args) != 1 or isinstance(proxy_args[0], basestring):
        def decorator(method):
            method.__doc__ = bases[method.__name__].__doc__.format(*proxy_args) + "\n" + method.__doc__
            return method
        return decorator
    else:
        method, = proxy_args
        method.__doc__ = bases[method.__name__].__doc__.format() + "\n" + method.__doc__
        return method
