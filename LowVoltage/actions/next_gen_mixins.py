# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import numbers
import inspect
import functools

import LowVoltage.exceptions as _exn
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

        def call_wrapped(*args):
            args = list(args)
            args[-1] = flatten(args[-1])
            return wrapped(*args)

        prototype = list(spec.args)
        if spec.defaults is not None:
            # Could we do that outside of the generated code?
            # Reusing the original default objects instead of dumping them as string would work in more cases.
            # But he this is good enough for None, which is our only use case yet.
            assert spec.defaults[-1] == []
            for i in range(len(prototype) - len(spec.defaults), len(prototype) - 1):
                prototype[i] += "={}".format(spec.defaults[i - len(prototype)])
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
            raise _exn.BuilderError("Mandatory parameter {} is missing.".format(self._name))
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


class OptionalDictParameter(object):
    def __init__(self, name, parent):
        self._name = name
        self._parent = parent
        self._values = {}

    def add(self, key, value):
        self._values[key] = self._convert(value)
        return self._parent

    @property
    def payload(self):
        data = {}
        if len(self._values) != 0:
            data[self._name] = self._values
        return data


def plain_parameter_mixin(typ):
    class PlainParameterMixin(object):
        def _convert(self, s):
            if isinstance(s, typ):
                return s
            else:
                raise TypeError("Parameter {} must be a {}.".format(self._name, typ.__name__))
    return PlainParameterMixin


class ItemParameterMixin(object):
    def _convert(self, item):
        if isinstance(item, dict):
            return _convert_dict_to_db(item)
        else:
            raise TypeError("Parameter {} must be a dict.".format(self._name))


class ValueParameterMixin(object):
    def _convert(self, value):
        return _convert_value_to_db(value)


class IntParameterMixin(plain_parameter_mixin(numbers.Integral)): pass
class BoolParameterMixin(plain_parameter_mixin(bool)): pass
class StringParameterMixin(plain_parameter_mixin(basestring)): pass


class OptionalItemParameter(OptionalScalarParameter, ItemParameterMixin): pass
class OptionalIntParameter(OptionalScalarParameter, IntParameterMixin): pass
class OptionalBoolParameter(OptionalScalarParameter, BoolParameterMixin): pass
class OptionalStringParameter(OptionalScalarParameter, StringParameterMixin): pass

class OptionalDictOfStringParameter(OptionalDictParameter, StringParameterMixin): pass
class OptionalDictOfValueParameter(OptionalDictParameter, ValueParameterMixin): pass

class MandatoryItemParameter(MandatoryScalarParameter, ItemParameterMixin): pass
class MandatoryIntParameter(MandatoryScalarParameter, IntParameterMixin): pass
class MandatoryBoolParameter(MandatoryScalarParameter, BoolParameterMixin): pass
class MandatoryStringParameter(MandatoryScalarParameter, StringParameterMixin): pass


class TableName(MandatoryStringParameter):
    def __init__(self, parent, value):
        super(TableName, self).__init__("TableName", parent, value)

    def set(self, table_name):
        """
        Set TableName. Mandatory, can also be set in the constructor.
        """
        return super(TableName, self).set(table_name)


class Key(MandatoryItemParameter):
    def __init__(self, parent, value):
        super(Key, self).__init__("Key", parent, value)

    def set(self, key):
        """
        Set Key. Mandatory, can also be set in the constructor.
        """
        return super(Key, self).set(key)


class Item(MandatoryItemParameter):
    def __init__(self, parent, value):
        super(Item, self).__init__("Item", parent, value)

    def set(self, item):
        """
        Set Item. Mandatory, can also be set in the constructor.
        """
        return super(Item, self).set(item)


class IndexName(OptionalStringParameter):
    def __init__(self, parent):
        super(IndexName, self).__init__("IndexName", parent)

    def set(self, index_name):
        """
        Set IndexName. The request will use this index instead of the table key.
        """
        return super(IndexName, self).set(index_name)


class ConditionExpression(OptionalStringParameter):
    def __init__(self, parent):
        super(ConditionExpression, self).__init__("ConditionExpression", parent)

    def set(self, expression):
        """
        Set the ConditionExpression, making the request conditional.
        It will raise a :exc:`.ConditionalCheckFailedException` if the condition is not met.
        """
        return super(ConditionExpression, self).set(expression)


class ConsistentRead(OptionalBoolParameter):
    def __init__(self, parent):
        super(ConsistentRead, self).__init__("ConsistentRead", parent)

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


class ExclusiveStartKey(OptionalItemParameter):
    def __init__(self, parent):
        super(ExclusiveStartKey, self).__init__("ExclusiveStartKey", parent)

    def set(self, key):
        """
        Set ExclusiveStartKey. The request will only scan items that are after this key.
        This is typically the :attr:`~{}Response.last_evaluated_key` of a previous response.
        """
        return super(ExclusiveStartKey, self).set(key)


class ExpressionAttributeNames(OptionalDictOfStringParameter):
    def __init__(self, parent):
        super(ExpressionAttributeNames, self).__init__("ExpressionAttributeNames", parent)

    def add(self, synonym, name):
        """
        Add a synonym for an attribute name to ExpressionAttributeNames.
        Useful for attributes whose names don't play well with ProjectionExpression, ConditionExpression or UpdateExpression because they contain a dot or brackets.
        """
        return super(ExpressionAttributeNames, self).add("#" + synonym, name)


class ExpressionAttributeValues(OptionalDictOfValueParameter):
    def __init__(self, parent):
        super(ExpressionAttributeValues, self).__init__("ExpressionAttributeValues", parent)

    def add(self, name, value):
        """
        Add a named value to ExpressionAttributeValues.
        """
        return super(ExpressionAttributeValues, self).add(":" + name, value)


class FilterExpression(OptionalStringParameter):
    def __init__(self, parent):
        super(FilterExpression, self).__init__("FilterExpression", parent)

    def set(self, expression):
        """
        Set the FilterExpression. The response will contain only items that match.
        """
        return super(FilterExpression, self).set(expression)


class Limit(OptionalIntParameter):
    def __init__(self, parent):
        super(Limit, self).__init__("Limit", parent)

    def set(self, limit):
        """
        Set Limit. The request will scan at most this number of items.
        """
        return super(Limit, self).set(limit)


class ProjectionExpression(object):
    def __init__(self, parent):
        self.__names = []
        self.__parent = parent

    @property
    def payload(self):
        data = {}
        if len(self.__names) != 0:
            data["ProjectionExpression"] = ", ".join(self.__names)
        return data

    @variadic(basestring)
    def add(self, names):
        """
        Add name(s) to ProjectionExpression.
        The request will return only projected attributes.
        """
        self.__names.extend(names)
        return self.__parent


class ReturnConsumedCapacity(OptionalStringParameter):
    def __init__(self, parent):
        super(ReturnConsumedCapacity, self).__init__("ReturnConsumedCapacity", parent)

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


class ReturnItemCollectionMetrics(OptionalStringParameter):
    def __init__(self, parent):
        super(ReturnItemCollectionMetrics, self).__init__("ReturnItemCollectionMetrics", parent)

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


class ReturnValues(OptionalStringParameter):
    def __init__(self, parent):
        super(ReturnValues, self).__init__("ReturnValues", parent)

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


class Select(OptionalStringParameter):
    def __init__(self, parent):
        super(Select, self).__init__("Select", parent)

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
        "index_name": IndexName.set,
        "item": Item.set,
        "key": Key.set,
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
        "table_name": TableName.set,
    }

    def patch(method, format_args):
        base = bases[method.__name__]
        assert inspect.getargspec(method) == inspect.getargspec(base), method
        method.__doc__ = base.__doc__.format(*format_args) + "\n" + method.__doc__

    if len(proxy_args) != 1 or isinstance(proxy_args[0], basestring):
        def decorator(method):
            patch(method, proxy_args)
            return method
        return decorator
    else:
        method = proxy_args[0]
        patch(method, ())
        return method
