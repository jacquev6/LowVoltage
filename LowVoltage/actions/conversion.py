# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
Attributes in DynamoDB can have `some types <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html>`__.
LowVoltage translates some Python types to be stored in DynamoDB:

- number ``10`` is stored as ``"N"`` (number)

>>> connection(PutItem(table, {"h": 0, "number": 10}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'number': 10}

- unicode ``u"foobar"`` is stored as ``"S"`` (string, properly utf8-encoded)

>>> connection(PutItem(table, {"h": 0, "unicode": u"foobar"}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'unicode': u'foobar'}

- bytes ``b"barbaz"`` is stored as ``"B"`` (binary, properly base64-encoded)

>>> connection(PutItem(table, {"h": 0, "bytes": b"barbaz"}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'bytes': 'barbaz'}

Note that strings are bytes in Python 2 and unicode in Python 3. You may want to avoid them.

- booleans ``True`` and ``False`` are stored as ``"BOOL"``

>>> connection(PutItem(table, {"h": 0, "boolean": True}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'boolean': True}

- ``None`` is stored as ``"NULL"``

>>> connection(PutItem(table, {"h": 0, "none": None}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'none': None}

- an homogeneous set of numbers ``{24, 57}`` is stored as ``"NS"`` (number set)

>>> connection(PutItem(table, {"h": 0, "set_of_number": {24, 57}}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'set_of_number': set([24, 57])}

- an homogeneous set of unicodes ``{u"foo", u"bar"}`` is stored as ``"SS"`` (string set)

>>> connection(PutItem(table, {"h": 0, "set_of_unicode": {u"foo", u"bar"}}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'set_of_unicode': set([u'foo', u'bar'])}

- an homogeneous set of bytes ``{b"bar", b"baz"}`` is stored as ``"BS"`` (binary set)

>>> connection(PutItem(table, {"h": 0, "set_of_bytes": {b"bar", b"baz"}}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'set_of_bytes': set(['baz', 'bar']), u'h': 0}

- a list ``["a", {"b": "c"}, 42]`` is stored as ``"L"`` (list)

>>> connection(PutItem(table, {"h": 0, "list": ["a", {"b": "c"}, 42]}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'list': ['a', {u'b': 'c'}, 42]}

Note that lists are stored as ``"L"`` even if they are homogeneous, so you have to use sets if you plan to use :meth:`.UpdateItem.add` and :meth:`.UpdateItem.delete`.

- a dict ``{"a": "b", "c": 42}`` is stored as ``"M"`` (map)

>>> connection(PutItem(table, {"h": 0, "dict": {"a": "b", "c": 42}}))
<LowVoltage.actions.put_item.PutItemResponse ...>
>>> connection(GetItem(table, {"h": 0})).item
{u'h': 0, u'dict': {u'a': 'b', u'c': 42}}

- everything else will raise a :exc:`TypeError`:

>>> connection(PutItem(table, {"h": 0, "something_else": {"a", 42}}))
Traceback (most recent call last):
  ...
TypeError: ...
"""

import base64
import numbers
import sys

import LowVoltage.testing as _tst


def _convert_dict_to_db(attributes):
    return {
        key: _convert_value_to_db(val)
        for key, val in attributes.iteritems()
    }


def _convert_value_to_db(value):
    if isinstance(value, unicode):
        return {"S": value}
    elif isinstance(value, bytes):
        return {"B": base64.b64encode(value).decode("utf8")}
    elif isinstance(value, bool):
        return {"BOOL": value}
    elif isinstance(value, numbers.Integral):
        return {"N": str(value)}
    elif value is None:
        return {"NULL": True}
    elif isinstance(value, (set, frozenset)):
        if len(value) == 0:
            raise TypeError
        elif all(isinstance(v, numbers.Integral) for v in value):
            return {"NS": [str(n) for n in value]}
        elif all(isinstance(v, unicode) for v in value):
            return {"SS": [s for s in value]}
        elif all(isinstance(v, bytes) for v in value):
            return {"BS": [base64.b64encode(b).decode("utf8") for b in value]}
        else:
            raise TypeError
    elif isinstance(value, list):
        return {"L": [_convert_value_to_db(v) for v in value]}
    elif isinstance(value, dict):
        return {"M": {n: _convert_value_to_db(v) for n, v in value.iteritems()}}
    else:
        raise TypeError


def _convert_db_to_dict(attributes):
    return {
        key: _convert_db_to_value(val)
        for key, val in attributes.iteritems()
    }


def _convert_db_to_value(value):
    if "S" in value:
        return value["S"]
    elif "B" in value:
        return bytes(base64.b64decode(value["B"].encode("utf8")))
    elif "BOOL" in value:
        return value["BOOL"]
    elif "N" in value:
        return int(value["N"])
    elif "NULL" in value:
        return None
    elif "NS" in value:
        return set(int(v) for v in value["NS"])
    elif "SS" in value:
        return set(value["SS"])
    elif "BS" in value:
        return set(bytes(base64.b64decode(v.encode("utf8"))) for v in value["BS"])
    elif "L" in value:
        return [_convert_db_to_value(v) for v in value["L"]]
    elif "M" in value:
        return {n: _convert_db_to_value(v) for n, v in value["M"].iteritems()}
    else:
        raise TypeError


class ConversionUnitTests(_tst.UnitTests):
    def test_convert_unicode_value_to_db(self):
        self.assertEqual(_convert_value_to_db(u"éoà"), {"S": u"éoà"})

    def test_convert_bytes_value_to_db(self):
        self.assertEqual(_convert_value_to_db(b"\xFF\x00\xAB"), {"B": u"/wCr"})

    def test_convert_bool_value_to_db(self):
        self.assertEqual(_convert_value_to_db(True), {"BOOL": True})
        self.assertEqual(_convert_value_to_db(False), {"BOOL": False})

    def test_convert_int_value_to_db(self):
        self.assertEqual(_convert_value_to_db(42), {"N": "42"})

    def test_convert_none_value_to_db(self):
        self.assertEqual(_convert_value_to_db(None), {"NULL": True})

    def test_convert_set_of_int_value_to_db(self):
        self.assertIn(_convert_value_to_db(set([42, 43])), [{"NS": ["42", "43"]}, {"NS": ["43", "42"]}])

    def test_convert_frozenset_value_to_db(self):
        self.assertIn(_convert_value_to_db(frozenset([42, 43])), [{"NS": ["42", "43"]}, {"NS": ["43", "42"]}])

    def test_convert_set_of_unicode_value_to_db(self):
        self.assertIn(_convert_value_to_db(set([u"éoà", u"bar"])), [{"SS": [u"éoà", u"bar"]}, {"SS": [u"bar", u"éoà"]}])

    def test_convert_set_of_byte_value_to_db(self):
        self.assertIn(_convert_value_to_db(set([b"\xFF\x00\xAB", b"bar"])), [{"BS": [u"/wCr", u"YmFy"]}, {"BS": [u"YmFy", u"/wCr"]}])

    def test_convert_list_value_to_db(self):
        self.assertEqual(_convert_value_to_db([True, 42]), {"L": [{"BOOL": True}, {"N": "42"}]})

    def test_convert_dict_value_to_db(self):
        self.assertEqual(_convert_value_to_db({"a": True, "b": 42}), {"M": {"a": {"BOOL": True}, "b": {"N": "42"}}})

    def test_convert_empty_set_value_to_db(self):
        with self.assertRaises(TypeError):
            _convert_value_to_db(set())

    def test_convert_set_of_tuple_value_to_db(self):
        with self.assertRaises(TypeError):
            _convert_value_to_db(set([(1, 2)]))

    def test_convert_heterogenous_set_value_to_db(self):
        with self.assertRaises(TypeError):
            _convert_value_to_db(set([1, "2"]))

    def test_convert_tuple_value_to_db(self):
        with self.assertRaises(TypeError):
            _convert_value_to_db((1, 2))

    def test_convert_db_to_unicode_value(self):
        self.assertEqual(_convert_db_to_value({"S": u"éoà"}), u"éoà")

    def test_convert_db_to_bytes_value(self):
        self.assertEqual(_convert_db_to_value({"B": u"/wCr"}), b"\xFF\x00\xAB")

    def test_convert_db_to_bool_value(self):
        self.assertEqual(_convert_db_to_value({"BOOL": True}), True)
        self.assertEqual(_convert_db_to_value({"BOOL": False}), False)

    def test_convert_db_to_int_value(self):
        self.assertEqual(_convert_db_to_value({"N": "42"}), 42)

    def test_convert_db_to_none_value(self):
        self.assertEqual(_convert_db_to_value({"NULL": True}), None)

    def test_convert_db_to_set_of_int_value(self):
        self.assertEqual(_convert_db_to_value({"NS": ["42", "43"]}), set([42, 43]))

    def test_convert_db_to_set_of_unicode_value(self):
        self.assertEqual(_convert_db_to_value({"SS": [u"éoà", u"bar"]}), set([u"éoà", u"bar"]))

    def test_convert_db_to_set_of_byte_value(self):
        self.assertEqual(_convert_db_to_value({"BS": [u"/wCr", u"YmFy"]}), set([b"\xFF\x00\xAB", b"bar"]))

    def test_convert_db_to_list_value(self):
        self.assertEqual(_convert_db_to_value({"L": [{"BOOL": True}, {"N": "42"}]}), [True, 42])

    def test_convert_db_to_dict_value(self):
        self.assertEqual(_convert_db_to_value({"M": {"a": {"BOOL": True}, "b": {"N": "42"}}}), {"a": True, "b": 42})

    def test_convert_db_to_empty_dict_value(self):
        with self.assertRaises(TypeError):
            _convert_db_to_value({})

    def test_convert_db_to_empty_list_value(self):
        with self.assertRaises(TypeError):
            _convert_db_to_value([])

    def test_convert_db_to_string_value(self):
        with self.assertRaises(TypeError):
            _convert_db_to_value("SSS")

    def test_convert_empty_dict_to_db(self):
        self.assertEqual(_convert_dict_to_db({}), {})

    def test_convert_dict_to_db(self):
        self.assertEqual(_convert_dict_to_db({"a": 42}), {"a": {"N": "42"}})

    def test_convert_db_to_empty_dict(self):
        self.assertEqual(_convert_db_to_dict({}), {})

    def test_convert_db_to_dict(self):
        self.assertEqual(_convert_db_to_dict({"a": {"N": "42"}}), {"a": 42})
