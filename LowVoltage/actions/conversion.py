# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import base64
import numbers
import sys
import unittest


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


class ConversionUnitTests(unittest.TestCase):
    def testConvertValueToDb(self):
        self.assertEqual(_convert_value_to_db(u"éoà"), {"S": u"éoà"})
        self.assertEqual(_convert_value_to_db(b"\xFF\x00\xAB"), {"B": u"/wCr"})
        self.assertEqual(_convert_value_to_db(True), {"BOOL": True})
        self.assertEqual(_convert_value_to_db(False), {"BOOL": False})
        self.assertEqual(_convert_value_to_db(42), {"N": "42"})
        self.assertEqual(_convert_value_to_db(None), {"NULL": True})
        self.assertIn(_convert_value_to_db(set([42, 43])), [{"NS": ["42", "43"]}, {"NS": ["43", "42"]}])
        self.assertIn(_convert_value_to_db(frozenset([42, 43])), [{"NS": ["42", "43"]}, {"NS": ["43", "42"]}])
        self.assertIn(_convert_value_to_db(set([u"éoà", u"bar"])), [{"SS": [u"éoà", u"bar"]}, {"SS": [u"bar", u"éoà"]}])
        self.assertIn(_convert_value_to_db(set([b"\xFF\x00\xAB", b"bar"])), [{"BS": [u"/wCr", u"YmFy"]}, {"BS": [u"YmFy", u"/wCr"]}])
        self.assertEqual(_convert_value_to_db([True, 42]), {"L": [{"BOOL": True}, {"N": "42"}]})
        self.assertEqual(_convert_value_to_db({"a": True, "b": 42}), {"M": {"a": {"BOOL": True}, "b": {"N": "42"}}})
        with self.assertRaises(TypeError):
            _convert_value_to_db(set())
        with self.assertRaises(TypeError):
            _convert_value_to_db(set([(1, 2)]))
        with self.assertRaises(TypeError):
            _convert_value_to_db(set([1, "2"]))
        with self.assertRaises(TypeError):
            _convert_value_to_db((1, 2))

    def testConvertDbToValue(self):
        self.assertEqual(_convert_db_to_value({"S": u"éoà"}), u"éoà")
        self.assertEqual(_convert_db_to_value({"B": u"/wCr"}), b"\xFF\x00\xAB")
        self.assertEqual(_convert_db_to_value({"BOOL": True}), True)
        self.assertEqual(_convert_db_to_value({"BOOL": False}), False)
        self.assertEqual(_convert_db_to_value({"N": "42"}), 42)
        self.assertEqual(_convert_db_to_value({"NULL": True}), None)
        self.assertEqual(_convert_db_to_value({"NS": ["42", "43"]}), set([42, 43]))
        self.assertEqual(_convert_db_to_value({"SS": [u"éoà", u"bar"]}), set([u"éoà", u"bar"]))
        self.assertEqual(_convert_db_to_value({"BS": [u"/wCr", u"YmFy"]}), set([b"\xFF\x00\xAB", b"bar"]))
        self.assertEqual(_convert_db_to_value({"L": [{"BOOL": True}, {"N": "42"}]}), [True, 42])
        self.assertEqual(_convert_db_to_value({"M": {"a": {"BOOL": True}, "b": {"N": "42"}}}), {"a": True, "b": 42})
        with self.assertRaises(TypeError):
            _convert_db_to_value({})
        with self.assertRaises(TypeError):
            _convert_db_to_value([])
        with self.assertRaises(TypeError):
            _convert_db_to_value("SSS")

    def testConvertDictToDb(self):
        self.assertEqual(_convert_dict_to_db({}), {})
        self.assertEqual(_convert_dict_to_db({"a": 42}), {"a": {"N": "42"}})

    def testConvertDbToDict(self):
        self.assertEqual(_convert_db_to_dict({}), {})
        self.assertEqual(_convert_db_to_dict({"a": {"N": "42"}}), {"a": 42})
