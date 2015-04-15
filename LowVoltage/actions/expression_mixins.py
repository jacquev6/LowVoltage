# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.actions.conversion import _convert_value_to_db


class ExpressionAttributeValuesMixin(object):
    def __init__(self):
        self.__expression_attribute_values = {}

    def _build_expression_attribute_values(self):
        data = {}
        if self.__expression_attribute_values:
            data["ExpressionAttributeValues"] = {
                ":" + n: _convert_value_to_db(v)
                for n, v in self.__expression_attribute_values.iteritems()
            }
        return data

    def expression_attribute_value(self, name, value):
        self.__expression_attribute_values[name] = value
        return self


class ExpressionAttributeValuesMixinUnitTests(unittest.TestCase):
    def testDefault(self):
        self.assertEqual(
            ExpressionAttributeValuesMixin()._build_expression_attribute_values(),
            {}
        )

    def testValues(self):
        self.assertEqual(
            ExpressionAttributeValuesMixin()
                .expression_attribute_value("n1", u"v1")
                .expression_attribute_value("n2", 42)
                ._build_expression_attribute_values(),
            {
                "ExpressionAttributeValues": {
                    ":n1": {"S": "v1"},
                    ":n2": {"N": "42"},
                },
            }
        )


class ExpressionAttributeNamesMixin(object):
    def __init__(self):
        self.__expression_attribute_names = {}

    def _build_expression_attribute_names(self):
        data = {}
        if self.__expression_attribute_names:
            data["ExpressionAttributeNames"] = {
                "#" + n: v
                for n, v in self.__expression_attribute_names.iteritems()
            }
        return data

    def expression_attribute_name(self, name, value):
        self.__expression_attribute_names[name] = value
        return self


class ExpressionAttributeNamesMixinUnitTests(unittest.TestCase):
    def testDefault(self):
        self.assertEqual(
            ExpressionAttributeNamesMixin()._build_expression_attribute_names(),
            {}
        )

    def testNames(self):
        self.assertEqual(
            ExpressionAttributeNamesMixin()
                .expression_attribute_name("n1", "p1")
                .expression_attribute_name("n2", "p2")
                ._build_expression_attribute_names(),
            {
                "ExpressionAttributeNames": {
                    "#n1": "p1",
                    "#n2": "p2",
                },
            }
        )


class ConditionExpressionMixin(object):
    def __init__(self):
        self.__condition_expression = None

    def _build_condition_expression(self):
        data = {}
        if self.__condition_expression:
            data["ConditionExpression"] = self.__condition_expression
        return data

    def condition_expression(self, expression):
        self.__condition_expression = expression
        return self


class ConditionExpressionMixinUnitTests(unittest.TestCase):
    def testDefault(self):
        self.assertEqual(
            ConditionExpressionMixin()._build_condition_expression(),
            {}
        )

    def testExpression(self):
        self.assertEqual(
            ConditionExpressionMixin().condition_expression("a=b")._build_condition_expression(),
            {
                "ConditionExpression": "a=b",
            }
        )


class FilterExpressionMixin(object):
    def __init__(self):
        self.__filter_expression = None

    def _build_filter_expression(self):
        data = {}
        if self.__filter_expression:
            data["FilterExpression"] = self.__filter_expression
        return data

    def filter_expression(self, expression):
        self.__filter_expression = expression
        return self


class FilterExpressionMixinUnitTests(unittest.TestCase):
    def testDefault(self):
        self.assertEqual(
            FilterExpressionMixin()._build_filter_expression(),
            {}
        )

    def testExpression(self):
        self.assertEqual(
            FilterExpressionMixin().filter_expression("a=b")._build_filter_expression(),
            {
                "FilterExpression": "a=b",
            }
        )


class ProjectionExpressionMixin(object):
    def __init__(self):
        self.__projections = []

    def _build_projection_expression(self):
        data = {}
        if self.__projections:
            data["ProjectionExpression"] = ", ".join(self.__projections)
        return data

    def project(self, *names):
        for name in names:
            if isinstance(name, basestring):
                name = [name]
            self.__projections.extend(name)
        return self


class ProjectionExpressionMixinUnitTests(unittest.TestCase):
    def testDefault(self):
        self.assertEqual(
            ProjectionExpressionMixin()._build_projection_expression(),
            {}
        )

    def testExpression(self):
        self.assertEqual(
            ProjectionExpressionMixin().project("a", ["b", "c"])._build_projection_expression(),
            {
                "ProjectionExpression": "a, b, c",
            }
        )
