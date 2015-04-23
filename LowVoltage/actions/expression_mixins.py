# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage.testing as _tst
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
        """
        Add a named value to ExpressionAttributeValues.
        """
        self.__expression_attribute_values[name] = value
        return self


class ExpressionAttributeValuesMixinUnitTests(_tst.UnitTests):
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

    def expression_attribute_name(self, name, path):
        """
        Add a name for a path to ExpressionAttributeNames.
        Useful for attributes whose names don't play well with ProjectionExpression, ConditionExpression or UpdateExpression.

            >>> connection(
            ...   PutItem(table, {"h": 0, "a.b.c": {"d[e]f.g": 42, "c": 0}})
            ... )
            <LowVoltage.actions.put_item.Result object at ...>

            >>> connection(
            ...   GetItem(table, {"h": 0})
            ...     .expression_attribute_name("a", "a.b.c")
            ...     .expression_attribute_name("b", "d[e]f.g")
            ...     .project("#a.#b")
            ... ).item
            {u'a.b.c': {u'd[e]f.g': 42}}
        """
        self.__expression_attribute_names[name] = path
        return self


class ExpressionAttributeNamesMixinUnitTests(_tst.UnitTests):
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
        """
        Add a ConditionExpression, making the request conditional.
        It will raise a :class:`ConditionalCheckFailedException` if the condition is not met.

            >>> connection(PutItem(table, {"h": 0, "a": 42}))
            <LowVoltage.actions.put_item.Result object at ...>

        The condition is met so this :class:`PutItem` succeeds:

            >>> connection(
            ...   PutItem(table, {"h": 0, "a": 43})
            ...     .condition_expression("a=:a")
            ...     .expression_attribute_value("a", 42)
            ... )
            <LowVoltage.actions.put_item.Result object at ...>

        The condition is not met anymore, so this :class:`PutItem` fails:

            >>> connection(
            ...   PutItem(table, {"h": 0, "a": 44})
            ...     .condition_expression("a=:a")
            ...     .expression_attribute_value("a", 42)
            ... )
            Traceback (most recent call last):
              ...
            ConditionalCheckFailedException
        """
        self.__condition_expression = expression
        return self


class ConditionExpressionMixinUnitTests(_tst.UnitTests):
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
        """
        Add a FilterExpression.
        """
        self.__filter_expression = expression
        return self


class FilterExpressionMixinUnitTests(_tst.UnitTests):
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
        """
        Add name(s) to ProjectionExpression.

            >>> connection(
            ...   PutItem(table, {"h": 0, "a": {"b": 42, "c": 0}, "d": True})
            ... )
            <LowVoltage.actions.put_item.Result object at ...>

            >>> connection(
            ...   GetItem(table, {"h": 0})
            ...     .project("a.b", "h")
            ...     .project("d")
            ... ).item
            {u'a': {u'b': 42}, u'h': 0, u'd': True}
        """
        for name in names:
            if isinstance(name, basestring):
                name = [name]
            self.__projections.extend(name)
        return self


class ProjectionExpressionMixinUnitTests(_tst.UnitTests):
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
