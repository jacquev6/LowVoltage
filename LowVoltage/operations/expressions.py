# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest


class _Boolean:
    def __and__(self, other):
        return _BooleanExpression(self, "AND", other)

    def __or__(self, other):
        return _BooleanExpression(self, "OR", other)

    def __invert__(self):
        return _BooleanNegation(self)


class _BooleanExpression(_Boolean):
    def __init__(self, left, operator, right):
        if not isinstance(left, _Boolean):
            raise TypeError  # pragma no cover (Defensive code)
        if not isinstance(right, _Boolean):
            raise TypeError
        self.__left = left
        self.__operator = operator
        self.__right = right

    def bool(self):
        return "({}) {} ({})".format(self.__left.bool(), self.__operator, self.__right.bool())


class _BooleanNegation(_Boolean):
    def __init__(self, operand):
        if not isinstance(operand, _Boolean):
            raise TypeError  # pragma no cover (Defensive code)
        self.__operand = operand

    def bool(self):
        return "NOT ({})".format(self.__operand.bool())


class _ComparisonExpression(_Boolean):
    def __init__(self, left, operator, right):
        if not isinstance(left, _Atom):
            raise TypeError  # pragma no cover (Defensive code)
        if not isinstance(right, _Atom):
            raise TypeError
        self.__left = left
        self.__operator = operator
        self.__right = right

    def bool(self):
        return "{}{}{}".format(self.__left.atom(), self.__operator, self.__right.atom())


class _Atom:
    def __eq__(self, other):
        return _ComparisonExpression(self, "=", other)

    def __ne__(self, other):
        return _ComparisonExpression(self, "<>", other)

    def __lt__(self, other):
        return _ComparisonExpression(self, "<", other)

    def __le__(self, other):
        return _ComparisonExpression(self, "<=", other)

    def __gt__(self, other):
        return _ComparisonExpression(self, ">", other)

    def __ge__(self, other):
        return _ComparisonExpression(self, ">=", other)


class Attr(_Atom):
    def __init__(self, name):
        self.__name = name

    def atom(self):
        return self.__name


class Val(_Atom):
    def __init__(self, label):
        self.__label = label

    def atom(self):
        return ":{}".format(self.__label)


class ConditionExpressionsTestCase(unittest.TestCase):
    def testAtomsComparison(self):
        self.assertEqual((Attr("a") == Attr("b")).bool(), "a=b")
        self.assertEqual((Attr("a") == Val("b")).bool(), "a=:b")
        self.assertEqual((Val("a") == Attr("b")).bool(), ":a=b")
        self.assertEqual((Val("a") == Val("b")).bool(), ":a=:b")

    def testComparisons(self):
        self.assertEqual((Attr("a") == Attr("b")).bool(), "a=b")
        self.assertEqual((Attr("a") != Attr("b")).bool(), "a<>b")
        self.assertEqual((Attr("a") < Attr("b")).bool(), "a<b")
        self.assertEqual((Attr("a") <= Attr("b")).bool(), "a<=b")
        self.assertEqual((Attr("a") > Attr("b")).bool(), "a>b")
        self.assertEqual((Attr("a") >= Attr("b")).bool(), "a>=b")

    def testBooleanAlgebra(self):
        # Too many parentheses are needed because of lower priority of bitwise operators ("&", "|" and "~"),
        # but we cannot override keywords ("and", "or" and "not") that would have higher priority.
        self.assertEqual(((Attr("a") == Attr("b")) & (Attr("c") == Attr("d"))).bool(), "(a=b) AND (c=d)")
        self.assertEqual(((Attr("a") == Attr("b")) | (Attr("c") == Attr("d"))).bool(), "(a=b) OR (c=d)")
        self.assertEqual((~((Attr("a") == Attr("b")) | (Attr("c") == Attr("d")))).bool(), "NOT ((a=b) OR (c=d))")

    def testMissingParenthesesInBooleanAlgebra(self):
        # But at least, missing parentheses are caught early
        with self.assertRaises(TypeError):
            Attr("a") == Attr("b") & Attr("c") == Attr("d")
        with self.assertRaises(TypeError):
            (Attr("a") == Attr("b")) & Attr("c") == Attr("d")
        with self.assertRaises(TypeError):
            Attr("a") == Attr("b") & (Attr("c") == Attr("d"))

    def testPlainWrongTypes(self):
        with self.assertRaises(TypeError):
            Attr("a") & (Attr("c") == Attr("d"))
        with self.assertRaises(TypeError):
            (Attr("a") == Attr("c")) & Attr("d")
        with self.assertRaises(TypeError):
            (Attr("a") == Attr("c")) == Attr("d")


if __name__ == "__main__":  # pragma no branch (Test code)
    unittest.main()  # pragma no cover (Test code)
