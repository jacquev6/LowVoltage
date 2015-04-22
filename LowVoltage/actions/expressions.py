# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage.testing as _tst


class _Boolean(object):
    def __and__(self, other):
        return _BooleanExpression(self, "AND", other)

    def __or__(self, other):
        return _BooleanExpression(self, "OR", other)

    def __invert__(self):
        return _BooleanNegation(self)


class _BooleanExpression(_Boolean):
    def __init__(self, left, operator, right):
        if not isinstance(left, _Boolean):
            raise TypeError
        if not isinstance(operator, basestring):
            raise TypeError
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
            raise TypeError
        self.__operand = operand

    def bool(self):
        return "NOT ({})".format(self.__operand.bool())


class _ComparisonExpression(_Boolean):
    def __init__(self, left, operator, right):
        if not isinstance(left, _Atom):
            raise TypeError
        if not isinstance(operator, basestring):
            raise TypeError
        if not isinstance(right, _Atom):
            raise TypeError
        self.__left = left
        self.__operator = operator
        self.__right = right

    def bool(self):
        return "{}{}{}".format(self.__left.atom(), self.__operator, self.__right.atom())


class _Atom(object):
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
        if not isinstance(name, basestring):
            raise TypeError
        self.__name = name

    def atom(self):
        return self.__name


class Val(_Atom):
    def __init__(self, label):
        if not isinstance(label, basestring):
            raise TypeError
        self.__label = label

    def atom(self):
        return ":{}".format(self.__label)


class In(_Boolean):
    def __init__(self, elem, set):
        if not isinstance(elem, _Atom):
            raise TypeError
        if not all(isinstance(elem, _Atom) for elem in set):
            raise TypeError
        self.__elem = elem
        self.__set = set

    def bool(self):
        return "{} IN ({})".format(self.__elem.atom(), ", ".join(elem.atom() for elem in self.__set))


class Between(_Boolean):
    def __init__(self, elem, low, high):
        if not isinstance(elem, _Atom):
            raise TypeError
        if not isinstance(low, _Atom):
            raise TypeError
        if not isinstance(high, _Atom):
            raise TypeError
        self.__elem = elem
        self.__low = low
        self.__high = high

    def bool(self):
        return "{} BETWEEN {} AND {}".format(self.__elem.atom(), self.__low.atom(), self.__high.atom())


class AttributeExists(_Boolean):
    def __init__(self, name):
        if not isinstance(name, basestring):
            raise TypeError
        self.__name = name

    def bool(self):
        return "attribute_exists({})".format(self.__name)


class Contains(_Boolean):
    def __init__(self, left, right):
        if not isinstance(left, _Atom):
            raise TypeError
        if not isinstance(right, _Atom):
            raise TypeError
        self.__left = left
        self.__right = right

    def bool(self):
        return "contains({}, {})".format(self.__left.atom(), self.__right.atom())


class BeginsWith(_Boolean):
    def __init__(self, left, right):
        if not isinstance(left, _Atom):
            raise TypeError
        if not isinstance(right, _Atom):
            raise TypeError
        self.__left = left
        self.__right = right

    def bool(self):
        return "begins_with({}, {})".format(self.__left.atom(), self.__right.atom())


class ConditionExpressionUnitTests(_tst.UnitTests):
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

    def testFunctions(self):
        # Erf, we cannot redefine list.__contains__ so we cannot obtain the syntax:
        # Attr("a") in [Val("b"), Val("c")]
        # And anyway the result of __contains__ is converted to a boolean by "in".
        # So we cannot implement Attr("a") in Set(Val("b"), Val("c")) either.
        self.assertEqual(In(Attr("a"), [Val("b"), Val("c")]).bool(), "a IN (:b, :c)")
        self.assertEqual(Between(Attr("a"), Val("b"), Val("c")).bool(), "a BETWEEN :b AND :c")
        self.assertEqual(AttributeExists("a").bool(), "attribute_exists(a)")
        self.assertEqual(Contains(Val("a"), Attr("b")).bool(), "contains(:a, b)")
        self.assertEqual(BeginsWith(Val("a"), Attr("b")).bool(), "begins_with(:a, b)")

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
        with self.assertRaises(TypeError):
            In("a", [Attr("a")])
        with self.assertRaises(TypeError):
            In(Attr("a"), ["a"])
        with self.assertRaises(TypeError):
            _BooleanExpression(Attr("a"), "==", Attr("a") == Attr("b"))
        with self.assertRaises(TypeError):
            _BooleanExpression(Attr("a") == Attr("b"), 42, Attr("a") == Attr("b"))
        with self.assertRaises(TypeError):
            _BooleanNegation(Attr("a"))
        with self.assertRaises(TypeError):
            _ComparisonExpression("a", "==", Attr("b"))
        with self.assertRaises(TypeError):
            _ComparisonExpression(Attr("a"), 42, Attr("b"))
        with self.assertRaises(TypeError):
            _ComparisonExpression(Attr("a"), "==", "b")
        with self.assertRaises(TypeError):
            Attr(42)
        with self.assertRaises(TypeError):
            Val(42)
        with self.assertRaises(TypeError):
            Between("a", Attr("b"), Attr("c"))
        with self.assertRaises(TypeError):
            Between(Attr("a"), "b", Attr("c"))
        with self.assertRaises(TypeError):
            Between(Attr("a"), Attr("b"), "c")
        with self.assertRaises(TypeError):
            AttributeExists(Attr("a"))
        with self.assertRaises(TypeError):
            Contains("a", Attr("b"))
        with self.assertRaises(TypeError):
            Contains(Attr("a"), "b")
        with self.assertRaises(TypeError):
            BeginsWith("a", Attr("b"))
        with self.assertRaises(TypeError):
            BeginsWith(Attr("a"), "b")
