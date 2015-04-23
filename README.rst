LowVoltage is a standalone Python (2.7+ and 3.4+) client for `DynamoDB <http://aws.amazon.com/documentation/dynamodb/>`__
that doesn't hide any feature of `the API <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/Welcome.html>`__.

It's licensed under the `MIT license <http://choosealicense.com/licenses/mit/>`__.
It depends only on the excellent `python-requests <http://python-requests.org>`__ library.
It's available on the `Python package index <http://pypi.python.org/pypi/LowVoltage>`__, its `documentation is hosted by Python <http://pythonhosted.org/LowVoltage>`__ and its source code is on `GitHub <https://github.com/jacquev6/LowVoltage>`__.

.. image:: https://img.shields.io/travis/jacquev6/LowVoltage/master.svg
    :target: https://travis-ci.org/jacquev6/LowVoltage

.. image:: https://img.shields.io/coveralls/jacquev6/LowVoltage/master.svg
    :target: https://coveralls.io/r/jacquev6/LowVoltage

.. image:: https://img.shields.io/codeclimate/github/jacquev6/LowVoltage.svg
    :target: https://codeclimate.com/github/jacquev6/LowVoltage

.. image:: https://img.shields.io/pypi/dm/LowVoltage.svg
    :target: https://pypi.python.org/pypi/LowVoltage

.. image:: https://img.shields.io/pypi/l/LowVoltage.svg
    :target: https://pypi.python.org/pypi/LowVoltage

.. image:: https://img.shields.io/pypi/v/LowVoltage.svg
    :target: https://pypi.python.org/pypi/LowVoltage

.. image:: https://pypip.in/py_versions/LowVoltage/badge.svg
    :target: https://pypi.python.org/pypi/LowVoltage

.. image:: https://pypip.in/status/LowVoltage/badge.svg
    :target: https://pypi.python.org/pypi/LowVoltage

.. image:: https://img.shields.io/github/issues/jacquev6/LowVoltage.svg
    :target: https://github.com/jacquev6/LowVoltage/issues

.. image:: https://img.shields.io/github/forks/jacquev6/LowVoltage.svg
    :target: https://github.com/jacquev6/LowVoltage/network

.. image:: https://img.shields.io/github/stars/jacquev6/LowVoltage.svg
    :target: https://github.com/jacquev6/LowVoltage/stargazers

Quick start
===========

Install from PyPI::

    $ pip install LowVoltage

Import the package and create a connection::

    >>> from LowVoltage import *
    >>> connection = make_connection("eu-west-1", EnvironmentCredentials())

Assuming you have a table named "LowVoltage.DocTests" with a hash key on the number attribute "h", you can put an item and get it back::

    >>> table = "LowVoltage.DocTests"

    >>> connection(PutItem(table, {"h": 0, "a": 42, "b": u"bar"}))
    <LowVoltage.actions.put_item.Result object at ...>

    >>> connection(GetItem(table, {"h": 0})).item
    {u'a': 42, u'h': 0, u'b': u'bar'}

Todo
====

- __str__ and __repr__
- docs
- credential provider for AWS's AIM roles
- create builder for attribute paths
- improve builder for expressions
- metrics
- debug logging
