==========
LowVoltage
==========

LowVoltage is a Python (2 and 3) client for `DynamoDB <http://aws.amazon.com/documentation/dynamodb/>`__
that doesn't hide any feature of `the API <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/Welcome.html>`__.

It's licensed under the `MIT license <http://choosealicense.com/licenses/mit/>`__.

It depends only on the excellent `python-requests <http://python-requests.org>`__ library.
It's available on the `Python package index <http://pypi.python.org/pypi/LowVoltage>`__ and the source code is on `GitHub <https://github.com/jacquev6/LowVoltage>`__.

Quick start
===========

Install from PyPI::

    $ pip install LowVoltage

Import the package and create a connection:

.. doctest::

    >>> from LowVoltage import *
    >>> connection = make_connection("eu-west-1", EnvironmentCredentials())

Assuming you have a table named "LowVoltage.DocTests" with a hash key on the number attribute "h", you can put an item and get it back:

.. doctest::

    >>> table = "LowVoltage.DocTests"

    >>> connection(PutItem(table, {"h": 0, "a": 42, "b": u"bar"}))
    <LowVoltage.actions.put_item.Result object at ...>

    >>> connection(GetItem(table, {"h": 0})).item
    {u'a': 42, u'h': 0, u'b': u'bar'}

Contents
========

.. toctree::
    :maxdepth: 20

    user_guide
    reference

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
