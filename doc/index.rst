LowVoltage's documentation
==========================

Contents:

.. toctree::
   :maxdepth: 2

Quick start
===========

Import the package and create a connection:

.. doctest::

    >>> from LowVoltage import *
    >>> connection = make_connection("eu-west-1", EnvironmentCredentials())

Assuming you have a table named "LowVoltage.DocTests" with a hash and range key on attributes "tab_h" and "tab_r", you can put an item and get it back:

.. doctest::

    >>> connection(PutItem("LowVoltage.DocTests", {"tab_h": u"foobar", "tab_r": 57, "a": 42, "b": u"toto"}))
    <LowVoltage.actions.put_item.Result object at ...>

    >>> connection(GetItem("LowVoltage.DocTests", {"tab_h": u"foobar", "tab_r": 57})).item
    {u'a': 42, u'b': u'toto', u'tab_h': u'foobar', u'tab_r': 57}

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
