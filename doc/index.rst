LowVoltage's documentation
==========================

Contents:

.. toctree::
   :maxdepth: 2

Quick start
===========

Import the package and create a connection:

.. doctest::

    >>> import LowVoltage as lv
    >>> c = lv.make_connection("eu-west-1", lv.EnvironmentCredentials())

Create a table and wait for it to be active:

.. doctest::

    >>> table = "LowVoltage.DocTests"
    >>> r = c.request(
    ...      lv.CreateTable(table)
    ...          .hash_key("h", lv.STRING)
    ...          .provisioned_throughput(1, 1)
    ... )
    >>> r.table_description.table_status
    u'CREATING'
    >>> lv.WaitForTableActivation(c, table)
    >>> r = c.request(lv.DescribeTable(table))
    >>> r.table.table_status
    u'ACTIVE'

Put an item and get it back:

.. doctest::

	>>> r = c.request(lv.PutItem(table, {"h": u"foobar", "a": 42, "b": u"toto"}))
	>>> r = c.request(lv.GetItem(table, {"h": u"foobar"}))
	>>> r.item
	{u'a': 42, u'h': u'foobar', u'b': u'toto'}

Delete the table:

.. doctest::

    >>> r = c.request(lv.DeleteTable(table))
    >>> r.table_description.table_status
    u'DELETING'

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
