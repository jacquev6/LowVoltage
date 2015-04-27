==========
User guide
==========

Introduction
============

Hopefully the :ref:`quick-start` was enough to get you started.

I do my best to respect standards, so alternative installation methods like ``easy_install LowVoltage`` or ``python setup.py install`` in the source code should work as well as ``pip install LowVoltage``.
You may want to use `virtualenv <https://virtualenv.pypa.io/en/latest/>`__.

Regarding imports, I suggest you ``import LowVoltage as lv`` in your production code.
All this documentation assumes that everything was imported in the global namespace: ``from LowVoltage import *``.
Note that everything is available in the top-level module, so you don't have to import nested modules.

Beware that LowVoltage is just a client library for DynamoDB, so you have to be familiar with the underlying concepts of this No-SQL database.
AWS provides `a lot of documentation <http://aws.amazon.com/documentation/dynamodb/>`__ and this documentation often links to it.

I'd like all communication about LowVoltage to be public so please use `GitHub issues <http://github.com/jacquev6/LowVoltage/issues>`__ for your questions and remarks as well as for reporting bugs.

Why?
====

- I wanted to learn DynamoDB
- I found out Boto is (was?) not up-to-date with newer API parameters and does (did?) not support Python 3
- I had some time and I love programming

.. _tenets:

Tenets
======

Users should be able to do everything that is permited by `the API <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference>`__.
--------------------------------------------------------------------------------------------------------------------------------------

There is nothing worse (well, maybe there is...) than knowing how to do something using an API but being incapacited by a client library.
So in LowVoltage we provide a first layer, :ref:`actions`, that maps exactly to the API.
Please `open an issue <http://github.com/jacquev6/LowVoltage/issues/new?title=Actions:%20missing%20functionality>`__ if something is missing!

Users should never risk a typo
------------------------------

We provide symbols for all DynamoDB constants.

Users should be able to choose simplicity over flexibility
----------------------------------------------------------

Even if you *want* to be able to :meth:`~.PutItem.return_item_collection_metrics_size`, most of the time you don't care. And processing :attr:`~.BatchWriteItemResponse.unprocessed_items` should be abstracted away. So we provide :ref:`compounds`. More details in :ref:`actions-vs-compounds` bellow.

The connection
==============

The main entry point of LowVoltage is the :class:`.Connection` class.

    >>> from LowVoltage import *
    >>> connection = Connection("us-west-2", EnvironmentCredentials())

Authentication
--------------

DynamoDB requires authentication and signing of the requests.
In LowVoltage we use simple dependency injection of a credentials provider (:class:`.EnvironmentCredentials` in the example above).
This approach is flexible and allows implementation of any credentials rotation policy.
LowVoltage comes with credentials providers for simple use cases.
See :mod:`.credentials` for details.

Error handling
--------------

Some errors are retryable: :exc:`.NetworkError` or :exc:`.ServerError` for example.
The :class:`.Connection` accepts a ``retry_policy`` parameter to specify this behavior.
See :mod:`.retry_policies` for details.
See also :mod:`.exceptions` for a description of the exceptions classes.

.. _actions-vs-compounds:

Actions vs. compounds
=====================

As briefly described in the :ref:`tenets`, LowVoltage is built in successive layers of increasing abstraction and decreasing flexibility.

The :ref:`actions` layer provides almost no abstraction over `the API <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference>`__.
It maps exactly to the DynamoDB actions, parameters and return values.
It does convert between :ref:`python-types` and DynamoDB notation though.

    >>> table = "LowVoltage.Tests.Doc.1"
    >>> connection(GetItem(table, {"h": 0})).item
    {u'h': 0, u'gr': 0, u'gh': 0}

The :ref:`compounds` layer provides helper functions that intend to complete actions in their simplest use cases.
For example :class:`.BatchGetItem` is limited to get 100 keys at once and requires processing :attr:`.BatchGetItemResponse.unprocessed_keys`, so we provide :func:`.iterate_batch_get_item` to do that.
The tradeoff is that you loose :attr:`.BatchGetItemResponse.consumed_capacity` and the ability to get items from several tables at once.
Similarly :func:`.batch_put_item` removes the limit of 25 items in :class:`.BatchWriteItem` but also removes the ability to put and delete from several tables in the same action.

    >>> batch_put_item(connection, table, {"h": 0, "a": 42}, {"h": 1, "b": 53})

Actions are instances that are *passed to* the :class:`.Connection` but compounds are functions that *receive* the connection as an argument:
actions are atomic while compounds are able to perform several actions.

Someday, maybe, we'll write a Table abstraction and implement an "active record" pattern? It would be even simpler than compounds, but less flexible.

Action building
===============

@todo Implement what's described bellow.

DynamoDB actions typically receive a lot of mandatory and optional parameters.

When you build an action, you *can* pass all mandatory parameters to the constructor.
You may want to use named parameters to reduce the risk of giving them in the wrong order.

    >>> GetItem("Table", {"h": 0})
    <LowVoltage.actions.get_item.GetItem object at ...>

Optional parameters are exposed only using `method chaining <https://en.wikipedia.org/wiki/Method_chaining#Python>`__ to avoid giving them in the wrong order.

    >>> GetItem("Table", {"h": 0}).return_consumed_capacity_total()
    <LowVoltage.actions.get_item.GetItem object at ...>

Alternatively, mandatory parameters can be set using method chaining as well.

    >>> GetItem().table_name("Table").key({"h": 0})
    <LowVoltage.actions.get_item.GetItem object at ...>

If you try to pass an action with missing mandatory parameters, you'll get a :exc:`.BuilderError`:

    >>> connection(GetItem().key({"h": 0}))
    Traceback (most recent call last):
      ...
    BuilderError: ...

Active resource
---------------

Some actions can operate on several resources.
:class:`.BatchGetItem` can get items from several tables at once for example.
To build those, we need a concept of "active table": :meth:`.BatchGetItem.keys` will always add keys to get from the active table.
The active table is set by :meth:`.BatchGetItem.table`.

    >>> (BatchGetItem()
    ...   .table("Table1").keys({"h": 0})
    ...   .table("Table2").keys({"x": 42})
    ... )
    <LowVoltage.actions.batch_get_item.BatchGetItem object at ...>

The previous example will get ``{"h": 0}`` from ``Table1`` and ``{"x": 42}`` from ``Table2``.

.. _variadic-functions:

Variadic functions
------------------

Some methods, like :meth:`.BatchGetItem.keys` are variadic.
But a special kind of variadic: not only do they accept any number of parameters, but for greater flexibility those arguments can also be iterable.

    >>> (BatchGetItem().table("Table1")
    ...   .keys({"h": 0})
    ...   .keys({"h": 1}, {"h": 2})
    ...   .keys({"h": 3}, [{"h": 4}, {"h": 5}], {"h": 6}, [{"h": 7}])
    ...   .keys({"h": h} for h in range(8, 12))
    ...   .keys(({"h": h} for h in range(12, 17)), {"h": 17}, [{"h": h} for h in range(18, 20)])
    ... )
    <LowVoltage.actions.batch_get_item.BatchGetItem object at ...>

Expressions
===========

@todoc Condition, projection, attribute names, attribute values.
@todoc Cross link here, next gen mixins and expression builders.
