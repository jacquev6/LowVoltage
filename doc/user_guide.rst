==========
User guide
==========

@todoc Add examples to the user guide.

Introduction
============

Hopefully the :ref:`quick-start` was enough to get you started.

I do my best to respect standards, so alternative installation methods like ``easy_install LowVoltage`` or ``python setup.py install`` in the source code should work as well as ``pip install LowVoltage``.
You may want to use `virtualenv <https://virtualenv.pypa.io/en/latest/>`__.

Regarding imports, I suggest you ``import LowVoltage as lv`` in your production code.
All this documentation assumes that everything was imported in teh global namespace: ``from LowVoltage import *``.
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

Authentication
==============

DynamoDB requires authentication and signing of the requests.
In LowVoltage we use simple dependency injection of a credentials provider.
This approach is flexible and allows implementation of any credentials rotation policy.
LowVoltage comes with credentials providers for simple use cases.
See the :mod:`.credentials` documentation for details.

.. _actions-vs-compounds:

Actions vs. compounds
=====================

As briefly described in the :ref:`tenets`, LowVoltage is built in successive layers of increasing abstraction and decreasing flexibility.

The :ref:`actions` layer provides almost no abstraction over `the API <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference>`__.
It maps exactly to the DynamoDB actions, parameters and return values. It does convert between Python types and DynamoDB notation though.

The :ref:`compounds` layer provides helper that intend to complete actions in their simplest use cases.
For example :class:`.BatchGetItem` is limited to get 100 keys at once and requires processing :attr:`.BatchGetItemResponse.unprocessed_keys`, so we provide :class:`.BatchGetItemIterator` to do that.
The tradeoff is that you loose :attr:`.BatchGetItemResponse.consumed_capacity` and the ability to get items from several tables at once.
Similarly :func:`.BatchPutItem` remove the limit of 25 items in :class:`.BatchWriteItem` but also removes the ability to put and delete from several tables in the same action.

You can easily distinguish between actions and compounds because actions are passed to the :class:`.Connection` but compounds *receive* the connection as an argument:
actions are atomic and compounds are able to perform several actions.

Someday, maybe, we'll write a Table abstraction and implement an "active record" pattern? It would be even simpler than compounds, but less flexible.

Action building
===============

DynamoDB actions typically receive a lot of arguments.
We chose to expose them using `method chaining <https://en.wikipedia.org/wiki/Method_chaining#Python>`__ to reduce the risk of giving them in the wrong order.
We believe this gives a better interface in our case than encouraging clients to use named parameters.

@todoc The notion of active [table|index].

Expressions
===========

@todoc Condition, projection, attribute names, attribute values. @todoc add links to here in the next gen mixins.

Error/retry strategy
====================

@todoc
