==========
User guide
==========

Introduction
============

Why?
====

- I wanted to learn DynamoDB
- I found out Boto is (was?) not up-to-date with newer API parameters and does (did?) not support Python 3
- I had some time

Tenets
======

- Users should be able to do everything that is permited by `the API <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference>`__.
- Users should never risk a typo: we provide symbols for all DynamoDB constants.
- Users should never have to use deprecated API parameters.

Everything is available in top-level module
===========================================

Credentials
===========

Actions vs. compounds
=====================

Actions: zero abstraction, total flexibility.

Compounds: some features disappear (BatchGet from several tables, return consumed capacity) but usage is simpler.

Builders (with proxies) for actions
===================================

Expressions
===========

Condition, projection, attribute names, attribute values. @todo add links to here in the next gen mixins.

Error/retry strategy
====================

Someday, maybe, an ORM-like?
============================

Even simpler than compounds, even less flexible.
