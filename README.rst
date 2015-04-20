LowVoltage is a standalone Python (2 and 3) client for DynamoDB that doesn't hide any feature of the API.

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

Todo
====

- __str__ and __repr__
- docs
- credential provider for AWS's AIM roles
- create builder for attribute paths
- improve builder for expressions
- metrics
- debug logging

Status
======

.. image:: https://travis-ci.org/jacquev6/LowVoltage.svg?branch=master
    :target: https://travis-ci.org/jacquev6/LowVoltage

.. image:: https://coveralls.io/repos/jacquev6/LowVoltage/badge.png
    :target: https://coveralls.io/r/jacquev6/LowVoltage
