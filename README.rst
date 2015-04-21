LowVoltage is a standalone Python (2 and 3) client for DynamoDB that doesn't hide any feature of the API.

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

.. image:: https://pypip.in/egg/LowVoltage/badge.svg
    :target: https://pypi.python.org/pypi/LowVoltage

.. image:: https://pypip.in/wheel/LowVoltage/badge.svg
    :target: https://pypi.python.org/pypi/LowVoltage

.. image:: https://img.shields.io/github/issues/jacquev6/LowVoltage.svg
    :target: https://github.com/jacquev6/LowVoltage/issues

.. image:: https://img.shields.io/github/forks/jacquev6/LowVoltage.svg
    :target: https://github.com/jacquev6/LowVoltage/network

.. image:: https://img.shields.io/github/stars/jacquev6/LowVoltage.svg
    :target: https://github.com/jacquev6/LowVoltage/stargazers


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
