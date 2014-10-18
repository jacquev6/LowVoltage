LowVoltage is a Python (2 and 3) client for DynamoDB that doesn't hide any feature of the API.

This library is at a very early stage. You may not want to use it yet.

Tenets
======

With LowVoltage, you should:

- be able to do everything that is permited by `the API <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference>`__
- never have to type a literal for a DynamoDB constant (@todo rephrase: we provide constants and methods for everything that is not variable in all API calls. We also provide builders for expressions, etc. so that the user never has to provide a string. But he can.)
- never have to use deprecated API parameters
- chose what level of abstraction you want to use (@todo link to the doc of different levels of abstraction)

Status
======

.. image:: https://travis-ci.org/jacquev6/LowVoltage.svg?branch=master
    :target: https://travis-ci.org/jacquev6/LowVoltage

.. image:: https://coveralls.io/repos/jacquev6/LowVoltage/badge.png
    :target: https://coveralls.io/r/jacquev6/LowVoltage
