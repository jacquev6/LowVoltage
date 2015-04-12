# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import LowVoltage.tests.dynamodb_local
from LowVoltage.tests.all import *


with LowVoltage.tests.dynamodb_local.DynamoDbLocal():
    unittest.main()
