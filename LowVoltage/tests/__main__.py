# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import LowVoltage.tests.dynamodb_local
from LowVoltage.tests.all import *


with LowVoltage.tests.dynamodb_local.DynamoDbLocal():
    unittest.main()
