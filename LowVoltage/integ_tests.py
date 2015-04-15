# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import LowVoltage.testing.dynamodb_local
from .actions.integ_tests import *
from .testing.integ_tests import *
from .connection import BasicConnectionIntegTests, RetryingConnectionIntegTests, CompletingConnectionIntegTests


if __name__ == "__main__":  # pragma no branch (Test code)
	with LowVoltage.testing.dynamodb_local.DynamoDbLocal():  # pragma no cover (Test code)
		unittest.main()  # pragma no cover (Test code)
