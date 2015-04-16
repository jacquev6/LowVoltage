# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.actions.tests.unit import *
from LowVoltage.testing.tests.unit import *

from LowVoltage.connection import BasicConnectionUnitTests, RetryingConnectionUnitTests, CompletingConnectionUnitTests
from LowVoltage.policies import ExponentialBackoffErrorPolicyUnitTests, FailFastErrorPolicyUnitTests


if __name__ == "__main__":  # pragma no branch (Test code)
    unittest.main()  # pragma no cover (Test code)
