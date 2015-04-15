# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from .actions.unit_tests import *
from .testing.unit_tests import *

from .connection import BasicConnectionUnitTests, RetryingConnectionUnitTests
from .policies import ExponentialBackoffErrorPolicyUnitTests, FailFastErrorPolicyUnitTests


if __name__ == "__main__":  # pragma no branch (Test code)
    unittest.main()  # pragma no cover (Test code)
