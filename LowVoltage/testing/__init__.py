# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest
import warnings

try:
    from testresources import TestLoader
except ImportError:  # pragma no cover (Test code)
    from unittest import TestLoader

from .unit_tests import *
from .local_integ_tests import *
from .connected_integ_tests import *


def main():  # pragma no cover (Test code)
    try:
        import testresources
        import MockMockMock
    except ImportError:
        warnings.warn("You are running a subset of LowVoltage's tests. 'pip install testresources MockMockMock' to enable the full test suite.")
    unittest.main(catchbreak=True, testLoader=TestLoader())
