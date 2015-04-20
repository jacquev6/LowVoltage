# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import testresources

from .cover import cover
from .local_integ_tests import *
from .connected_integ_tests import *


def fix_table_description(d):
    d.attribute_definitions = sorted(d.attribute_definitions, key=lambda d: d.attribute_name)


def main():  # pragma no cover (Test code)
    unittest.main(catchbreak=True, testLoader=testresources.TestLoader())
