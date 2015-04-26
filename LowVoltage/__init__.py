# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
All public symbols are visible in the top-level module, no need to import submodules.

All examples in this doc depend on:

>>> from LowVoltage import *
>>> table = "LowVoltage.Tests.Doc.1"
>>> table2 = "LowVoltage.Tests.Doc.2"
>>> connection = Connection("us-west-2", EnvironmentCredentials())
"""

from actions import *
from attribute_types import *
from compounds import *
from connection import *
from exceptions import *

# @todo __str__ and __repr__
# @todo create builder for attribute paths
# @todo improve builder for expressions
# @todo metrics
# @todo debug logging
# @todo Table abstraction (will DescribeTable to know the keys and indexes available, and choose the right index to Query, maybe even do a GetItem if the query is key_eq on ha&sh and range.) Higher level than compounds.
