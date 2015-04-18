# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import dynamodb_local
from .cover import cover
from .test_cases import *


def fix_table_description(d):
    d.attribute_definitions = sorted(d.attribute_definitions, key=lambda d: d.attribute_name)
