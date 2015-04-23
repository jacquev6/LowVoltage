# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
In DynamoDB, the key attributes are typed.
Here are a few constants for those types, to be used in :class:`.CreateTable` or compared to what :class:`.DescribeTable` returns.
"""

STRING = "S"
"The 'string' attribute type"

NUMBER = "N"
"The 'number' attribute type"

BINARY = "B"
"The 'binary' attribute type"
