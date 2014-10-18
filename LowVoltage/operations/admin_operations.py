# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

from LowVoltage.operations.operation import Operation


class ListTables(Operation):
	class Result(object):
		def __init__(self, TableNames, LastEvaluatedTableName=None):
			self.table_names = TableNames
			self.last_evaluated_table_name = LastEvaluatedTableName

	def __init__(self):
		super(ListTables, self).__init__("ListTables")

	def build(self):
		data = {}
		return data
