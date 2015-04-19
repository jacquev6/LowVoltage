# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime
import unittest

import LowVoltage as _lv
from .dynamodb_local import LocalIntegTests


class LocalIntegTestsWithTableH(LocalIntegTests):
    def setUp(self):
        self.connection.request(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 1)
        )
        self.setUpItems()

    def setUpItems(self):
        pass

    def tearDown(self):
        self.connection.request(_lv.DeleteTable("Aaa"))


class LocalIntegTestsWithTableHR(LocalIntegTests):
    def setUp(self):
        self.connection.request(
            _lv.CreateTable("Aaa")
                .hash_key("h", _lv.STRING)
                .range_key("r", _lv.NUMBER)
                .provisioned_throughput(1, 1)
        )
        self.setUpItems()

    def setUpItems(self):
        pass

    def tearDown(self):
        self.connection.request(_lv.DeleteTable("Aaa"))


class ConnectedIntegTests(unittest.TestCase):
    __execution_date = datetime.datetime.now().strftime("%Y-%m-%d.%H-%M-%S.%f")

    @classmethod
    def make_table_name(cls, base_name=None):
        assert cls.__name__.endswith("ConnectedIntegTests")
        parts = ["LowVoltage", "IntegTests", cls.__execution_date, cls.__name__[:-19]]
        if base_name is not None:
            parts.append(base_name)
        return ".".join(parts)

    @classmethod
    def setUpClass(cls):
        cls.connection = _lv.make_connection("eu-west-1", _lv.EnvironmentCredentials())
