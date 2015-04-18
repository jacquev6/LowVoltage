# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

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
