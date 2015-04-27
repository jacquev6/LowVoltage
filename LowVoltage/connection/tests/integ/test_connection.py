# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import LowVoltage as _lv
import LowVoltage.testing as _tst


class ConnectionLocalIntegTests(_tst.LocalIntegTests):
    class TestAction:
        class response_class(object):
            def __init__(self, **kwds):
                self.kwds = kwds

        def __init__(self, name, payload):
            self.name = name
            self.payload = payload

    def test_network_error(self):
        connection = _lv.Connection("us-west-2", _lv.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65555/", _lv.ExponentialBackoffRetryPolicy(0, 1, 3))
        with self.assertRaises(_lv.NetworkError):
            connection(self.TestAction("ListTables", {}))

    def test_request(self):
        r = self.connection(self.TestAction("ListTables", {}))
        self.assertIsInstance(r, self.TestAction.response_class)
        self.assertEqual(r.kwds, {"TableNames": []})

    def test_client_error(self):
        with self.assertRaises(_lv.InvalidAction):
            self.connection(self.TestAction("UnexistingAction", {}))

    def test_unexisting_table(self):
        with self.assertRaises(_lv.ResourceNotFoundException):
            self.connection(self.TestAction("GetItem", {"TableName": "Bbb"}))
