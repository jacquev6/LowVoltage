# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime
import glob
import io
import os
import stat
import subprocess
import tarfile

import requests
try:
    from testresources import TestResourceManager, ResourcedTestCase
except ImportError:  # pragma no cover (Test code)
    class TestResourceManager(object):
        pass

    class ResourcedTestCase(object):
        pass

import LowVoltage as _lv


class DynamoDbLocalResourceManager(TestResourceManager):
    def make(self, dependencies):
        self.__download_if_needed()

        self.__process = subprocess.Popen(
            # ["sleep 7; java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -inMemory -port 65432"],
            ["java", "-Djava.library.path=./DynamoDBLocal_lib", "-jar", "DynamoDBLocal.jar", "-inMemory", "-port", "65432"],
            cwd=".dynamodblocal",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        return _lv.Connection(
            "us-west-2",
            _lv.StaticCredentials("DummyKey", "DummySecret"),
            endpoint="http://localhost:65432/",
            retry_policy=_lv.ExponentialBackoffRetryPolicy(1, 2, 5),
        )

    def __download_if_needed(self):  # pragma no cover (Test code)
        if not os.path.exists(".dynamodblocal/DynamoDBLocal.jar"):
            archive = requests.get("http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest").content
            tarfile.open(fileobj=io.BytesIO(archive)).extractall(".dynamodblocal")
            # Fix permissions, needed at least when running with Cygwin's Python
            for f in glob.glob(".dynamodblocal/DynamoDBLocal_lib/*"):
                os.chmod(f, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

    def clean(self, resource):
        self.__process.kill()


class LocalIntegTests(ResourcedTestCase):
    resources = [("connection", DynamoDbLocalResourceManager())]

    before_start = datetime.datetime.utcnow()
    after_end = before_start + datetime.timedelta(minutes=10)

    def assertDateTimeIsReasonable(self, t):
        self.assertGreaterEqual(t, self.before_start)
        self.assertLessEqual(t, self.after_end)


class LocalIntegTestsWithTableH(LocalIntegTests):
    def setUp(self):
        super(LocalIntegTestsWithTableH, self).setUp()
        self.connection(
            _lv.CreateTable("Aaa").hash_key("h", _lv.STRING).provisioned_throughput(1, 1)
        )

    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        super(LocalIntegTestsWithTableH, self).tearDown()


class LocalIntegTestsWithTableHR(LocalIntegTests):
    def setUp(self):
        super(LocalIntegTestsWithTableHR, self).setUp()
        self.connection(
            _lv.CreateTable("Aaa")
                .hash_key("h", _lv.STRING)
                .range_key("r", _lv.NUMBER)
                .provisioned_throughput(1, 1)
        )

    def tearDown(self):
        self.connection(_lv.DeleteTable("Aaa"))
        super(LocalIntegTestsWithTableHR, self).tearDown()
