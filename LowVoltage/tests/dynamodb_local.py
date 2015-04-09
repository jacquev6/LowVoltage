# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import datetime
import glob
import io
import os
import stat
import subprocess
import tarfile
import unittest

import requests

import LowVoltage


class DynamoDbLocal(object):  # pragma no cover (Test code)
    def __init__(self):
        self.__process = None

    def __enter__(self):
        if not os.path.exists(".dynamodblocal/DynamoDBLocal.jar"):
            archive = requests.get("http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest").content
            tarfile.open(fileobj=io.BytesIO(archive)).extractall(".dynamodblocal")
            # Fix permissions, needed at least when running with Cygwin's Python
            for f in glob.glob(".dynamodblocal/DynamoDBLocal_lib/*"):
                os.chmod(f, stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR)

        self.__process = subprocess.Popen(
            ["java", "-Djava.library.path=./DynamoDBLocal_lib", "-jar", "DynamoDBLocal.jar", "-inMemory", "-port", "65432"],
            cwd=".dynamodblocal",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

    def __exit__(self, *dummy):
        self.__process.kill()
        self.__process = None


class TestCase(unittest.TestCase):
    before_start = datetime.datetime.utcnow()
    after_end = before_start + datetime.timedelta(minutes=10)

    @classmethod
    def setUpClass(cls):
        cls.connection = LowVoltage.Connection("us-west-2", LowVoltage.StaticCredentials("DummyKey", "DummySecret"), "http://localhost:65432/")

    def assertDateTimeIsReasonable(self, t):
        self.assertGreaterEqual(t, self.before_start)
        self.assertLessEqual(t, self.after_end)


def main(*args, **kwds):  # pragma no cover (Test code)
    with DynamoDbLocal():
        unittest.main(*args, **kwds)
