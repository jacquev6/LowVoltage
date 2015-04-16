#!/usr/bin/env python
# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import os
import setuptools
import setuptools.command.test

version = "0.1.0"


class TestCommand(setuptools.command.test.test):
    def run_tests(self, *args, **kwds):
        import LowVoltage.testing.dynamodb_local
        with LowVoltage.testing.dynamodb_local.DynamoDbLocal():
            setuptools.command.test.test.run_tests(self, *args, **kwds)


def find_packages(directory):
    for dirpath, dirnames, filenames in os.walk(directory):
        if "__init__.py" in filenames:
            yield dirpath.replace("/", ".")


setuptools.setup(
    name="LowVoltage",
    version=version,
    description="Standalone DynamoDB client not hiding any feature",
    author="Vincent Jacques",
    author_email="vincent@vincent-jacques.net",
    url="http://jacquev6.github.io/LowVoltage",
    packages=sorted(find_packages("LowVoltage")),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Environment :: Web Environment",
    ],
    test_suite="LowVoltage.tests.local",
    use_2to3=True,
    cmdclass={"test": TestCommand},
)
