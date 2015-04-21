#!/usr/bin/env python
# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import contextlib
import os
import setuptools
import setuptools.command.test


version = "0.2.3"


setuptools.setup(
    name="LowVoltage",
    version=version,
    description="Standalone DynamoDB client not hiding any feature",
    author="Vincent Jacques",
    author_email="vincent@vincent-jacques.net",
    url="http://jacquev6.github.io/LowVoltage",
    packages=sorted(dirpath.replace("/", ".") for dirpath, dirnames, filenames in os.walk("LowVoltage") if "__init__.py" in filenames),
    classifiers=[
        "Development Status :: 3 - Alpha",
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
    test_suite="LowVoltage.tests" if "AWS_ACCESS_KEY_ID" in os.environ else "LowVoltage.tests.local",
    test_loader="testresources:TestLoader",
    use_2to3=True,
)
