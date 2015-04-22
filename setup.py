#!/usr/bin/env python
# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import contextlib
import os
import setuptools
import setuptools.command.test


version = "0.2.6"


setuptools.setup(
    name="LowVoltage",
    version=version,
    description="Standalone DynamoDB client not hiding any feature",
    author="Vincent Jacques",
    author_email="vincent@vincent-jacques.net",
    url="http://pythonhosted.org/LowVoltage",
    packages=setuptools.find_packages(),
    license="MIT",
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
    test_suite="LowVoltage.tests" if "AWS_ACCESS_KEY_ID" in os.environ else "LowVoltage.tests.local",
    test_loader="testresources:TestLoader",
    use_2to3=True,
    command_options={
        "build_sphinx": {
            "version": ("setup.py", version),
            "release": ("setup.py", version),
            "source_dir": ("setup.py", "doc"),
        }
    }
)
