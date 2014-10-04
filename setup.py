#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

import setuptools

version = "0.1.0"


if __name__ == "__main__":
    setuptools.setup(
        name="LowVoltage",
        version=version,
        description="Standalone DynamoDB client not hiding any feature",
        author="Vincent Jacques",
        author_email="vincent@vincent-jacques.net",
        url="http://jacquev6.github.io/LowVoltage",
        packages=[
            "LowVoltage",
        ],
        classifiers=[
            "Development Status :: 3 - Alpha",
            "License :: OSI Approved",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 2.7",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.3",
            "Environment :: Web Environment",
        ],
        test_suite="LowVoltage",
        use_2to3=True
    )
