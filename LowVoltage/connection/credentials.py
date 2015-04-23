# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import os


class StaticCredentials(object):
    """The simplest credential provider: a constant key/secret pair"""

    def __init__(self, key, secret):
        self.__credentials = (key, secret)

    def get(self):
        return self.__credentials


class EnvironmentCredentials(object):
    """Credential provider reading the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables"""

    # @todo Fail fast if variables not in environ

    def get(self):
        return (os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
