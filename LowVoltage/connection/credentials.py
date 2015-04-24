# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
@todo Link to user guide (authentication).

Authentication credentials are passed to the connection as a credentials provider.
On each request, the connection retrieves a key/secret pair from the credentials provider, uses it to sign the request, and then discards it.
This allows credentials rotation in the same long-lived connection: the credential provider just has to return the new credentials.

.. py:class:: Credentials

    The interface to be implemented by all credential providers. Note that you must not inherit from this class, just implement the same interface.

    .. py:method:: get()

        Return the key/secret to be used to sign the next request.

        :type: tuple of two strings
"""

import os


class StaticCredentials(object):
    """
    The simplest credential provider: a constant key/secret pair.
    """

    def __init__(self, key, secret):
        self.__credentials = (key, secret)

    def get(self):
        return self.__credentials


class EnvironmentCredentials(object):
    """
    Credential provider reading the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.
    """

    def __init__(self):
        os.environ["AWS_ACCESS_KEY_ID"]
        os.environ["AWS_SECRET_ACCESS_KEY"]

    def get(self):
        return (os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
