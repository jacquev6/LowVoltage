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

        Return the key/secret/token to be used to sign the next request. If the credentials are permanent, return ``None`` as token.

        :type: tuple of (string, string, ``None`` or string)
"""

import datetime
import os

import requests

import LowVoltage.testing as _tst
import LowVoltage.exceptions as _exn


class StaticCredentials(object):
    """
    The simplest credential provider: a constant key/secret pair (and optionally a session token if you know what you're doing).
    """

    def __init__(self, key, secret, token=None):
        self.__key = key
        self.__secret = secret
        self.__token = token

    def get(self):
        return self.__key, self.__secret, self.__token


class EnvironmentCredentials(object):
    """
    Credential provider reading the ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY`` and optionally ``AWS_SECURITY_TOKEN`` environment variables.
    """

    def __init__(self):
        os.environ["AWS_ACCESS_KEY_ID"]
        os.environ["AWS_SECRET_ACCESS_KEY"]

    def get(self):
        return (os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], os.environ.get("AWS_SECURITY_TOKEN"))


class Ec2RoleCredentials(object):
    """
    Credentials provider using EC2 instance metadata to retrieve temporary, automatically rotated, credentials
    from the `IAM role of the instance <http://docs.aws.amazon.com/IAM/latest/UserGuide/roles-usingrole-ec2instance.html>`__.
    Usable *only* on an EC2 instance with an IAM role assigned.

    :param requests_session: a ``Session`` object from the `python-requests <http://python-requests.org>`__ library. Typically not used. Leave it to ``None`` and one will be created for you.
    """

    def __init__(self, requests_session=None):
        if requests_session is None:
            requests_session = requests.Session()

        self.__session = requests_session
        try:
            role = self.__session.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/").text
        except requests.exceptions.RequestException as e:
            raise _exn.NetworkError(e)
        except Exception as e:
            raise _exn.UnknownError(e)
        self.__creds_uri = "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}".format(role)

        self.__key = None
        self.__secret = None
        self.__token = None

        # Dependency injection through monkey-patching
        self.__now = datetime.datetime.utcnow

    def get(self):
        now = self.__now()
        if self.__needs_refresh(now):
            self.__refresh(now)

        return self.__key, self.__secret, self.__token

    def __needs_refresh(self, now):
        if self.__key is None:
            return True
        elif now >= self.__expiration:
            return True
        elif now >= self.__next_refresh:
            return True
        else:
            return False

    def __refresh(self, now):
        try:
            creds = self.__session.get(self.__creds_uri).json()
        except requests.exceptions.RequestException as e:
            raise _exn.NetworkError(e)
        except Exception as e:
            raise _exn.UnknownError(e)
        # {
        #     u'Code': u'Success',
        #     u'LastUpdated': u'2015-04-24T13:36:55Z',
        #     u'AccessKeyId': u'ASIAISBLBZIEQJ6USRAQ',
        #     u'SecretAccessKey': u'xxx',
        #     u'Token': u'yyy',
        #     u'Expiration': u'2015-04-24T19:36:54Z',
        #     u'Type': u'AWS-HMAC'
        # }
        self.__key = creds["AccessKeyId"]
        self.__secret = creds["SecretAccessKey"]
        self.__token = creds["Token"]
        # Refresh every hour and 15 minutes before expiration: http://docs.aws.amazon.com/IAM/latest/UserGuide/roles-usingrole-ec2instance.html
        self.__expiration = datetime.datetime.strptime(creds["Expiration"], "%Y-%m-%dT%H:%M:%SZ") - datetime.timedelta(minutes=15)
        self.__next_refresh = now + datetime.timedelta(hours=1)


class Ec2RoleCredentialsUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(Ec2RoleCredentialsUnitTests, self).setUp()
        self.session = self.mocks.create("session")
        self.response = self.mocks.create("response")

    def test_refresh_scenarios(self):
        self.session.expect.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/").andReturn(self.response.object)
        self.response.expect.text.andReturn("RoleName")

        credentials = Ec2RoleCredentials(self.session.object)

        self.now = self.mocks.replace("credentials._Ec2RoleCredentials__now")

        self.now.expect().andReturn(datetime.datetime(2015, 04, 24, 12, 30, 0))
        self.session.expect.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/RoleName").andReturn(self.response.object)
        self.response.expect.json().andReturn({"AccessKeyId": "key1", "SecretAccessKey": "secret1", "Token": "token1", "Expiration": "2015-04-24T15:00:30Z"})

        self.assertEqual(credentials.get(), ("key1", "secret1", "token1"))

        self.now.expect().andReturn(datetime.datetime(2015, 04, 24, 12, 31, 0))
        self.assertEqual(credentials.get(), ("key1", "secret1", "token1"))

        self.now.expect().andReturn(datetime.datetime(2015, 04, 24, 13, 29, 0))
        self.assertEqual(credentials.get(), ("key1", "secret1", "token1"))

        self.now.expect().andReturn(datetime.datetime(2015, 04, 24, 13, 30, 0))
        self.session.expect.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/RoleName").andReturn(self.response.object)
        self.response.expect.json().andReturn({"AccessKeyId": "key2", "SecretAccessKey": "secret2", "Token": "token3", "Expiration": "2015-04-24T14:00:30Z"})
        self.assertEqual(credentials.get(), ("key2", "secret2", "token3"))

        self.now.expect().andReturn(datetime.datetime(2015, 04, 24, 13, 45, 0))
        self.assertEqual(credentials.get(), ("key2", "secret2", "token3"))

        self.now.expect().andReturn(datetime.datetime(2015, 04, 24, 13, 46, 0))
        self.session.expect.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/RoleName").andReturn(self.response.object)
        self.response.expect.json().andReturn({"AccessKeyId": "key3", "SecretAccessKey": "secret3", "Token": "token3", "Expiration": "2015-04-24T18:00:30Z"})
        self.assertEqual(credentials.get(), ("key3", "secret3", "token3"))

    def test_network_error_during_construction(self):
        with self.assertRaises(_exn.NetworkError):
            Ec2RoleCredentials()

    def test_unknown_error_during_construction(self):
        self.session.expect.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/").andRaise(Exception)

        with self.assertRaises(_exn.UnknownError):
            Ec2RoleCredentials(self.session.object)

    def test_network_error_during_refresh(self):
        self.session.expect.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/").andReturn(self.response.object)
        self.response.expect.text.andReturn("RoleName")

        credentials = Ec2RoleCredentials(self.session.object)

        self.now = self.mocks.replace("credentials._Ec2RoleCredentials__now")

        self.now.expect().andReturn(datetime.datetime(2015, 04, 24, 12, 30, 0))
        self.session.expect.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/RoleName").andRaise(requests.exceptions.RequestException)

        with self.assertRaises(_exn.NetworkError):
            credentials.get()

    def test_unknown_error_during_refresh(self):
        self.session.expect.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/").andReturn(self.response.object)
        self.response.expect.text.andReturn("RoleName")

        credentials = Ec2RoleCredentials(self.session.object)

        self.now = self.mocks.replace("credentials._Ec2RoleCredentials__now")

        self.now.expect().andReturn(datetime.datetime(2015, 04, 24, 12, 30, 0))
        self.session.expect.get("http://169.254.169.254/latest/meta-data/iam/security-credentials/RoleName").andRaise(Exception)

        with self.assertRaises(_exn.UnknownError):
            credentials.get()
