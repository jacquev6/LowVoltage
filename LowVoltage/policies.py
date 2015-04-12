# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import exceptions as _exn


class FailFastErrorPolicy(object):
    def get_retry_delay_on_exception(self, action, exception, errors):
        return None


class FailFastErrorPolicyUnitTests(unittest.TestCase):
    def setUp(self):
        self.policy = FailFastErrorPolicy()

    def test(self):
        self.assertIsNone(self.policy.get_retry_delay_on_exception(object(), _exn.ServerError(), 0))


class ExponentialBackoffErrorPolicy(object):
    def __init__(self, first_wait, multiplier, max_tries):
        self.__first_wait = first_wait
        self.__multiplier = multiplier
        self.__max_tries = max_tries

    def get_retry_delay_on_exception(self, action, exception, errors):
        # @todo Should we wait different times for different errors?
        if errors >= self.__max_tries or not isinstance(exception, (_exn.ServerError, _exn.NetworkError, _exn.ProvisionedThroughputExceededException)):
            return None
        else:
            return self.__first_wait * (self.__multiplier ** (errors - 1))


class ExponentialBackoffErrorPolicyUnitTests(unittest.TestCase):
    def setUp(self):
        self.policy = ExponentialBackoffErrorPolicy(1, 3, 4)

    def test_wait_after_first_failure(self):
        self.assertEqual(
            self.policy.get_retry_delay_on_exception(object(), _exn.ServerError(), 1),
            1
        )

    def test_wait_after_second_failure(self):
        self.assertEqual(
            self.policy.get_retry_delay_on_exception(object(), _exn.ServerError(), 2),
            3
        )

    def test_wait_after_third_failure(self):
        self.assertEqual(
            self.policy.get_retry_delay_on_exception(object(), _exn.ServerError(), 3),
            9
        )

    def test_wait_after_fourth_failure(self):
        self.assertIsNone(self.policy.get_retry_delay_on_exception(object(), _exn.ServerError(), 4))
