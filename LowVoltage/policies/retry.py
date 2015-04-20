# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import unittest

import LowVoltage as _lv


class ExponentialBackoffRetryPolicy(object):
    def __init__(self, first_wait, multiplier, max_tries):
        self.__first_wait = first_wait
        self.__multiplier = multiplier
        self.__max_tries = max_tries

    def get_retry_delay_on_exception(self, action, exception, errors):
        # @todo Should we wait different times for different errors?
        if errors >= self.__max_tries or not exception.retryable:
            return None
        else:
            return self.__first_wait * (self.__multiplier ** (errors - 1))


class ExponentialBackoffRetryPolicyUnitTests(unittest.TestCase):
    def setUp(self):
        self.policy = ExponentialBackoffRetryPolicy(1, 3, 4)
        super(ExponentialBackoffRetryPolicyUnitTests, self).setUp()

    def test_wait_after_first_failure(self):
        self.assertEqual(
            self.policy.get_retry_delay_on_exception(object(), _lv.ServerError(), 1),
            1
        )

    def test_wait_after_second_failure(self):
        self.assertEqual(
            self.policy.get_retry_delay_on_exception(object(), _lv.ServerError(), 2),
            3
        )

    def test_wait_after_third_failure(self):
        self.assertEqual(
            self.policy.get_retry_delay_on_exception(object(), _lv.ServerError(), 3),
            9
        )

    def test_wait_after_fourth_failure(self):
        self.assertIsNone(self.policy.get_retry_delay_on_exception(object(), _lv.ServerError(), 4))
