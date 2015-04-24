# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
@todo Link to user guide (error management).

Some errors are retryable: :exc:`.NetworkError` or :exc:`.ProvisionedThroughputExceededException` for example.
When they happen, it makes sense to wait for a short while and then retry the request.
Retry policies decide if it's worth retrying and how long to wait.

.. py:class:: RetryPolicy

    The interface to be implemented by all retry policies. Note that you must not inherit from this class, just implement the same interface.

    .. py:method:: retry(action, exceptions)

        Return the delay before retrying the action. ``None`` if the action shouldn't be retried.

        :param action: the action that failed.
        :param exceptions: the (retryable) exceptions that occured so far. The most recent exception is ``exceptions[-1]``.

        :type: ``None`` or number (in seconds)
"""

import LowVoltage as _lv
import LowVoltage.testing as _tst


class ExponentialBackoffRetryPolicy(object):
    """
    Retry failed requests with a waiting time growing exponentialy.

    :param first_wait: the duration to wait before the first retry.
    :param multiplier: the factor by which to augment the waiting duration between successive retries. Greater than 1.
    :param max_retries: the maximum number of times to retry a failed action (0 meaning "don't retry", 1 meaning "retry once", etc.).
    """
    def __init__(self, first_wait, multiplier, max_retries):
        self.__first_wait = first_wait
        self.__multiplier = multiplier
        self.__max_retries = max_retries

    def retry(self, action, exceptions):
        # @todo Should we wait different times for different errors?
        if len(exceptions) > self.__max_retries:
            return None
        else:
            return self.__first_wait * (self.__multiplier ** (len(exceptions) - 1))

    # This is needed for the documentation of DEFAULT
    def __repr__(self):
        return "ExponentialBackoffRetryPolicy({}, {}, {})".format(self.__first_wait, self.__multiplier, self.__max_retries)


DEFAULT = ExponentialBackoffRetryPolicy(1, 1.5, 4)
"""
The default retry policy: a reasonable exponential backoff.
"""


FAIL_FAST = ExponentialBackoffRetryPolicy(1, 1, 0)
"""
A retry policy that never retries anything.
"""


class ExponentialBackoffRetryPolicyUnitTests(_tst.UnitTests):
    def setUp(self):
        self.policy = ExponentialBackoffRetryPolicy(1, 3, 3)
        super(ExponentialBackoffRetryPolicyUnitTests, self).setUp()

    def test_wait_after_first_failure(self):
        self.assertEqual(
            self.policy.retry(object(), [_lv.ServerError()]),
            1
        )

    def test_wait_after_second_failure(self):
        self.assertEqual(
            self.policy.retry(object(), [_lv.ServerError(), _lv.ServerError()]),
            3
        )

    def test_wait_after_third_failure(self):
        self.assertEqual(
            self.policy.retry(object(), [_lv.ServerError(), _lv.ServerError(), _lv.ServerError()]),
            9
        )

    def test_give_up_after_fourth_failure(self):
        self.assertIsNone(self.policy.retry(object(), [_lv.ServerError(), _lv.ServerError(), _lv.ServerError(), _lv.ServerError()]))

    def test_repr(self):
        self.assertEqual(repr(self.policy), "ExponentialBackoffRetryPolicy(1, 3, 3)")
