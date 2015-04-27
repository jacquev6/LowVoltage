# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

import datetime
import hashlib
import hmac
import json
import urlparse
import time

import requests

import LowVoltage as _lv
import LowVoltage.testing as _tst
import LowVoltage.exceptions as _exn
from . import retry_policies


class Connection(object):
    """
    The main entry point of the package.

    :param region: the identifier of the AWS region you want to connect to. Like ``"us-west-2"`` or ``"eu-west-1"``.
    :param credentials: a credentials providers. See :mod:`.credentials`.
    :param endpoint:
        the HTTP endpoint you want to connect to.
        Typically useful to connect to `DynamoDB Local <http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html>`__
        with ``endpoint="http://localhost:8000/"``.
        If left ``None``, it will be computed from the region.
    :param retry_policy: a retry policy. See :mod:`.retry_policies`. If left ``None``, the :obj:`~.retry_policies.DEFAULT` retry policy will be used.
    :param requests_session: a ``Session`` object from the `python-requests <http://python-requests.org>`__ library. Typically not used. Leave it to ``None`` and one will be created for you.
    """

    def __init__(self, region, credentials, endpoint=None, retry_policy=None, requests_session=None):
        if endpoint is None:
            endpoint = "https://dynamodb.{}.amazonaws.com/".format(region)
        if retry_policy is None:
            retry_policy = retry_policies.DEFAULT
        if requests_session is None:
            requests_session = requests.Session()

        self.__region = region
        self.__credentials = credentials
        self.__endpoint = endpoint
        self.__host = urlparse.urlparse(self.__endpoint).hostname
        self.__retry_policy = retry_policy
        self.__session = requests_session

        # Dependency injection through monkey-patching
        self.__signer = Signer(self.__region, self.__host)
        self.__responder = Responder()
        self.__now = datetime.datetime.utcnow

    def __call__(self, action):
        """
        Send requests and return responses.
        """
        errors = []
        while True:
            try:
                return self.__request_once(action)
            except _exn.Error as e:
                if e.retryable:
                    errors.append(e)
                    delay = self.__retry_policy.retry(action, errors)
                    if delay is None:
                        raise
                    else:
                        time.sleep(delay)
                else:
                    raise

    def __request_once(self, action):
        key, secret, token = self.__credentials.get()
        payload = json.dumps(action.payload)
        headers = self.__signer(key, secret, self.__now(), action.name, payload)
        if token is not None:
            headers["X-Amz-Security-Token"] = token
        try:
            r = self.__session.post(self.__endpoint, data=payload, headers=headers)
        except requests.exceptions.RequestException as e:
            raise _exn.NetworkError(e)
        except Exception as e:
            raise _exn.UnknownError(e)

        return self.__responder(action.response_class, r)


class ConnectionUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(ConnectionUnitTests, self).setUp()
        self.credentials = self.mocks.create("credentials")
        self.retry_policy = self.mocks.create("retry_policy")
        self.session = self.mocks.create("session")
        self.connection = Connection(
            region="us-west-2",
            credentials=self.credentials.object,
            endpoint="http://endpoint.com:8000/",
            retry_policy=self.retry_policy.object,
            requests_session=self.session.object,
        )
        self.now = self.mocks.replace("self.connection._Connection__now")
        self.signer = self.mocks.replace("self.connection._Connection__signer")
        self.responder = self.mocks.replace("self.connection._Connection__responder")
        self.action = self.mocks.create("action")

    def test_identification_with_token(self):
        self.credentials.expect.get().andReturn(("a", "b", "t"))
        self.action.expect.payload.andReturn({"d": "e"})
        self.now.expect().andReturn("f")
        self.action.expect.name.andReturn("c")
        self.signer.expect("a", "b", "f", "c", '{"d": "e"}').andReturn({"g": "h"})
        self.session.expect.post("http://endpoint.com:8000/", data='{"d": "e"}', headers={"g": "h", "X-Amz-Security-Token": "t"}).andReturn("i")
        self.action.expect.response_class.andReturn("j")
        self.responder.expect("j", "i").andReturn("k")

        self.assertEqual(self.connection(self.action.object), "k")

    def __expect_post(self):
        self.credentials.expect.get().andReturn(("a", "b", None))
        self.action.expect.payload.andReturn({"d": "e"})
        self.now.expect().andReturn("f")
        self.action.expect.name.andReturn("c")
        self.signer.expect("a", "b", "f", "c", '{"d": "e"}').andReturn({"g": "h"})
        return self.session.expect.post("http://endpoint.com:8000/", data='{"d": "e"}', headers={"g": "h"})

    def test_success_on_first_try(self):
        self.__expect_post().andReturn("i")
        self.action.expect.response_class.andReturn("j")
        self.responder.expect("j", "i").andReturn("k")

        self.assertEqual(self.connection(self.action.object), "k")

    def test_success_on_fourth_try(self):
        self.__expect_post().andReturn("i")
        self.action.expect.response_class.andReturn("j")
        exception1 = _exn.ProvisionedThroughputExceededException()
        self.responder.expect("j", "i").andRaise(exception1)
        self.retry_policy.expect.retry(self.action.object, [exception1]).andReturn(0)

        self.__expect_post().andReturn("k")
        self.action.expect.response_class.andReturn("l")
        exception2 = _exn.ProvisionedThroughputExceededException()
        self.responder.expect("l", "k").andRaise(exception2)
        self.retry_policy.expect.retry(self.action.object, [exception1, exception2]).andReturn(0)

        self.__expect_post().andReturn("m")
        self.action.expect.response_class.andReturn("n")
        exception3 = _exn.ProvisionedThroughputExceededException()
        self.responder.expect("n", "m").andRaise(exception3)
        self.retry_policy.expect.retry(self.action.object, [exception1, exception2, exception3]).andReturn(0)

        self.__expect_post().andReturn("o")
        self.action.expect.response_class.andReturn("p")
        self.responder.expect("p", "o").andReturn("q")

        self.assertEqual(self.connection(self.action.object), "q")

    def test_failure_on_second_try(self):
        self.__expect_post().andReturn("i")
        self.action.expect.response_class.andReturn("j")
        exception1 = _exn.ProvisionedThroughputExceededException()
        self.responder.expect("j", "i").andRaise(exception1)
        self.retry_policy.expect.retry(self.action.object, [exception1]).andReturn(0)

        self.__expect_post().andReturn("k")
        self.action.expect.response_class.andReturn("l")
        exception2 = _exn.UnknownClientError()
        self.responder.expect("l", "k").andRaise(exception2)

        with self.assertRaises(_exn.UnknownClientError) as catcher:
            self.connection(self.action.object)
        self.assertIs(catcher.exception, exception2)

    def test_give_up_after_third_try(self):
        self.__expect_post().andReturn("i")
        self.action.expect.response_class.andReturn("j")
        exception1 = _exn.ProvisionedThroughputExceededException()
        self.responder.expect("j", "i").andRaise(exception1)
        self.retry_policy.expect.retry(self.action.object, [exception1]).andReturn(0)

        self.__expect_post().andReturn("k")
        self.action.expect.response_class.andReturn("l")
        exception2 = _exn.ProvisionedThroughputExceededException()
        self.responder.expect("l", "k").andRaise(exception2)
        self.retry_policy.expect.retry(self.action.object, [exception1, exception2]).andReturn(0)

        self.__expect_post().andReturn("m")
        self.action.expect.response_class.andReturn("n")
        exception3 = _exn.ProvisionedThroughputExceededException()
        self.responder.expect("n", "m").andRaise(exception3)
        self.retry_policy.expect.retry(self.action.object, [exception1, exception2, exception3]).andReturn(None)

        with self.assertRaises(_exn.ProvisionedThroughputExceededException) as catcher:
            self.connection(self.action.object)
        self.assertIs(catcher.exception, exception3)

    def test_success_after_network_error(self):
        exception = requests.exceptions.RequestException()
        self.__expect_post().andRaise(exception)
        self.retry_policy.expect.retry.withArguments(lambda args, kwds: args[0] is self.action.object and isinstance(args[1][0], _exn.NetworkError)).andReturn(0)

        self.__expect_post().andReturn("k")
        self.action.expect.response_class.andReturn("l")
        exception2 = _exn.ProvisionedThroughputExceededException()
        self.responder.expect("l", "k").andReturn("m")

        self.assertEqual(self.connection(self.action.object), "m")

    def test_failure_on_unkown_exception_raised_by_requests(self):
        exception = Exception()
        self.__expect_post().andRaise(exception)

        with self.assertRaises(_exn.UnknownError) as catcher:
            self.connection(self.action.object)
        self.assertEqual(catcher.exception.args, (exception,))

    def test_success_after_network_error_during_credentials(self):
        exception = _exn.NetworkError()
        self.credentials.expect.get().andRaise(exception)
        self.retry_policy.expect.retry(self.action.object, [exception]).andReturn(0)

        self.__expect_post().andReturn("k")
        self.action.expect.response_class.andReturn("l")
        exception2 = _exn.ProvisionedThroughputExceededException()
        self.responder.expect("l", "k").andReturn("m")

        self.assertEqual(self.connection(self.action.object), "m")


class Signer(object):
    def __init__(self, region, host):
        self.__host = host
        self.__region = region

    def __call__(self, key, secret, now, action, payload):
        # http://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html

        timestamp = now.strftime("%Y%m%dT%H%M%SZ")
        datestamp = now.strftime("%Y%m%d")

        headers = {
            "Content-Type": "application/x-amz-json-1.0",
            "X-Amz-Date": timestamp,
            "X-Amz-Target": "DynamoDB_20120810.{}".format(action),
            "Host": self.__host,
        }

        header_names = ";".join(k.lower() for k in sorted(headers.keys()))
        request = "POST\n/\n\n{}\n{}\n{}".format(
            "".join("{}:{}\n".format(k.lower(), v) for k, v in sorted(headers.iteritems())),
            header_names,
            hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        )
        credentials = "{}/{}/dynamodb/aws4_request".format(datestamp, self.__region)
        to_sign = "AWS4-HMAC-SHA256\n{}\n{}\n{}".format(timestamp, credentials, hashlib.sha256(request.encode("utf-8")).hexdigest())

        hmac_key = hmac.new(
            hmac.new(
                hmac.new(
                    hmac.new(
                        "AWS4{}".format(secret).encode("utf-8"),
                        datestamp.encode("utf-8"),
                        hashlib.sha256
                    ).digest(),
                    self.__region.encode("utf-8"),
                    hashlib.sha256
                ).digest(),
                "dynamodb".encode("utf-8"),
                hashlib.sha256
            ).digest(),
            "aws4_request".encode("utf-8"),
            hashlib.sha256
        ).digest()

        headers["Authorization"] = "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}".format(
            key,
            credentials,
            header_names,
            hmac.new(hmac_key, to_sign.encode("utf-8"), hashlib.sha256).hexdigest(),
        )

        return headers


class SignerUnitTests(_tst.UnitTests):
    def test(self):
        signer = Signer("us-west-2", "localhost")
        self.assertEqual(
            signer("DummyKey", "DummySecret", datetime.datetime(2014, 10, 4, 6, 33, 2), "Operation", '{"Payload": "Value"}'),
            {
                "Host": "localhost",
                "Content-Type": "application/x-amz-json-1.0",
                "Authorization": "AWS4-HMAC-SHA256 Credential=DummyKey/20141004/us-west-2/dynamodb/aws4_request, SignedHeaders=content-type;host;x-amz-date;x-amz-target, Signature=f47b4025d95692c1623d01bd7db6d53e68f7a8a28264c1ab3393477f0dae520a",
                "X-Amz-Date": "20141004T063302Z",
                "X-Amz-Target": "DynamoDB_20120810.Operation",
            }
        )


class Responder(object):
    def __call__(self, response_class, r):
        status_code = r.status_code
        if status_code == 200:
            try:
                data = r.json()
            except ValueError:
                raise _exn.ServerError(200, r.text)
            return response_class(**data)
        else:
            self.__raise(status_code, r)

    def __raise(self, status_code, r):
        try:
            data = r.json()
        except ValueError:
            data = r.text
        if isinstance(data, dict):
            typ = data.get("__type")
        else:
            typ = None
        if 400 <= status_code < 500:
            if typ is not None:
                for suffix, cls in _exn.client_errors:
                    if typ.endswith(suffix):
                        raise cls(data)
            raise _exn.UnknownClientError(status_code, data)
        elif 500 <= status_code < 600:
            raise _exn.ServerError(status_code, data)
        else:
            raise _exn.UnknownError(status_code, data)


class ResponderUnitTests(_tst.UnitTestsWithMocks):
    def setUp(self):
        super(ResponderUnitTests, self).setUp()
        self.response_class = self.mocks.create("response_class")
        self.response_instance = object()
        self.requests_response = self.mocks.create("requests_response")
        self.json = {"a": 0}
        self.responder = Responder()

    def test_good_response(self):
        self.requests_response.expect.status_code.andReturn(200)
        self.requests_response.expect.json().andReturn(self.json)
        self.response_class.expect(a=0).andReturn(self.response_instance)

        self.assertIs(self.responder(self.response_class.object, self.requests_response.object), self.response_instance)

    def test_non_json_response_with_good_status(self):
        self.requests_response.expect.status_code.andReturn(200)
        self.requests_response.expect.json().andRaise(ValueError)
        self.requests_response.expect.text.andReturn("foobar")

        with self.assertRaises(_exn.ServerError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (200, "foobar"))

    def test_unknown_client_error_with_correct_json(self):
        self.requests_response.expect.status_code.andReturn(400)
        self.requests_response.expect.json().andReturn({"__type": "NobodyKnewThisCouldHappen", "Message": "tralala"})

        with self.assertRaises(_exn.UnknownClientError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (400, {"__type": "NobodyKnewThisCouldHappen", "Message": "tralala"}))

    def test_unknown_client_error_with_json_without_type(self):
        self.requests_response.expect.status_code.andReturn(400)
        self.requests_response.expect.json().andReturn({"Message": "tralala"})

        with self.assertRaises(_exn.UnknownClientError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (400, {"Message": "tralala"}))

    def test_unknown_client_error_with_non_dict_json(self):
        self.requests_response.expect.status_code.andReturn(400)
        self.requests_response.expect.json().andReturn(["Message", "tralala"])

        with self.assertRaises(_exn.UnknownClientError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (400, ["Message", "tralala"]))

    def test_unknown_client_error_without_json(self):
        self.requests_response.expect.status_code.andReturn(400)
        self.requests_response.expect.json().andRaise(ValueError)
        self.requests_response.expect.text.andReturn("Message: tralala")

        with self.assertRaises(_exn.UnknownClientError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (400, "Message: tralala"))

    def test_server_error_with_correct_json(self):
        self.requests_response.expect.status_code.andReturn(500)
        self.requests_response.expect.json().andReturn({"__type": "NobodyKnewThisCouldHappen", "Message": "tralala"})

        with self.assertRaises(_exn.ServerError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (500, {"__type": "NobodyKnewThisCouldHappen", "Message": "tralala"}))

    def test_server_error_with_json_without_type(self):
        self.requests_response.expect.status_code.andReturn(500)
        self.requests_response.expect.json().andReturn({"Message": "tralala"})

        with self.assertRaises(_exn.ServerError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (500, {"Message": "tralala"}))

    def test_server_error_with_non_dict_json(self):
        self.requests_response.expect.status_code.andReturn(500)
        self.requests_response.expect.json().andReturn(["Message", "tralala"])

        with self.assertRaises(_exn.ServerError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (500, ["Message", "tralala"]))

    def test_server_error_without_json(self):
        self.requests_response.expect.status_code.andReturn(500)
        self.requests_response.expect.json().andRaise(ValueError)
        self.requests_response.expect.text.andReturn("Message: tralala")

        with self.assertRaises(_exn.ServerError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (500, "Message: tralala"))

    def test_unknown_error_with_correct_json(self):
        self.requests_response.expect.status_code.andReturn(750)
        self.requests_response.expect.json().andReturn({"__type": "NobodyKnewThisCouldHappen", "Message": "tralala"})

        with self.assertRaises(_exn.UnknownError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (750, {"__type": "NobodyKnewThisCouldHappen", "Message": "tralala"}))

    def test_unknown_error_with_json_without_type(self):
        self.requests_response.expect.status_code.andReturn(750)
        self.requests_response.expect.json().andReturn({"Message": "tralala"})

        with self.assertRaises(_exn.UnknownError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (750, {"Message": "tralala"}))

    def test_unknown_error_with_non_dict_json(self):
        self.requests_response.expect.status_code.andReturn(750)
        self.requests_response.expect.json().andReturn(["Message", "tralala"])

        with self.assertRaises(_exn.UnknownError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (750, ["Message", "tralala"]))

    def test_unknown_error_without_json(self):
        self.requests_response.expect.status_code.andReturn(750)
        self.requests_response.expect.json().andRaise(ValueError)
        self.requests_response.expect.text.andReturn("Message: tralala")

        with self.assertRaises(_exn.UnknownError) as catcher:
            self.responder(self.response_class.object, self.requests_response.object)
        self.assertEqual(catcher.exception.args, (750, "Message: tralala"))

    def test_known_client_errors(self):
        for type_name, cls in [
            ("xxx.AccessDeniedException", _exn.AccessDeniedException),
            ("xxx.ConditionalCheckFailedException", _exn.ConditionalCheckFailedException),
            ("xxx.IncompleteSignature", _exn.IncompleteSignature),
            ("xxx.InvalidAction", _exn.InvalidAction),
            ("xxx.InvalidClientTokenId", _exn.InvalidClientTokenId),
            ("xxx.InvalidParameterCombination", _exn.InvalidParameterCombination),
            ("xxx.InvalidParameterValue", _exn.InvalidParameterValue),
            ("xxx.InvalidQueryParameter", _exn.InvalidQueryParameter),
            ("xxx.InvalidSignatureException", _exn.InvalidSignatureException),
            ("xxx.ItemCollectionSizeLimitExceededException", _exn.ItemCollectionSizeLimitExceededException),
            ("xxx.LimitExceededException", _exn.LimitExceededException),
            ("xxx.MalformedQueryString", _exn.MalformedQueryString),
            ("xxx.MissingAction", _exn.MissingAction),
            ("xxx.MissingAuthenticationToken", _exn.MissingAuthenticationToken),
            ("xxx.MissingParameter", _exn.MissingParameter),
            ("xxx.OptInRequired", _exn.OptInRequired),
            ("xxx.ProvisionedThroughputExceededException", _exn.ProvisionedThroughputExceededException),
            ("xxx.RequestExpired", _exn.RequestExpired),
            ("xxx.ResourceInUseException", _exn.ResourceInUseException),
            ("xxx.ResourceNotFoundException", _exn.ResourceNotFoundException),
            ("xxx.Throttling", _exn.Throttling),
            ("xxx.ValidationError", _exn.ValidationError),
            ("xxx.ValidationException", _exn.ValidationException),
        ]:
            self.requests_response.expect.status_code.andReturn(400)
            self.requests_response.expect.json().andReturn({"__type": type_name, "Message": "tralala"})

            with self.assertRaises(cls) as catcher:
                self.responder(self.response_class.object, self.requests_response.object)

    def test_different_statuses(self):
        self.requests_response.expect.status_code.andReturn(400)
        self.requests_response.expect.json().andReturn({})
        with self.assertRaises(_exn.ClientError):
            self.responder(self.response_class.object, self.requests_response.object)
        self.requests_response.expect.status_code.andReturn(453)
        self.requests_response.expect.json().andReturn({})
        with self.assertRaises(_exn.ClientError):
            self.responder(self.response_class.object, self.requests_response.object)
        self.requests_response.expect.status_code.andReturn(499)
        self.requests_response.expect.json().andReturn({})
        with self.assertRaises(_exn.ClientError):
            self.responder(self.response_class.object, self.requests_response.object)

        self.requests_response.expect.status_code.andReturn(500)
        self.requests_response.expect.json().andReturn({})
        with self.assertRaises(_exn.ServerError):
            self.responder(self.response_class.object, self.requests_response.object)
        self.requests_response.expect.status_code.andReturn(547)
        self.requests_response.expect.json().andReturn({})
        with self.assertRaises(_exn.ServerError):
            self.responder(self.response_class.object, self.requests_response.object)
        self.requests_response.expect.status_code.andReturn(599)
        self.requests_response.expect.json().andReturn({})
        with self.assertRaises(_exn.ServerError):
            self.responder(self.response_class.object, self.requests_response.object)

        self.requests_response.expect.status_code.andReturn(600)
        self.requests_response.expect.json().andReturn({})
        with self.assertRaises(_exn.UnknownError):
            self.responder(self.response_class.object, self.requests_response.object)
        self.requests_response.expect.status_code.andReturn(612)
        self.requests_response.expect.json().andReturn({})
        with self.assertRaises(_exn.UnknownError):
            self.responder(self.response_class.object, self.requests_response.object)
        self.requests_response.expect.status_code.andReturn(9999)
        self.requests_response.expect.json().andReturn({})
        with self.assertRaises(_exn.UnknownError):
            self.responder(self.response_class.object, self.requests_response.object)
