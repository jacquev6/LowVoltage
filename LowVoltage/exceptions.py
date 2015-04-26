# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>


class Error(Exception):
    """
    The base class of all exceptions raised by the package.
    """
    retryable = False


class UnknownError(Error):
    """
    Exception raised for errors that can't be attributed to the client, the network or the server.

    You should ``except Error`` instead of this one.
    """


class ServerError(Error):
    """
    Exception raised when the problem can be blamed on the server.
    Typically DynamoDB returned a 5XX status code or a response that couldn't be decoded.
    """
    retryable = True


class NetworkError(Error):
    """
    Exception raised when the problem can be blamed on the network.
    Connection refused, timeout, etc.
    """
    retryable = True


class ClientError(Error):
    """
    Exception raised when the problem can be blamed on the client.

    See bellow for specialized exceptions for some client errors.
    """


class UnknownClientError(ClientError):
    """
    Exception raised for errors that can be attributed to the client but are not known by the package.
    Typically DynamoDB returned a 4XX status code but we couldn't match the returned type to any known client error.

    You should ``except ClientError`` instead of this one.

    Feel free to `open an issue <https://github.com/jacquev6/LowVoltage/issues/new?title=UnknownClientError>`__
    if your ``except ClientError`` clause catches an :exc:`UnknownClientError`.
    Put the exception details and we'll add it to the package.
    """


class BuilderError(ClientError):
    """
    Exception raised when you make a mistake while using a builder.

    For example if you call :meth:`.CreateTable.project` when you don't have an active index.
    Or :meth:`.BatchGetItem.keys` when you don't have an active table.
    """


# All 4XXs from http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html
# and "Errors" sections of all actions (like http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_Errors)


class ConditionalCheckFailedException(ClientError):
    """
    Raised by conditional updates when the condition is not met.
    See :meth:`.PutItem.condition_expression`, :meth:`.UpdateItem.condition_expression` and :meth:`.DeleteItem.condition_expression`.
    """


class IncompleteSignature(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class InvalidAction(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class InvalidClientTokenId(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class InvalidParameterCombination(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class InvalidParameterValue(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class InvalidQueryParameter(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class ItemCollectionSizeLimitExceededException(ClientError):
    """
    Exception raised when the item collection of a hash key is too large (in a table with a LSI).
    """


class LimitExceededException(ClientError):
    """
    Exception raised when too many tables are beeing modified at the same time.
    Or when there are too many tables.

    See `the reference of the CreateTable action <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_Errors>`__.
    """


class MalformedQueryString(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class MissingAction(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class MissingAuthenticationToken(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class MissingParameter(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class OptInRequired(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class ProvisionedThroughputExceededException(ClientError):
    """
    Exception raised when the provisioned throughput is reached.
    """
    retryable = True


class RequestExpired(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """
    retryable = True


class ResourceInUseException(ClientError):
    """
    Exception raise when trying to modify a table that's not in the "ACTIVE" state.
    """
    retryable = True


class ResourceNotFoundException(ClientError):
    """
    Exception raised when trying to use a non-existent table.
    """


class Throttling(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """
    retryable = True


class ValidationError(ClientError):
    """
    See `common errors <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    """


class ValidationException(ClientError):
    """
    Exception raised when the request is invalid.
    """


# Error discovered by chance, not documented


class AccessDeniedException(ClientError):
    """
    Exception `not documented <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    Seems to be raised when credentials are valid, but the operation is not allowed by IAM policies.
    """


class InvalidSignatureException(ClientError):
    """
    Exception `not documented <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    Seems to be raised when credentials are not valid.
    """


class SerializationException(ClientError):
    """
    Exception `not documented <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    Seems to be raised when a number argument (passed as a string) cannot be converted to an actual number.
    """


class UnrecognizedClientException(ClientError):
    """
    Exception `not documented <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    Seems to be raised when using (valid) temporary credentials but an invalid token.
    """


# Sorted by decreasing suffix length to ensure BasicConnection._raise finds the right class using str.endswith.
client_errors = sorted(
    [
        ("ConditionalCheckFailedException", ConditionalCheckFailedException),
        ("IncompleteSignature", IncompleteSignature),
        ("InvalidAction", InvalidAction),
        ("InvalidClientTokenId", InvalidClientTokenId),
        ("InvalidParameterCombination", InvalidParameterCombination),
        ("InvalidParameterValue", InvalidParameterValue),
        ("InvalidQueryParameter", InvalidQueryParameter),
        ("ItemCollectionSizeLimitExceededException", ItemCollectionSizeLimitExceededException),
        ("LimitExceededException", LimitExceededException),
        ("MalformedQueryString", MalformedQueryString),
        ("MissingAction", MissingAction),
        ("MissingAuthenticationToken", MissingAuthenticationToken),
        ("MissingParameter", MissingParameter),
        ("OptInRequired", OptInRequired),
        ("ProvisionedThroughputExceededException", ProvisionedThroughputExceededException),
        ("RequestExpired", RequestExpired),
        ("ResourceInUseException", ResourceInUseException),
        ("ResourceNotFoundException", ResourceNotFoundException),
        ("Throttling", Throttling),
        ("ValidationError", ValidationError),
        ("ValidationException", ValidationException),

        ("AccessDeniedException", AccessDeniedException),
        ("InvalidSignatureException", InvalidSignatureException),
        ("SerializationException", SerializationException),
        ("UnrecognizedClientException", UnrecognizedClientException),
    ],
    key=lambda (prefix, cls): -len(prefix)
)
