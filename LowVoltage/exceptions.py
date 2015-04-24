# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

"""
@todo Link to user guide (error management).
"""


class Error(Exception):
    """
    @todo Document
    """
    retryable = False


class UnknownError(Error):
    """
    @todo Document
    """
    pass


class ServerError(Error):
    """
    @todo Document
    """
    retryable = True


class NetworkError(Error):
    """
    @todo Document
    """
    retryable = True


class ClientError(Error):
    """
    @todo Document
    """
    pass


class UnknownClientError(ClientError):
    """
    @todo Document
    """
    pass


# All 4XXs from http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html
# and "Errors" sections of all actions (like http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_Errors)


class ConditionalCheckFailedException(ClientError):
    """
    @todo Document
    """
    pass


class IncompleteSignature(ClientError):
    """
    @todo Document
    """
    pass


class InvalidAction(ClientError):
    """
    @todo Document
    """
    pass


class InvalidClientTokenId(ClientError):
    """
    @todo Document
    """
    pass


class InvalidParameterCombination(ClientError):
    """
    @todo Document
    """
    pass


class InvalidParameterValue(ClientError):
    """
    @todo Document
    """
    pass


class InvalidQueryParameter(ClientError):
    """
    @todo Document
    """
    pass


class ItemCollectionSizeLimitExceededException(ClientError):
    """
    @todo Document
    """
    pass


class LimitExceededException(ClientError):
    """
    @todo Document
    """
    pass


class MalformedQueryString(ClientError):
    """
    @todo Document
    """
    pass


class MissingAction(ClientError):
    """
    @todo Document
    """
    pass


class MissingAuthenticationToken(ClientError):
    """
    @todo Document
    """
    pass


class MissingParameter(ClientError):
    """
    @todo Document
    """
    pass


class OptInRequired(ClientError):
    """
    @todo Document
    """
    pass


class ProvisionedThroughputExceededException(ClientError):
    """
    @todo Document
    """
    retryable = True


class RequestExpired(ClientError):
    """
    @todo Document
    """
    retryable = True


class ResourceInUseException(ClientError):
    """
    @todo Document
    """
    retryable = True


class ResourceNotFoundException(ClientError):
    """
    @todo Document
    """
    pass


class Throttling(ClientError):
    """
    @todo Document
    """
    retryable = True


class ValidationError(ClientError):
    """
    @todo Document
    """
    pass


class ValidationException(ClientError):
    """
    @todo Document
    """
    pass


# Error discovered by chance, not documented


class AccessDeniedException(ClientError):
    """
    Exception `not documented <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    Seems to be raised when credentials are valid, but the operation is not allowed by IAM policies.
    """
    pass


class InvalidSignatureException(ClientError):
    """
    Exception `not documented <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    Seems to be raised when credentials are not valid.
    """
    pass


class UnrecognizedClientException(ClientError):
    """
    Exception `not documented <http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html>`__.
    Seems to be raised when using (valid) temporary credentials but an invalid token.
    """
    pass


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
        ("UnrecognizedClientException", UnrecognizedClientException),
    ],
    key=lambda (prefix, cls): -len(prefix)
)
