# coding: utf8

# Copyright 2014-2015 Vincent Jacques <vincent@vincent-jacques.net>

class Error(Exception): pass

class UnknownError(Error): pass
class ServerError(Error): pass
class NetworkError(Error): pass
class ClientError(Error): pass

class UnknownClientError(ClientError): pass
# All 4XXs from http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/CommonErrors.html
# and "Errors" sections of all actions (like http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#API_GetItem_Errors)
class ConditionalCheckFailedException(ClientError): pass
class IncompleteSignature(ClientError): pass
class InvalidAction(ClientError): pass
class InvalidAction(ClientError): pass
class InvalidClientTokenId(ClientError): pass
class InvalidParameterCombination(ClientError): pass
class InvalidParameterValue(ClientError): pass
class InvalidQueryParameter(ClientError): pass
class ItemCollectionSizeLimitExceededException(ClientError): pass
class LimitExceededException(ClientError): pass
class MalformedQueryString(ClientError): pass
class MissingAction(ClientError): pass
class MissingAuthenticationToken(ClientError): pass
class MissingParameter(ClientError): pass
class OptInRequired(ClientError): pass
class ProvisionedThroughputExceededException(ClientError): pass
class RequestExpired(ClientError): pass
class ResourceInUseException(ClientError): pass
class ResourceNotFoundException(ClientError): pass
class Throttling(ClientError): pass
class ValidationError(ClientError): pass
class ValidationException(ClientError): pass

# Sorted by decreasing suffix length to ensure BasicConnection._raise finds the right class using str.endswith.
client_errors = sorted(
    [
        ("ConditionalCheckFailedException", ConditionalCheckFailedException),
        ("IncompleteSignature", IncompleteSignature),
        ("InvalidAction", InvalidAction),
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
    ],
    key=lambda (prefix, cls): -len(prefix)
)
