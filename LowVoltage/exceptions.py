# -*- coding: utf-8 -*-

# Copyright 2013-2014 Vincent Jacques <vincent@vincent-jacques.net>

class Error(Exception):
    pass


class UnknownError(Error):
    pass


class ServerError(Error):
    pass


class NetworkError(Error):
    pass


class ClientError(Error):
    pass


class ResourceNotFoundException(ClientError):
    pass


class ValidationException(ClientError):
    pass


class ConditionalCheckFailedException(ClientError):
    pass


class ItemCollectionSizeLimitExceededException(ClientError):
    pass


class ProvisionedThroughputExceededException(ClientError):
    pass


class LimitExceededException(ClientError):
    pass


class ResourceInUseException(ClientError):
    pass
