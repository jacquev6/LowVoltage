class Error(Exception):
    pass


class UnknownError(Error):
    pass


class ServerError(Error):
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
