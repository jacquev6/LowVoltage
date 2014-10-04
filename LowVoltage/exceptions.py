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
