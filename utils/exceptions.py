class EntityInaccessableError(Exception):
    "Raised when entity in project for which user doesn't have permission"

    def __init__(self, message):
        self.message = message


class EntityDoesntExist(Exception):
    "Raised when entity doesn't exist"
    def __init__(self, message):
        self.message = message


class EntityAlreadyInCollectionError(Exception):
    "Raised when entity is already in collection"
    def __init__(self, message):
        self.message = message


class EntityNotInCollectionError(Exception):
    "Raised when entity is not in collection"
    def __init__(self, message):
        self.message = message


class EntityNotAvailableCollectionError(Exception):
    "Raised when entity is not available or registered in collection"
    def __init__(self, message):
        self.message = message


class VerifySignatureError(Exception):
    "Raised if checking of x-hub-signature-256 signature fails"

    def __init__(self, value):
        super().__init__(value)
        self.value = value


class NotFound(Exception):
    "Raised when nothing found by the query when it was required"
    def __def__(self, message):
        self.message = message
