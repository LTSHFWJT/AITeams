class AIMemoryError(Exception):
    pass


class RecordNotFound(AIMemoryError):
    pass


class InvalidScope(AIMemoryError):
    pass


class StorageError(AIMemoryError):
    pass
