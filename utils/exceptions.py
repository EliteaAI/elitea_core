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


class PoolSaturationError(Exception):
    "Raised when task pool is saturated and no workers are available"

    def __init__(self, pool: str, retry_after: int = 5):
        self.pool = pool
        self.retry_after = retry_after
        super().__init__(f"Pool '{pool}' saturated - no workers available")


class MaintenanceInProgressError(Exception):
    """Raised by task_node.start_task when maintenance mode is active.

    Callers that need entry-point-specific error shapes (SIO vs REST vs RPC)
    catch this and translate; unhandled propagation is intentional — during
    maintenance, task dispatch is not a supported operation, and a loud
    exception is preferable to silently returning None (which is
    indistinguishable from pool saturation).
    """

    def __init__(self, task_name: str = "?"):
        self.task_name = task_name
        super().__init__(f"Maintenance mode active - task '{task_name}' rejected")
