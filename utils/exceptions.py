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


# Twinned with usage/hooks.py's BUDGET_ERROR_MESSAGE/BUDGET_ERROR_CODES — edit both together;
# the SDK and the UI match on this body, never on the status code
BUDGET_ERROR_MESSAGE = (
    "The budget for shared models has been reached. Requests are unavailable "
    "until the budget resets or an administrator raises the limit."
)

BUDGET_ERROR_CODES = {
    "project": "project_budget_exceeded",
    "member": "member_budget_exceeded",
}


class BudgetDoorClosedError(Exception):
    """Raised by task_node.start_task when the project's budget is already exhausted.

    Carries the same type/code/message triple the inference-plane refusal uses, so the SDK
    and the UI recognise it as a budget refusal rather than a generic dispatch failure.
    """

    def __init__(self, scope: str = "project", project_id=None):
        self.scope = scope
        self.project_id = project_id
        self.type = "budget_exceeded"
        self.code = BUDGET_ERROR_CODES.get(scope, BUDGET_ERROR_CODES["project"])
        self.message = BUDGET_ERROR_MESSAGE
        super().__init__(BUDGET_ERROR_MESSAGE)

    def body(self):
        """The wire body the inference-plane refusal uses, byte-identical."""
        return {"error": {"message": self.message, "type": self.type, "code": self.code}}


INPUT_GUARDRAIL_BLOCKED_TYPE = "input_guardrail_blocked"
_INPUT_GUARDRAIL_CATEGORY_LABELS = {
    "credentials": "a credential",
    "personal_data": "personal data",
}


class InputGuardrailBlockedError(Exception):
    """Raised when the input guardrail (#6886) blocks a turn before it reaches a model.

    Carries rule ids and segment kinds only; the detected value never reaches it. The
    message is twinned with indexer_worker's build_input_guardrail_error.
    """

    def __init__(self, categories=None, rules=None, segments=None):
        self.categories = [str(c) for c in categories or ["error"]]
        self.rules = [str(r) for r in rules or []]
        self.segments = [str(s) for s in segments or []]
        self.type = INPUT_GUARDRAIL_BLOCKED_TYPE
        self.code = INPUT_GUARDRAIL_BLOCKED_TYPE
        if self.categories != ["error"]:
            labels = [_INPUT_GUARDRAIL_CATEGORY_LABELS.get(c, c) for c in self.categories]
            self.message = (
                f"Your message appears to contain {' and '.join(labels)}. It was not sent to the model. "
                "Remove it, or store it as a project secret and reference it instead."
            )
            self.message_key = f"input_guardrail.blocked.{self.categories[0]}"
        else:
            # fail_closed on a detector error: nothing was detected, the check could not run
            self.message = "Your message could not be checked by the input guardrail, so it was not sent to the model."
            self.message_key = "input_guardrail.blocked.error"
        super().__init__(self.message)

    def verdict(self):
        """The ``input_guardrail`` block of the SIO event and the stored message meta."""
        return {
            "action": "block",
            "categories": self.categories,
            "rules": self.rules,
            "segments": self.segments,
            "message_key": self.message_key,
        }

    def body(self):
        """The REST 422 body."""
        return {
            "error": self.type,
            "categories": self.categories,
            "rules": self.rules,
            "message": self.message,
            "message_key": self.message_key,
        }
