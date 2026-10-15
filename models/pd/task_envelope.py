"""Versioned worker task envelope (C-1009, EliteaAI/elitea_issues#6940).

Dependency-free on purpose. Plugins are separate repos and cannot import each other, so
this file exists as a byte-identical copy in two places; change both together:
  - pylon_main/plugins/elitea_core/models/pd/task_envelope.py
  - pylon_indexer/plugins/indexer_worker/utils/task_envelope.py

The envelope carries NO secrets: only the contract version, the security profile the
task was dispatched under, and the worker capabilities it requires.
"""

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, ValidationError

SCHEMA_VERSION = 1
ENVELOPE_KEY = "envelope"
CAPABILITY_ENVELOPE_V1 = "task_envelope.v1"

PROFILE_LEGACY = "legacy"
PROFILE_GOVERNED = "governed"

UNKNOWN_SCHEMA_VERSION = "UNKNOWN_SCHEMA_VERSION"
MALFORMED_ENVELOPE = "MALFORMED_ENVELOPE"
CAPABILITY_MISSING = "CAPABILITY_MISSING"


class EnvelopeRejected(Exception):
    """A worker (or Main) refuses a task whose envelope it cannot honour."""

    def __init__(self, reason_code: str, missing: tuple = ()):
        super().__init__(reason_code)
        self.reason_code = reason_code
        self.missing = tuple(missing)


class TaskEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: int = SCHEMA_VERSION
    security_profile: Literal["legacy", "governed"] = PROFILE_LEGACY
    required_capabilities: frozenset[str] = frozenset()
    context_ref: Optional[str] = None


def attach_envelope(kwargs: dict, profile: str) -> dict:
    """Add the envelope for ``profile`` to a task payload; an existing one is kept as is.

    Keeping it means a re-dispatch (parallel child / reconcile) reuses the parent's
    envelope instead of re-deriving it from the current flag.
    """
    if ENVELOPE_KEY in kwargs:
        return kwargs
    if profile == PROFILE_GOVERNED:
        envelope = TaskEnvelope(
            security_profile=PROFILE_GOVERNED,
            required_capabilities=frozenset({CAPABILITY_ENVELOPE_V1}),
        )
    else:
        envelope = TaskEnvelope()
    dumped = envelope.model_dump(mode="json")
    dumped["required_capabilities"] = sorted(envelope.required_capabilities)
    kwargs[ENVELOPE_KEY] = dumped
    return kwargs


def read_envelope(kwargs: dict, supported_capabilities=None) -> TaskEnvelope:
    """Parse the envelope of a task payload.

    Missing or legacy: the legacy default, whatever else it carries (old behaviour).
    Anything else is validated strictly and refused with ``EnvelopeRejected``; when
    ``supported_capabilities`` is given a governed envelope must also be covered by it.
    """
    raw = kwargs.get(ENVELOPE_KEY)
    if raw is None:
        return TaskEnvelope()
    if not isinstance(raw, dict):
        raise EnvelopeRejected(MALFORMED_ENVELOPE)
    if raw.get("security_profile", PROFILE_LEGACY) == PROFILE_LEGACY:
        return TaskEnvelope()
    if raw.get("security_profile") == PROFILE_GOVERNED and raw.get("schema_version") != SCHEMA_VERSION:
        raise EnvelopeRejected(UNKNOWN_SCHEMA_VERSION)
    try:
        envelope = TaskEnvelope.model_validate(raw)
    except ValidationError:
        raise EnvelopeRejected(MALFORMED_ENVELOPE) from None
    if supported_capabilities is not None:
        missing = sorted(envelope.required_capabilities - frozenset(supported_capabilities))
        if missing:
            raise EnvelopeRejected(CAPABILITY_MISSING, missing)
    return envelope
