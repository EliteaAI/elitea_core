"""Event schema registry for payload validation.

Provides Pydantic-based schema validation for Redis Streams events.
Each event type can have a registered schema that validates:
- On publish: raises ValidationError to prevent invalid data entering streams
- On consume: logs warning and routes to DLQ, never crashes the consumer

The registry is decoupled from the event_classification registry — an event
can be classified without a schema (backward-compatible), and schema
validation is opt-in.

Usage:
    from elitea_core.events.schema_registry import (
        register_event_schema, validate_publish, validate_consume,
    )
    from pydantic import BaseModel

    class TaskDistributionEvent(BaseModel):
        task_id: str
        task_name: str
        pool: str = "default"
        args: list = []
        kwargs: dict = {}

    register_event_schema("work:task_distribution", TaskDistributionEvent)

    # On publish side — raises on invalid
    validate_publish("work:task_distribution", {"task_id": "abc", "task_name": "run"})

    # On consume side — returns (True, data) or (False, error_info)
    valid, result = validate_consume("work:task_distribution", raw_data)
"""

from typing import Any, Dict, List, Optional, Tuple, Type

from pydantic import BaseModel, ValidationError

from pylon.core.tools import log


_SCHEMA_REGISTRY: Dict[str, Type[BaseModel]] = {}


class SchemaValidationError(Exception):
    """Raised when event payload fails schema validation on publish."""

    def __init__(self, event_name: str, errors: List[Dict[str, Any]]):
        self.event_name = event_name
        self.errors = errors
        details = "; ".join(
            f"{e.get('loc', '?')}: {e.get('msg', 'unknown')}" for e in errors
        )
        super().__init__(
            f"Schema validation failed for event '{event_name}': {details}"
        )


def register_event_schema(event_name: str, schema_class: Type[BaseModel]) -> None:
    """Register a Pydantic model as the schema for an event type.

    Args:
        event_name: The event/stream name (e.g. "work:task_distribution").
        schema_class: A Pydantic BaseModel subclass defining the payload shape.
    """
    if not (isinstance(schema_class, type) and issubclass(schema_class, BaseModel)):
        raise TypeError(
            f"schema_class must be a Pydantic BaseModel subclass, got {type(schema_class)}"
        )
    _SCHEMA_REGISTRY[event_name] = schema_class
    log.info("Registered event schema: %s -> %s", event_name, schema_class.__name__)


def unregister_event_schema(event_name: str) -> bool:
    """Remove a schema registration. Returns True if it existed."""
    return _SCHEMA_REGISTRY.pop(event_name, None) is not None


def get_event_schema(event_name: str) -> Optional[Type[BaseModel]]:
    """Get the registered schema for an event, or None if unregistered."""
    return _SCHEMA_REGISTRY.get(event_name)


def is_registered(event_name: str) -> bool:
    """Check if an event has a registered schema."""
    return event_name in _SCHEMA_REGISTRY


def list_registered() -> Dict[str, str]:
    """List all registered schemas as {event_name: schema_class_name}."""
    return {name: cls.__name__ for name, cls in _SCHEMA_REGISTRY.items()}


def clear_registry() -> None:
    """Remove all schema registrations (for testing)."""
    _SCHEMA_REGISTRY.clear()


def validate_publish(event_name: str, payload: dict) -> dict:
    """Validate payload before publishing to a stream.

    If the event has a registered schema, validates the payload against it.
    If no schema is registered, passes through without validation (backward-compatible).

    Args:
        event_name: The event/stream name.
        payload: The event data dict to validate.

    Returns:
        The validated (and potentially coerced) payload as a dict.

    Raises:
        SchemaValidationError: If the payload is invalid.
    """
    schema_class = _SCHEMA_REGISTRY.get(event_name)
    if schema_class is None:
        return payload

    try:
        validated = schema_class.model_validate(payload)
        return validated.model_dump()
    except ValidationError as e:
        errors = e.errors()
        raise SchemaValidationError(event_name, errors) from e


def validate_consume(event_name: str, payload: dict) -> Tuple[bool, Any]:
    """Validate payload when consuming from a stream.

    Unlike validate_publish, this NEVER raises — it returns a result tuple.
    Invalid payloads should be routed to the DLQ by the caller.

    If no schema is registered, always returns (True, payload).

    Args:
        event_name: The event/stream name.
        payload: The raw event data dict.

    Returns:
        Tuple of (is_valid, result):
        - (True, validated_dict) on success
        - (False, error_info_dict) on failure
    """
    schema_class = _SCHEMA_REGISTRY.get(event_name)
    if schema_class is None:
        return True, payload

    try:
        validated = schema_class.model_validate(payload)
        return True, validated.model_dump()
    except ValidationError as e:
        errors = e.errors()
        error_info = {
            "event_name": event_name,
            "schema": schema_class.__name__,
            "errors": errors,
            "payload_keys": list(payload.keys()) if isinstance(payload, dict) else str(type(payload)),
        }
        log.warning(
            "Schema validation failed on consume for '%s': %s",
            event_name, errors
        )
        return False, error_info


# --------------------------------------------------------------------------
# Built-in event schemas
# --------------------------------------------------------------------------

class TaskDistributionEvent(BaseModel):
    """Schema for work:task_distribution stream events."""
    task_id: str
    task_name: str
    pool: str = "default"
    args: list = []
    kwargs: dict = {}
    meta: dict = {}
    submitted_at: float = 0.0


class TaskStatusChangeEvent(BaseModel):
    """Schema for task_status_change events."""
    task_id: str
    status: str
    result: Any = None
    error: Optional[str] = None
    timestamp: float = 0.0


class ApplicationFullResponseEvent(BaseModel):
    """Schema for application_full_response events."""
    stream_id: str
    project_id: int
    conversation_id: str
    response: Any = None
    meta: dict = {}


class ApplicationStreamResponseEvent(BaseModel):
    """Schema for application_stream_response events."""
    stream_id: str
    project_id: int
    chunk: str = ""
    done: bool = False


class VoiceTTSAudioChunkEvent(BaseModel):
    """Schema for voice_tts_audio_chunk events."""
    sid: str
    chunk_index: int = 0
    audio_data: str = ""
    format: str = "pcm"


class VoiceTTSDoneEvent(BaseModel):
    """Schema for voice_tts_done events."""
    sid: str
    total_chunks: int = 0


class VoiceASRTranscriptDoneEvent(BaseModel):
    """Schema for voice_asr_transcript_done events."""
    sid: str
    transcript: str
    is_final: bool = True
    confidence: float = 0.0


class CacheInvalidationEvent(BaseModel):
    """Schema for cache invalidation broadcast events."""
    cache_type: str
    key: Optional[str] = None
    invalidate_all: bool = False


class BootstrapRuntimeInfoEvent(BaseModel):
    """Schema for bootstrap_runtime_info events."""
    runtime_id: str
    info: dict = {}
    action: str = "update"


class LeaderElectionEvent(BaseModel):
    """Schema for leader election announcements."""
    service_name: str
    leader_id: str
    timestamp: float = 0.0


# --------------------------------------------------------------------------
# Register built-in schemas
# --------------------------------------------------------------------------

def _register_builtins() -> None:
    """Register built-in event schemas. Called at module load."""
    register_event_schema("work:task_distribution", TaskDistributionEvent)
    register_event_schema("task_status_change", TaskStatusChangeEvent)
    register_event_schema("application_full_response", ApplicationFullResponseEvent)
    register_event_schema("application_stream_response", ApplicationStreamResponseEvent)
    register_event_schema("voice_tts_audio_chunk", VoiceTTSAudioChunkEvent)
    register_event_schema("voice_tts_done", VoiceTTSDoneEvent)
    register_event_schema("voice_asr_transcript_done", VoiceASRTranscriptDoneEvent)
    register_event_schema("bootstrap_runtime_info", BootstrapRuntimeInfoEvent)


_register_builtins()
