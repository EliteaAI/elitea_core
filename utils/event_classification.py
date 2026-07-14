#!/usr/bin/python3
# coding=utf-8

#   Copyright 2025 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Event classification registry for horizontal scaling.

Classifies all Redis pub/sub events into three categories:
- broadcast: all pods must process (cache invalidation, config sync)
- work: exactly one pod must process (task execution, DB writes)
- notification: best-effort delivery, loss acceptable (UI updates)

Used by Phase 4 (Redis Streams migration) to determine which events
should move from pub/sub broadcast to Stream-based work queues.
"""

import enum
from typing import Dict, Optional, Tuple


class EventType(enum.Enum):
    BROADCAST = "broadcast"
    WORK = "work"
    NOTIFICATION = "notification"


class StreamRetention:
    WORK = 10000
    NOTIFICATION = 1000
    DLQ = 50000


_REGISTRY: Dict[str, Tuple[EventType, str]] = {}
_STREAM_RETENTION: Dict[str, int] = {}


def register_event(event_name: str, event_type: EventType, description: str = "") -> None:
    _REGISTRY[event_name] = (event_type, description)


def get_event_type(event_name: str) -> Optional[EventType]:
    entry = _REGISTRY.get(event_name)
    return entry[0] if entry else None


def get_event_description(event_name: str) -> str:
    entry = _REGISTRY.get(event_name)
    return entry[1] if entry else ""


def get_events_by_type(event_type: EventType) -> Dict[str, str]:
    return {
        name: desc
        for name, (etype, desc) in _REGISTRY.items()
        if etype == event_type
    }


def get_work_events() -> Dict[str, str]:
    return get_events_by_type(EventType.WORK)


def get_broadcast_events() -> Dict[str, str]:
    return get_events_by_type(EventType.BROADCAST)


def get_notification_events() -> Dict[str, str]:
    return get_events_by_type(EventType.NOTIFICATION)


def get_retention(event_name: str) -> int:
    """Get MAXLEN retention for a registered event name."""
    event_type = get_event_type(event_name)
    if event_type == EventType.WORK:
        return StreamRetention.WORK
    elif event_type == EventType.NOTIFICATION:
        return StreamRetention.NOTIFICATION
    return StreamRetention.WORK


def register_stream_retention(stream_name: str, maxlen: int) -> None:
    """Register a custom MAXLEN retention for a specific stream name.

    Overrides the default classification-based retention for this stream.
    """
    _STREAM_RETENTION[stream_name] = maxlen


def get_stream_retention(stream_name: str) -> int:
    """Get MAXLEN retention for a stream name.

    Priority:
    1. Explicit per-stream registration (register_stream_retention)
    2. DLQ prefix detection → StreamRetention.DLQ
    3. Stream-name-to-event lookup (work:* → WORK, notify:* → NOTIFICATION)
    4. Default: StreamRetention.WORK
    """
    if stream_name in _STREAM_RETENTION:
        return _STREAM_RETENTION[stream_name]

    if stream_name.startswith("dlq:"):
        return StreamRetention.DLQ

    if stream_name.startswith("work:"):
        return StreamRetention.WORK

    if stream_name.startswith("notify:") or stream_name.startswith("notification:"):
        return StreamRetention.NOTIFICATION

    return StreamRetention.WORK


def list_stream_retentions() -> Dict[str, int]:
    """Return all explicitly registered stream retention configs."""
    return dict(_STREAM_RETENTION)


def is_registered(event_name: str) -> bool:
    return event_name in _REGISTRY


def list_all() -> Dict[str, Tuple[EventType, str]]:
    return dict(_REGISTRY)


def clear_registry() -> None:
    _REGISTRY.clear()
    _STREAM_RETENTION.clear()


# --- Built-in event registrations ---

# pylon_main — elitea_core: Application stream events
register_event(
    "application_stream_response",
    EventType.NOTIFICATION,
    "Agent streaming tokens to SIO room (filtered by stream_id ownership)"
)
register_event(
    "application_full_response",
    EventType.WORK,
    "Agent execution complete — triggers DB save of conversation message"
)
register_event(
    "application_partial_response",
    EventType.WORK,
    "Partial response checkpoint — triggers DB partial save"
)
register_event(
    "application_child_message",
    EventType.WORK,
    "Child message from agent — triggers DB save"
)

# pylon_main — elitea_core: Voice TTS/ASR events
register_event(
    "voice_tts_audio_chunk",
    EventType.NOTIFICATION,
    "TTS audio chunk routed to specific SIO client"
)
register_event(
    "voice_tts_done",
    EventType.NOTIFICATION,
    "TTS generation complete for specific SIO client"
)
register_event(
    "voice_tts_error",
    EventType.NOTIFICATION,
    "TTS error routed to specific SIO client"
)
register_event(
    "voice_asr_transcript_delta",
    EventType.NOTIFICATION,
    "ASR realtime partial transcript to specific SIO client"
)
register_event(
    "voice_asr_transcript_done",
    EventType.WORK,
    "ASR transcript complete — modifies ASR session state"
)
register_event(
    "voice_asr_error",
    EventType.NOTIFICATION,
    "ASR error routed to specific SIO client"
)
register_event(
    "voice_asr_speech_started",
    EventType.NOTIFICATION,
    "ASR speech detection started for specific SIO client"
)
register_event(
    "voice_asr_vad_flush",
    EventType.NOTIFICATION,
    "ASR VAD flush notification to specific SIO client"
)

# pylon_main — elitea_core: Config collection events (startup sync)
register_event(
    "application_toolkit_configurations_collected",
    EventType.BROADCAST,
    "Toolkit configurations from indexer — all pods update their config cache"
)
register_event(
    "application_toolkits_collected",
    EventType.BROADCAST,
    "Toolkit schemas from indexer — all pods update schema registry"
)
register_event(
    "application_file_loaders_collected",
    EventType.BROADCAST,
    "File loader types from indexer — all pods update index_types"
)
register_event(
    "application_mcp_prebuilt_config_collected",
    EventType.BROADCAST,
    "MCP prebuilt configs from indexer — all pods update config cache"
)

# pylon_main — elitea_core: Task lifecycle
register_event(
    "task_status_change",
    EventType.WORK,
    "Task completed/failed — pops callback and invokes webhook (exactly-once via GETDEL)"
)

# pylon_main — worker_client
register_event(
    "stream_event",
    EventType.NOTIFICATION,
    "LLM stream event — filtered by stream_id ownership on receiving pod"
)
register_event(
    "bootstrap_runtime_info",
    EventType.BROADCAST,
    "Runtime info update from indexer — all pods update local cache"
)
register_event(
    "bootstrap_runtime_info_prune",
    EventType.BROADCAST,
    "Prune stale runtime info entries — all pods clean their cache"
)
register_event(
    "runtime_engine_ready",
    EventType.BROADCAST,
    "LiteLLM runtime engine ready — all pods set readiness Event"
)

# pylon_main — logging_hub
register_event(
    "log_data",
    EventType.NOTIFICATION,
    "Task log data for SIO room delivery — best-effort (duplicate = cosmetic issue)"
)

# pylon_main — tracing (separate channel: audit_trail)
register_event(
    "audit_event",
    EventType.BROADCAST,
    "Audit span from indexer — all subscriber pods write to audit store"
)

# pylon_indexer — worker_core
register_event(
    "bootstrap_runtime_update",
    EventType.BROADCAST,
    "Runtime update from pylon_main — all indexer pods update local runtime info"
)

# pylon_indexer — indexer_worker: Request/response pattern
register_event(
    "application_toolkits_request",
    EventType.BROADCAST,
    "Request for toolkits — all indexer pods respond (wasteful but harmless)"
)
register_event(
    "application_file_loaders_request",
    EventType.BROADCAST,
    "Request for file loaders — all indexer pods respond"
)
register_event(
    "application_toolkit_configurations_request",
    EventType.BROADCAST,
    "Request for toolkit configurations — all indexer pods respond"
)
register_event(
    "application_mcp_prebuilt_config_request",
    EventType.BROADCAST,
    "Request for MCP prebuilt configs — all indexer pods respond"
)

# pylon_indexer — indexer_worker: State mutation (should be work events)
register_event(
    "indexer_empty_agent_state",
    EventType.WORK,
    "Delete agent checkpoints — redundant deletes safe but wasteful"
)
register_event(
    "indexer_delete_checkpoint",
    EventType.WORK,
    "Delete specific checkpoint — redundant deletes safe but wasteful"
)

# pylon_indexer — voice_router
register_event(
    "voice_events",
    EventType.WORK,
    "Voice audio/control routed by (sid, event_type) — only pod with handler processes"
)

# pylon_indexer — provider_worker
register_event(
    "provider_invocation_started",
    EventType.NOTIFICATION,
    "Provider invocation tracking — same-pod event, local dict update"
)
register_event(
    "provider_invocation_ended",
    EventType.NOTIFICATION,
    "Provider invocation ended — same-pod event, local dict cleanup"
)
register_event(
    "task_stop_request",
    EventType.WORK,
    "Cancel task invocations — only pod running the task cancels"
)

# Arbiter internal — TaskNode
register_event(
    "task_result_payload",
    EventType.WORK,
    "Task result delivery — matched by task_id, only requesting node processes"
)
register_event(
    "task_node_announce",
    EventType.BROADCAST,
    "TaskNode startup announcement — all nodes update peer list"
)
register_event(
    "task_node_withhold",
    EventType.BROADCAST,
    "TaskNode shutdown notice — all nodes remove from peer list"
)
register_event(
    "task_start_query",
    EventType.WORK,
    "Task placement query — nodes respond with capacity info"
)
register_event(
    "task_start_candidate",
    EventType.WORK,
    "Task candidate response — matched by task_id to requesting node"
)
register_event(
    "task_start_request",
    EventType.WORK,
    "Task assignment — only matching ident processes"
)
register_event(
    "task_start_ack",
    EventType.WORK,
    "Task start acknowledgment — matched by task_id"
)
register_event(
    "task_state_announce",
    EventType.BROADCAST,
    "Task state broadcast — all nodes update local state cache"
)
register_event(
    "task_state_query",
    EventType.BROADCAST,
    "Task state query — all nodes respond with local state"
)
register_event(
    "task_state_reply",
    EventType.WORK,
    "Task state response — matched by requestor"
)
register_event(
    "task_pool_query",
    EventType.BROADCAST,
    "Task pool query — all nodes respond"
)
register_event(
    "task_pool_reply",
    EventType.BROADCAST,
    "Task pool reply — all nodes update pool info"
)

# Arbiter internal — PresenceNode, ServiceNode, StreamNode
register_event(
    "presence_join",
    EventType.BROADCAST,
    "Presence join announcement — all nodes update presence map"
)
register_event(
    "presence_leave",
    EventType.BROADCAST,
    "Presence leave announcement — all nodes remove from presence map"
)
register_event(
    "service_discovery",
    EventType.BROADCAST,
    "Service discovery — all nodes respond with services"
)
register_event(
    "service_provider",
    EventType.BROADCAST,
    "Service provider registration — all nodes register provider"
)
register_event(
    "service_request",
    EventType.WORK,
    "Service invocation — only matching provider processes"
)
register_event(
    "service_response",
    EventType.WORK,
    "Service response — matched by request_id to caller"
)


# --- Built-in stream retention registrations ---
# Explicit overrides for known streams. Streams not listed here fall back to
# prefix-based detection in get_stream_retention().

register_stream_retention("work:task_distribution", StreamRetention.WORK)
register_stream_retention("work:voice_events", StreamRetention.WORK)
register_stream_retention("work:service_request", StreamRetention.WORK)
register_stream_retention("notify:stream_event", StreamRetention.NOTIFICATION)
register_stream_retention("notify:log_data", StreamRetention.NOTIFICATION)
register_stream_retention("notify:voice_tts_audio_chunk", StreamRetention.NOTIFICATION)
register_stream_retention("dlq:work:task_distribution", StreamRetention.DLQ)
register_stream_retention("dlq:work:voice_events", StreamRetention.DLQ)
register_stream_retention("dlq:work:service_request", StreamRetention.DLQ)
