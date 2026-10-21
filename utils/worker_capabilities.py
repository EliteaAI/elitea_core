"""Worker capability registry and governed dispatch gate (C-1009, #6940).

Workers advertise their capabilities on ``task_node_capabilities`` (arbiter's task-node
announce payload cannot carry them). Main keeps what it last heard and refuses to
dispatch a governed task that no live worker can honour.
"""

import threading
import time

from pylon.core.tools import log

from ..models.pd.task_envelope import (
    PROFILE_GOVERNED, attach_envelope, read_envelope,
)
from .exceptions import WorkerCapabilityUnavailable
from .security_metrics_client import SecurityCounter

CAPABILITIES_EVENT = "task_node_capabilities"
CAPABILITIES_REQUEST_EVENT = "task_node_capabilities_request"
WITHHOLD_EVENT = "task_node_withhold"
DEFAULT_CAPABILITY_TTL_S = 120

# Tasks whose payload carries an envelope. Every one of them accepts **kwargs on the worker
# side, so an older worker ignores the extra key; other tasks are left untouched.
ENVELOPED_TASKS = frozenset({
    "indexer_agent",
    "indexer_predict_agent",
    "indexer_test_toolkit_tool",
    "indexer_configuration_check_connection",
    "indexer_test_mcp_connection",
    "indexer_mcp_sync_tools",
    "indexer_governed_check_connection",   # the governed_ops pool: probes of a saved credential (#6946)
})

# What a worker must be able to do beyond the base envelope to run a governed task: resolve credential slots (a
# governed payload carries slots, not material, #6946), and for the probe also be the governed_ops pool. Added here,
# not in ``attach_envelope`` (its file is a byte-identical copy in the worker), and to an envelope already in the
# payload too: a re-dispatched parent that predates this requirement is held to it as well.
CAPABILITY_CREDENTIAL_SLOTS_V1 = "credential_slots.v1"
CAPABILITY_GOVERNED_OPS_POOL_V1 = "governed_ops_pool.v1"
GOVERNED_TASK_CAPABILITIES = {
    "indexer_agent": (CAPABILITY_CREDENTIAL_SLOTS_V1,),
    "indexer_predict_agent": (CAPABILITY_CREDENTIAL_SLOTS_V1,),
    "indexer_governed_check_connection": (CAPABILITY_CREDENTIAL_SLOTS_V1, CAPABILITY_GOVERNED_OPS_POOL_V1),
}
# A worker that announces this asks for all the slots of a run in one release (``restricted_release_credentials``). It
# is preferred and not required: a worker without it releases slot by slot (``restricted_release_credential``), which
# Main still serves, so a rolling upgrade never leaves a governed task without a worker.
CAPABILITY_CREDENTIAL_SLOTS_BATCH_V1 = "credential_slots_batch.v1"
GOVERNED_TASK_PREFERRED_CAPABILITIES = {
    "indexer_agent": (CAPABILITY_CREDENTIAL_SLOTS_BATCH_V1,),
    "indexer_predict_agent": (CAPABILITY_CREDENTIAL_SLOTS_BATCH_V1,),
}


GOVERNED_DISPATCH_UNAVAILABLE = SecurityCounter(
    "governed_dispatch_unavailable_total",
    "Governed tasks refused at dispatch because no live worker supports them",
)
GOVERNED_DISPATCH_WITHOUT_BATCH = _Counter(
    "governed_dispatch_without_slot_batch_total",
    "Governed tasks dispatched while no live worker announces the batch release of credential slots",
)


class WorkerCapabilityRegistry:
    """``{ident: (capabilities, last_seen)}`` with TTL; entries use the local receive time."""

    def __init__(self, ttl_s=DEFAULT_CAPABILITY_TTL_S, clock=time.monotonic):
        self.ttl_s = ttl_s
        self._clock = clock
        self._lock = threading.Lock()
        self._workers = {}

    def on_capabilities(self, event_name, event_payload):
        _ = event_name
        ident = event_payload.get("ident") if isinstance(event_payload, dict) else None
        capabilities = event_payload.get("capabilities") if isinstance(event_payload, dict) else None
        if not isinstance(ident, str) or not isinstance(capabilities, (list, tuple, set, frozenset)):
            return
        with self._lock:
            self._workers[ident] = (frozenset(str(c) for c in capabilities), self._clock())

    def on_withhold(self, event_name, event_payload):
        _ = event_name
        with self._lock:
            self._workers.pop(event_payload.get("ident"), None)

    def has_capable_worker(self, required) -> bool:
        required = frozenset(required)
        deadline = self._clock() - self.ttl_s
        with self._lock:
            for ident in [i for i, (_, seen) in self._workers.items() if seen < deadline]:
                del self._workers[ident]
            return any(required <= caps for caps, _ in self._workers.values())


def apply_envelope_gate(task_name, call_kwargs, profile, registry):
    """Attach the envelope to a ``start_task`` call; refuse a governed task nobody can run.

    ``profile`` is the current ``GovernanceProfile``. An envelope already in the payload
    (re-dispatch of a parent) is reused and gates on its own requirements, never on the flag.
    """
    if task_name not in ENVELOPED_TASKS:
        return
    payload = call_kwargs.get("kwargs")
    if payload is None:
        payload = call_kwargs["kwargs"] = {}
    attach_envelope(payload, profile.profile)
    envelope = read_envelope(payload)
    if envelope.security_profile != PROFILE_GOVERNED:
        return
    required = envelope.required_capabilities | frozenset(GOVERNED_TASK_CAPABILITIES.get(task_name, ()))
    if required != envelope.required_capabilities:
        payload["envelope"]["required_capabilities"] = sorted(required)
    if not registry.has_capable_worker(required):
        GOVERNED_DISPATCH_UNAVAILABLE.inc()
        log.warning(
            "task_node.start_task: refused governed task %s - no live worker supports %s",
            task_name, sorted(required),
        )
        raise WorkerCapabilityUnavailable(task_name, tuple(sorted(required)))
    preferred = frozenset(GOVERNED_TASK_PREFERRED_CAPABILITIES.get(task_name, ()))
    if preferred and not registry.has_capable_worker(required | preferred):   # never a refusal: counted only
        GOVERNED_DISPATCH_WITHOUT_BATCH.inc()
