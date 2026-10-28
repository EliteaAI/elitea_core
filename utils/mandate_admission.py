"""The trusted request path of autonomous runs (#6947, C-1009).

A scheduled or webhook run has no signed-in caller, and ``applications_predict_sio`` is an RPC whose ``user_id`` and
``is_system_user`` arguments the caller supplies. The authority of a mandate run therefore cannot travel as an id:
the two in-process callers that know a run is legitimate (the scheduler tick, which loaded the trigger row itself,
and the webhook API, after the signature check) mint a single-use ticket for the admission request they built, and
hand the ticket to ``predict_sio`` by reference. A ticket that was forged, replayed, expired, minted by another
replica, or presented for another agent version is simply not found, and the run is an ordinary system run: refused
with ``MANDATE_REQUIRED`` when governed.

An owner schedule (#7011) is a third trusted caller: its starter loaded the schedule row itself and runs it as the
schedule's owner (``prepare_schedule_run``).

Nothing here runs when governance is off: no ticket is minted or looked up, no row is read.
"""
import secrets
import threading
import time
from collections import OrderedDict
from typing import Optional

from pylon.core.tools import log
from tools import db, this

from ..models.pd.admission import AdmissionRequest
from .governance import is_auditing, is_governed
from .trigger_principal import TriggerPrincipal, resolve_trigger_principal

# Refusals that say "try again", not "no": a webhook sender retries a 5xx and never a 403
UNAVAILABLE_CODES = frozenset({"DEPENDENCY_UNAVAILABLE", "AUDIT_UNAVAILABLE"})
TICKET_TTL_S = 60
MAX_TICKETS = 1000
MANDATE_PREFIX = "mandate:"


class TicketStore:
    """Bounded, expiring, single-use map of ticket -> admission request, in this process's memory only."""

    def __init__(self, ttl_s: int = TICKET_TTL_S, capacity: int = MAX_TICKETS, clock=time.monotonic):
        self._ttl_s = ttl_s
        self._capacity = capacity
        self._clock = clock
        self._lock = threading.Lock()
        self._items = OrderedDict()   # ticket -> (expires, request); insertion order is expiry order

    def __len__(self):
        return len(self._items)

    def mint(self, request: AdmissionRequest) -> str:
        ticket = secrets.token_urlsafe(32)
        with self._lock:
            now = self._clock()
            while self._items and next(iter(self._items.values()))[0] <= now:
                self._items.popitem(last=False)
            while len(self._items) >= self._capacity:
                self._items.popitem(last=False)   # full of live tickets: the oldest goes first
            self._items[ticket] = (now + self._ttl_s, request)
        return ticket

    def take(self, ticket) -> Optional[AdmissionRequest]:
        """The request behind a ticket, once. An unknown, used or expired ticket is ``None``."""
        if not isinstance(ticket, str):
            return None
        with self._lock:
            entry = self._items.pop(ticket, None)
            if entry is None or entry[0] <= self._clock():
                return None
            return entry[1]


def refusal_http_status(code) -> int:
    """The HTTP status of a governed refusal answered to a webhook: 503 when the platform could not decide, else 403."""
    return 503 if code in UNAVAILABLE_CODES else 403


def governed_module():
    """The current plugin's module when credential governance is on, else ``None`` (one attribute read)."""
    module = this.module
    return module if is_governed(module) else None


def mandate_admission_request(  # pylint: disable=R0913
    *, project_id, version_row, principal: TriggerPrincipal, entry_point, trigger_ref, binding_project_id=None,
) -> AdmissionRequest:
    """The admission request of one firing of a trigger whose agent version has a mandate. The mandate is named
    here; whether it can run is decided by admission, from the row, at the moment of the firing."""
    if principal.kind != "mandate" or not str(principal.ref).startswith(MANDATE_PREFIX):
        raise ValueError("the trigger has no mandate")
    return AdmissionRequest(
        project_id=project_id, binding_project_id=binding_project_id or project_id,
        application_id=getattr(version_row, "application_id", None), version_id=getattr(version_row, "id", None),
        run_kind="mandate", entry_point=entry_point, caller_type="trigger", caller_id=None,
        mandate_ref=principal.ref[len(MANDATE_PREFIX):], trigger_ref=trigger_ref,
    )


def mint_trigger_admission(module, request: AdmissionRequest) -> Optional[str]:
    """A single-use ticket for a mandate request, or ``None`` (governance off, or not a mandate request)."""
    if not is_governed(module) or request.run_kind != "mandate":
        return None
    return module._mandate_tickets.mint(request)  # pylint: disable=W0212


def take_trigger_admission(  # pylint: disable=R0913
    module, ticket, *, project_id, version_id, binding_project_id=None,
) -> Optional[AdmissionRequest]:
    """The request a ticket was minted for, once, only for the project and agent version ``predict_sio`` loaded itself.
    The ticket is spent even when it does not match. ``None`` means "no mandate": the run is an ordinary system run."""
    if ticket is None or not is_governed(module):
        return None
    request = module._mandate_tickets.take(ticket)  # pylint: disable=W0212
    if request is None or request.project_id != project_id or request.version_id != version_id:
        return None
    if binding_project_id is not None and request.binding_project_id != binding_project_id:
        return None
    return request


def prepare_trigger_run(  # pylint: disable=R0913
    *, trigger_kind, project_id, version, entry_point, trigger_ref, default_user_id,
):
    """For one firing of an agent-version trigger: ``(user_id, mandate_ticket)``.

    Governance off: ``(default_user_id, None)`` and nothing else happens. Governed with a mandate on the version: the
    mandate owner as the run's user (History, routing principal, conversation author) and a ticket for the request.
    Governed without a mandate: the old user and no ticket, which ``predict_sio`` refuses as ``MANDATE_REQUIRED``.
    """
    module = governed_module()
    if module is None:
        return default_user_id, None
    with db.get_session(project_id) as session:
        principal = resolve_trigger_principal(
            session, trigger_kind=trigger_kind, project_id=project_id, version_id=version.id)
    if principal.kind != "mandate" or principal.owner_user_id is None:
        return default_user_id, None
    request = mandate_admission_request(
        project_id=project_id, version_row=version, principal=principal, entry_point=entry_point,
        trigger_ref=trigger_ref)
    return principal.owner_user_id, mint_trigger_admission(module, request)


def schedule_trigger_ref(schedule_id) -> str:
    """The trigger of one owner schedule (#7011), as admission and the audit trail name it."""
    return f"schedule:entity:{schedule_id}"


def _covering_mandate(project_id, version, owner_id) -> Optional[TriggerPrincipal]:
    """The mandate principal that covers an owner schedule's run, or ``None``: the version's latest mandate, made for
    the schedule's own owner. A schedule is its owner's, so another person's mandate never lends its authority to it.
    One indexed read, the one the legacy scheduler does."""
    with db.get_session(project_id) as session:
        principal = resolve_trigger_principal(
            session, trigger_kind="pipeline_schedule", project_id=project_id, version_id=version.id)
    if principal.kind != "mandate" or principal.owner_user_id is None or principal.owner_user_id != owner_id:
        return None
    return principal


def _audit_only_refusal(principal: Optional[TriggerPrincipal]) -> Optional[str]:
    """What enforce would refuse a run for, as far as the covering mandate's latest revision row says (its state):
    no mandate, or one that is expired, suspended or revoked. ``None``: the mandate is active."""
    if principal is None:
        return "MANDATE_REQUIRED"
    if principal.mandate_state == "expired":
        return "MANDATE_EXPIRED"
    return None if principal.mandate_state == "active" else "MANDATE_INACTIVE"


def prepare_schedule_run(*, project_id, version, schedule_id, owner_id) -> dict:
    """For one run of an owner schedule (#7011, agent or pipeline): the extra arguments of its ``predict_sio`` call.

    Off: ``{}``, nothing is read. Enforce or audit only: the run is ``unattended``, so the request ``predict_sio``
    builds without a ticket is a system run (refused as ``MANDATE_REQUIRED`` under enforce, not rehearsed under audit
    only), while its payload stays the owner's. Enforce: a run its owner's mandate covers gets a ticket for the mandate
    request (entry point ``schedule``). Audit only: a run enforce would refuse for its mandate leaves one ``would_deny``
    event (``_audit_only_refusal``); the rest of a mandate admission is not rehearsed. The audit-only evaluation never
    stops a run.
    """
    module = this.module
    if is_governed(module):
        principal = _covering_mandate(project_id, version, owner_id)
        if principal is None:
            return {"unattended": True}
        request = mandate_admission_request(
            project_id=project_id, version_row=version, principal=principal, entry_point="schedule",
            trigger_ref=schedule_trigger_ref(schedule_id))
        return {"unattended": True, "mandate_ticket": mint_trigger_admission(module, request)}
    if not is_auditing(module):
        return {}
    try:
        code = _audit_only_refusal(_covering_mandate(project_id, version, owner_id))
        if code is not None:
            from .admission import record_schedule_would_deny  # pylint: disable=C0415  # audit only needs the writer
            record_schedule_would_deny(module, code, project_id=project_id, application_id=version.application_id,
                                       trigger_ref=schedule_trigger_ref(schedule_id))
    except Exception as exc:  # pylint: disable=W0703
        log.error("schedule %s: the audit-only evaluation failed (%s)", schedule_id, type(exc).__name__)
    return {"unattended": True}
