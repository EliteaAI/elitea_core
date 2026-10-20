"""Admission request and the view of an admitted run (#6756, C-1009).

``AdmissionRequest`` is built at an entry point from what that entry point can trust: the
authenticated connection (``auth.sio_users[sid]``) or HTTP request (``flask.g.auth``), and the
version row it loaded itself. It never reads ``user_context``, ``user_id`` or any id the client
put in the payload. The session reference it may carry is a secret: it is never dumped or printed.
"""
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from ...utils.governance import is_governed

CALLER_TYPES = ("user", "token")


class AdmissionRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    project_id: int
    binding_project_id: int
    application_id: Optional[int] = None
    version_id: Optional[int] = None
    run_kind: Literal["interactive", "system", "mandate"]
    entry_point: Literal["chat_sio", "chat_sio_llm", "predict_api", "schedule", "webhook"]
    caller_type: Literal["user", "token", "public", "trigger"]
    caller_id: Optional[int] = None
    caller_reference: Optional[SecretStr] = Field(default=None, exclude=True, repr=False)
    trace_ref: Optional[str] = None
    # A run under an autonomous mandate (#6947): the mandate id and the trigger that fired it
    # (``schedule:version:<id>`` / ``webhook:version:<id>``). Only the trusted trigger paths build these.
    mandate_ref: Optional[str] = None
    trigger_ref: Optional[str] = None


class ExecutionSecurityContextView(BaseModel):
    """What an admitted run gets back: references only, nothing secret."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    context_ref: str
    run_id: str
    attempt: int
    project_id: int
    application_id: int
    version_id: int
    agent_binding_ref: str
    policy_revision: str
    expires_at: datetime
    # A mandate run only: when Main stops it, and the code that says which bound applied (#6947)
    run_deadline: Optional[datetime] = None
    run_deadline_code: Optional[str] = None


def _holder(sid):
    """The authenticated holder of this call, or None. ``sid`` first, then the HTTP request."""
    from tools import auth  # pylint: disable=C0415
    if sid:
        return auth.sio_users.get(sid)
    import flask  # pylint: disable=C0415
    if flask.has_request_context():
        return getattr(flask.g, "auth", None)
    return None


def admission_request_from(  # pylint: disable=R0913
    module, *, sid, project_id, binding_project_id, version_row, run_kind, entry_point,
) -> Optional[AdmissionRequest]:
    """The admission request for this call, or ``None`` when governance is off (nothing is built or read).

    An unreadable or unknown holder is the ``public`` caller, which admission refuses.
    """
    if not is_governed(module):
        return None
    holder = _holder(sid)
    caller_type, caller_id, reference = "public", None, None
    kind = getattr(holder, "type", None)
    if kind in CALLER_TYPES:
        try:
            caller_id = int(getattr(holder, "id", None))
            caller_type = kind
        except (TypeError, ValueError):
            caller_id = None
    if caller_type == "user":
        reference = getattr(holder, "reference", None)
    return AdmissionRequest(
        project_id=project_id,
        binding_project_id=binding_project_id,
        application_id=getattr(version_row, "application_id", None),
        version_id=getattr(version_row, "id", None),
        run_kind=run_kind,
        entry_point=entry_point,
        caller_type=caller_type,
        caller_id=caller_id,
        caller_reference=reference if isinstance(reference, str) and reference else None,
    )
