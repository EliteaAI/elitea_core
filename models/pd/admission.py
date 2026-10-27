"""Admission request and the view of an admitted run (#6756, C-1009).

``AdmissionRequest`` is built at an entry point from what that entry point can trust: the
authenticated connection (``auth.sio_users[sid]``) or HTTP request (``flask.g.auth``), and the
version row it loaded itself. It never reads ``user_context``, ``user_id`` or any id the client
put in the payload. The session reference it may carry is a secret: it is never dumped or printed.
"""
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from ...utils.governance import is_auditing, is_governed

CALLER_TYPES = ("user", "token")
# The agent or pipeline editor's test chat (#7213): the one entry point admission may run as a ``development`` run (the
# author's own identity and credentials, no binding). Set by ``chat_all.predict_sio`` from the conversation row it
# loaded, never from anything the client sent with the turn.
ENTRY_EDITOR_CHAT = "editor_chat"
# A person testing a toolkit's tool, or indexing it on demand (#7229): development runs of that person, set by the test
# entry (``test_toolkit_tool_sio``, ``do_runtool``) from the tool it was asked to run, never from the payload
ENTRY_TOOLKIT_TEST = "toolkit_test"
ENTRY_INDEX_ON_DEMAND = "index_on_demand"
# A plain LLM chat of the chat page (no agent): the person's own run, with no definition to bind (#7227 D1)
ENTRY_CHAT_LLM = "chat_sio_llm"


class AdmissionRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", arbitrary_types_allowed=True)

    project_id: int
    binding_project_id: int
    application_id: Optional[int] = None
    version_id: Optional[int] = None
    run_kind: Literal["interactive", "system", "mandate"]
    entry_point: Literal["chat_sio", "editor_chat", "chat_sio_llm", "predict_api", "schedule", "webhook", "toolkit_test",
                         "index_on_demand"]
    caller_type: Literal["user", "token", "public", "trigger"]
    caller_id: Optional[int] = None
    caller_reference: Optional[SecretStr] = Field(default=None, exclude=True, repr=False)
    trace_ref: Optional[str] = None
    # A run under an autonomous mandate (#6947): the mandate id and the trigger that fired it
    # (``schedule:version:<id>`` / ``webhook:version:<id>``). Only the trusted trigger paths build these.
    mandate_ref: Optional[str] = None
    trigger_ref: Optional[str] = None
    # A toolkit run (#7229): the saved toolkit it tests or indexes (no application, no version)
    toolkit_id: Optional[int] = None


class ExecutionSecurityContextView(BaseModel):
    """What an admitted run gets back: references only, nothing secret."""
    model_config = ConfigDict(frozen=True, extra="forbid")

    context_ref: str
    run_id: str
    attempt: int
    project_id: int
    application_id: Optional[int] = None   # None for a toolkit run (#7229)
    version_id: Optional[int] = None
    agent_binding_ref: str
    policy_revision: str
    expires_at: datetime
    # The credential-use decisions of the run (the entries of ``allowed_bindings``): references and ids, never
    # material. Dispatch seals the payload against exactly these (#6946)
    allowed_bindings: list = Field(default_factory=list)
    # A mandate run only: when Main stops it, and the code that says which bound applied (#6947)
    run_deadline: Optional[datetime] = None
    run_deadline_code: Optional[str] = None
    # Who the run acts for (#7214): the subject reference (``idp:<tid>:<oid>`` or ``mandate:<id>``), the user the
    # platform acts as, and the agent's project; what the run token is minted from. References only
    subject_ref: Optional[str] = None
    subject_user_id: Optional[int] = None
    binding_project_id: Optional[int] = None
    # The sub-agents admitted with the run (#7227): their decisions by prefetch key (``<application id>:<version id>``),
    # which dispatch seals each one's toolkits against
    children: dict = Field(default_factory=dict)


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
    module, *, sid, project_id, binding_project_id, version_row, run_kind, entry_point, toolkit_id=None,
) -> Optional[AdmissionRequest]:
    """The admission request for this call, or ``None`` when governance is off (nothing is built or read). Audit only
    builds it too: the same request is evaluated, and nothing is refused.

    An unreadable or unknown holder is the ``public`` caller, which admission refuses.
    """
    if not (is_governed(module) or is_auditing(module)):
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
        toolkit_id=toolkit_id if isinstance(toolkit_id, int) and not isinstance(toolkit_id, bool) else None,
    )
