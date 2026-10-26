"""Internal on-behalf-of: the run token of a governed run (#7214, C-1009).

Main is the issuer and the internal Elitea MCP is the resource. When a run is admitted, Main mints one short-lived
token for it: ``sub`` the verified subject (or the mandate), ``uid`` the user the platform acts as, ``act`` the agent
binding (``development:<digest>`` for a development run), ``aud`` ``elitea:internal``, ``scope`` the binding's platform
operations intersected with the user's RBAC (computed once), ``ctx``/``prj``/``run``/``tsk`` the security context it
belongs to, ``exp`` the context's expiry and a ``jti``. It is HMAC-SHA256 signed with a key derived from
``SECRETS_MASTER_KEY``; there is no revocation store: the token is only accepted while its context passes
``validate_context`` (state, expiry and every epoch the context was admitted under).

The token travels to the worker as one task kwarg (``TOKEN_KWARG``), which the worker takes out of the payload before
anything else reads it; the SDK sends it as the bearer of the internal MCP requests of that run and of no other origin.
It is never written to a toolkit's settings, a log line, an audit event or the task meta.

On the MCP side the auth plugin hands a bearer with ``TOKEN_PREFIX`` to ``verify_request`` (registered at init): the
signature, the audience, the expiry, the route (an internal MCP endpoint, or the in-process API call such an endpoint
makes), then the context. The request then runs as the user ``uid`` (``flask.g.auth``) with the claims kept in
``flask.g.internal_obo``: the endpoint's own RBAC decorator still decides, and ``check_operation`` refuses an operation
outside ``scope``. Every write is recorded as a ``platform_operation`` audit event before it is made.
"""
import base64
import hashlib
import hmac
import json
import re
import time
import uuid
from datetime import timezone
from typing import Optional
from urllib.parse import urlsplit

from pylon.core.tools import log
from sqlalchemy import select
from tools import auth, config as c, db, this

from ..models.agent_identity import AgentIdentityBinding, ExecutionSecurityContext
from .mcp_versioning import INTERNAL_MCP_ENVIRON_KEY

TOKEN_PREFIX = "eio1."
TOKEN_KWARG = "internal_run_token"   # the task kwarg that carries the token to the worker (popped there first)
ISSUER = "elitea:main"
AUDIENCE = "elitea:internal"
KEY_CONTEXT = b"elitea-internal-obo-v1"
MAX_TOKEN_CHARS = 4096
CLAIM_KEYS = frozenset({"v", "iss", "aud", "sub", "uid", "act", "scope", "ctx", "prj", "run", "tsk", "iat", "exp",
                        "jti"})
# What the in-process API call of an internal MCP endpoint carries: the claims its endpoint already verified (an object
# in the WSGI environ, which no HTTP header can set), so the call is not validated a second time
VERIFIED_ENVIRON_KEY = "elitea.internal_obo.claims"
DEVELOPMENT_LABEL = "development:"   # what a development run (#7213) has where a governed run has its binding id
INTERNAL_FALLBACK_URL = "http://pylon_main:8080"   # the in-cluster origin internal toolkits use (internal_tools)
# The routes of the internal MCP (routes/mcp_sse.py); the run token is refused on every other route
MCP_ROUTE_ENDPOINTS = frozenset({
    "client_streamable_http", "client_tag_filtered_streamable_http", "client_parametrized_streamable_http",
})

# Platform operations an agent binding may allow (``allowed_platform_operations``). ``read``: any GET; ``build``: any
# other call; ``credentials``: a write to the credential plugins; ``run``: starting an agent or a toolkit tool.
OPERATIONS = ("read", "build", "credentials", "run")
DEFAULT_BINDING_OPERATIONS = ("read",)   # a binding approved before #7214 (no list) may only read
CREDENTIAL_PLUGINS = frozenset({"secrets", "configurations"})
# The RBAC half of the scope: an operation is in scope only when the user holds one of these in the agent's project.
# A coarse pre-filter: the endpoint's own decorator still decides every call. ``read`` needs no extra permission.
OPERATION_PERMISSIONS = {
    "read": frozenset(),
    "build": frozenset({
        "models.applications.applications.create", "models.applications.application.update",
        "models.applications.version.update", "models.applications.tool.patch", "models.applications.skills.create",
    }),
    "credentials": frozenset({
        "configurations.configuration.create", "configurations.configuration.update",
        "configuration.secrets.secret.edit",
    }),
    "run": frozenset({"models.applications.predict.post"}),
}
READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

INTERNAL_TOKEN_INVALID = "INTERNAL_TOKEN_INVALID"
INTERNAL_TOKEN_EXPIRED = "INTERNAL_TOKEN_EXPIRED"
CONTEXT_NOT_LIVE = "CONTEXT_NOT_LIVE"
INTERNAL_SCOPE_DENIED = "INTERNAL_SCOPE_DENIED"
AUDIT_UNAVAILABLE = "AUDIT_UNAVAILABLE"
LEGACY_PAT_INTERNAL_MCP = "LEGACY_PAT_INTERNAL_MCP"
PLATFORM_OPERATION = "PLATFORM_OPERATION"
MESSAGES = {
    INTERNAL_TOKEN_INVALID: "The Elitea tools of this run could not prove who they act for; the call was refused.",
    INTERNAL_TOKEN_EXPIRED: "The Elitea tools of this run are past the run's time limit; start the run again.",
    CONTEXT_NOT_LIVE: "The run this Elitea tool call belongs to is no longer admitted; the call was refused.",
    INTERNAL_SCOPE_DENIED: "This agent is not allowed to do that on your behalf: its approved operations do not "
                           "include it. Do it yourself in Elitea, or ask an operator to extend the agent's binding.",
    AUDIT_UNAVAILABLE: "The security audit trail is unavailable; the change was not made. Try again later.",
}
_B64 = re.compile(r"^[A-Za-z0-9_-]+$")


class TokenUnavailable(Exception):
    """No run token can be minted (no key, or the context cannot be read): the run's internal tools get none."""


# --- key --------------------------------------------------------------------------------------

_KEY = {}


def signing_key() -> Optional[bytes]:
    """The run token key: HMAC-SHA256 of ``SECRETS_MASTER_KEY`` under this purpose, derived once per process. None
    when the deployment has no master key (no token is minted, every token is refused)."""
    master = getattr(c, "SECRETS_MASTER_KEY", None)
    if not master:
        return None
    raw = master.encode() if isinstance(master, str) else bytes(master)
    fingerprint = hashlib.sha256(raw).digest()
    cached = _KEY.get("key")
    if cached is not None and cached[0] == fingerprint:
        return cached[1]
    key = hmac.new(raw, KEY_CONTEXT, hashlib.sha256).digest()
    _KEY["key"] = (fingerprint, key)
    return key


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _mac(key: bytes, body: str) -> str:
    return _b64(hmac.new(key, (TOKEN_PREFIX + body).encode("ascii"), hashlib.sha256).digest())


# --- minting ----------------------------------------------------------------------------------

def binding_operations(project_id, actor) -> tuple:
    """The platform operations the binding behind ``actor`` allows: one read of its row. A development run has no
    binding (the author's own RBAC decides alone), a binding without a list allows ``read`` only."""
    if isinstance(actor, str) and actor.startswith(DEVELOPMENT_LABEL):
        return OPERATIONS
    try:
        binding_id = uuid.UUID(str(actor))
    except ValueError:
        return ()
    with db.get_session(project_id) as session:
        stored = session.execute(
            select(AgentIdentityBinding.allowed_platform_operations).where(AgentIdentityBinding.id == binding_id)
        ).scalar_one_or_none()
    if stored is None:
        return DEFAULT_BINDING_OPERATIONS
    return tuple(op for op in OPERATIONS if op in stored)


def scope_for(project_id, actor, user_id) -> list:
    """``binding operations ∩ the user's RBAC`` in the agent's project, in ``OPERATIONS`` order. Read once per mint."""
    allowed = binding_operations(project_id, actor)
    if not allowed:
        return []
    needs_rbac = any(OPERATION_PERMISSIONS[op] for op in allowed)
    permissions = set(auth.get_user_permissions(user_id, mode="default", project_id=project_id)) if needs_rbac else set()
    return [op for op in allowed if not OPERATION_PERMISSIONS[op] or permissions & OPERATION_PERMISSIONS[op]]


def _origin(url) -> Optional[str]:
    if not isinstance(url, str) or not url:
        return None
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return None
    return f"{parts.scheme}://{parts.netloc}".lower()


def internal_origins() -> list:
    """The origins the worker may send the run token to: the platform's own (``APP_HOST``) and the in-cluster one."""
    found = []
    for url in (getattr(c, "APP_HOST", None), INTERNAL_FALLBACK_URL):
        origin = _origin(url)
        if origin and origin not in found:
            found.append(origin)
    return found


def mint(*, sub, uid, act, scope, ctx, prj, run, tsk, exp, now=None) -> tuple:
    """``(token, claims)``; ``TokenUnavailable`` without a key."""
    key = signing_key()
    if key is None:
        raise TokenUnavailable("no signing key")
    issued = int(now if now is not None else time.time())
    claims = {
        "v": 1, "iss": ISSUER, "aud": AUDIENCE, "sub": str(sub), "uid": int(uid), "act": str(act),
        "scope": list(scope), "ctx": str(ctx), "prj": int(prj), "run": str(run), "tsk": str(tsk),
        "iat": issued, "exp": int(exp), "jti": uuid.uuid4().hex,
    }
    body = _b64(json.dumps(claims, sort_keys=True, separators=(",", ":")).encode("utf-8"))
    return f"{TOKEN_PREFIX}{body}.{_mac(key, body)}", claims


def delivery(token, claims) -> dict:
    """What the worker gets: the token, where it may go, and a stable name of the identity behind it (the SDK keys
    its tool-discovery cache by that, not by a token that changes every run)."""
    identity = hashlib.sha256(json.dumps(
        [claims["sub"], claims["uid"], claims["act"], claims["scope"]], separators=(",", ":")).encode()).hexdigest()
    return {"token": token, "origins": internal_origins(), "identity": identity}


def mint_for_admission(view, task_name) -> dict:
    """The delivery of a run Main just admitted (``ExecutionSecurityContextView``): reads only the binding's
    operations and the user's (cached) permissions."""
    if signing_key() is None:   # before any read: a deployment without the key pays nothing for it
        raise TokenUnavailable("no signing key")
    if view.subject_ref is None or view.subject_user_id is None:
        raise TokenUnavailable("no subject")
    scope = scope_for(view.binding_project_id, view.agent_binding_ref, view.subject_user_id)
    token, claims = mint(
        sub=view.subject_ref, uid=view.subject_user_id, act=view.agent_binding_ref, scope=scope, ctx=view.context_ref,
        prj=view.project_id, run=view.run_id, tsk=task_name, exp=view.expires_at.timestamp())
    return delivery(token, claims)


def mint_for_context(project_id, context_ref, task_name) -> dict:
    """The delivery of a dispatch that runs under a context admitted earlier (a child, a reconcile run): one read of
    the context row, then as at admission. The token expires with the context."""
    if signing_key() is None:
        raise TokenUnavailable("no signing key")
    try:
        context_id = uuid.UUID(str(context_ref))
    except ValueError:
        raise TokenUnavailable("bad context") from None
    columns = ExecutionSecurityContext
    with db.get_session(project_id) as session:
        row = session.execute(
            select(columns.subject, columns.caller, columns.agent_binding_ref, columns.definition_digest,
                   columns.binding_project_id, columns.run_id, columns.expires_at)
            .where(columns.id == context_id)
        ).one_or_none()
    user_id = (row.caller or {}).get("user_id") if row is not None else None
    subject_ref = (row.subject or {}).get("ref") if row is not None else None
    if row is None or not isinstance(user_id, int) or isinstance(user_id, bool) or not subject_ref:
        raise TokenUnavailable("no context")
    actor = (DEVELOPMENT_LABEL + str(row.definition_digest)) if row.agent_binding_ref is None \
        else str(row.agent_binding_ref)
    binding_project_id = row.binding_project_id or project_id
    expires_at = row.expires_at
    exp = (expires_at if expires_at.tzinfo is not None else expires_at.replace(tzinfo=timezone.utc)).timestamp()
    token, claims = mint(
        sub=subject_ref, uid=user_id, act=actor, scope=scope_for(binding_project_id, actor, user_id),
        ctx=str(context_id), prj=project_id, run=row.run_id, tsk=task_name, exp=exp)
    return delivery(token, claims)


# --- verification -----------------------------------------------------------------------------

def _refusal(code, status=401) -> dict:
    return {"ok": False, "status": status, "code": code, "message": MESSAGES[code]}


def parse(token, key, now=None) -> dict:
    """The claims of a token Main signed with ``key``, or ``ValueError(code)``: structure, signature, issuer,
    audience and expiry. The context is not checked here."""
    if not isinstance(token, str) or len(token) > MAX_TOKEN_CHARS or not token.startswith(TOKEN_PREFIX):
        raise ValueError(INTERNAL_TOKEN_INVALID)
    body, sep, mac = token[len(TOKEN_PREFIX):].partition(".")
    if not sep or not _B64.match(body) or not _B64.match(mac):
        raise ValueError(INTERNAL_TOKEN_INVALID)
    if key is None or not hmac.compare_digest(_mac(key, body), mac):
        raise ValueError(INTERNAL_TOKEN_INVALID)
    try:
        claims = json.loads(_unb64(body))
    except (ValueError, TypeError):
        raise ValueError(INTERNAL_TOKEN_INVALID) from None
    if not isinstance(claims, dict) or set(claims) != CLAIM_KEYS or claims.get("v") != 1 \
            or claims.get("iss") != ISSUER or claims.get("aud") != AUDIENCE:
        raise ValueError(INTERNAL_TOKEN_INVALID)
    for name in ("uid", "prj", "iat", "exp"):
        if not isinstance(claims[name], int) or isinstance(claims[name], bool):
            raise ValueError(INTERNAL_TOKEN_INVALID)
    if claims["uid"] <= 0 or not isinstance(claims["scope"], list) or not all(op in OPERATIONS for op in claims["scope"]):
        raise ValueError(INTERNAL_TOKEN_INVALID)
    for name in ("sub", "act", "ctx", "run", "tsk", "jti"):
        if not isinstance(claims[name], str) or not claims[name]:
            raise ValueError(INTERNAL_TOKEN_INVALID)
    if int(now if now is not None else time.time()) >= claims["exp"]:
        raise ValueError(INTERNAL_TOKEN_EXPIRED)
    return claims


def _route_accepts(environ, endpoint, claims) -> Optional[str]:
    """``"mcp"`` on an internal MCP route, ``"inner"`` on the in-process API call of a verified one, else None."""
    if environ.get(INTERNAL_MCP_ENVIRON_KEY) is True:
        verified = environ.get(VERIFIED_ENVIRON_KEY)
        return "inner" if isinstance(verified, dict) and verified.get("jti") == claims["jti"] else None
    if isinstance(endpoint, str) and endpoint.rsplit(".", 1)[-1] in MCP_ROUTE_ENDPOINTS:
        return "mcp"
    return None


def verify_request(token, environ, endpoint=None, module=None, now=None) -> dict:
    """The auth plugin's verifier of ``TOKEN_PREFIX`` bearers: ``{"ok": True, "user_id"}`` (and ``flask.g.internal_obo``
    set to the claims), or ``{"ok": False, "status", "code", "message"}``. Never raises, never logs the token."""
    try:
        claims = parse(token, signing_key(), now=now)
    except ValueError as exc:
        return _refusal(str(exc))
    if endpoint is None:
        import flask  # pylint: disable=C0415
        endpoint = flask.request.endpoint if flask.has_request_context() else None
    route = _route_accepts(environ, endpoint, claims)
    if route is None:
        return _refusal(INTERNAL_TOKEN_INVALID, 403)
    if route == "mcp":
        from .admission import validate_context  # pylint: disable=C0415
        caller = {"kind": "indexer_worker", "task_name": claims["tsk"], "run_id": claims["run"]}
        verdict = validate_context(module or this.module, claims["prj"], claims["ctx"], caller)
        if not isinstance(verdict, dict) or not verdict.get("ok"):
            return _refusal(CONTEXT_NOT_LIVE, 403)
    _set_claims(claims)
    return {"ok": True, "user_id": claims["uid"]}


def _set_claims(claims):
    try:
        import flask  # pylint: disable=C0415
        if flask.has_request_context():
            flask.g.internal_obo = claims
    except Exception:  # pylint: disable=W0703
        pass


def current_claims() -> Optional[dict]:
    """The verified run token claims of this request, or None (a session, a PAT, no request)."""
    try:
        import flask  # pylint: disable=C0415
        if not flask.has_request_context():
            return None
        claims = getattr(flask.g, "internal_obo", None)
    except Exception:  # pylint: disable=W0703
        return None
    return claims if isinstance(claims, dict) else None


# --- authorization and audit on the MCP side ---------------------------------------------------

def operation_of(method, path) -> str:
    """The platform operation of an API call: a read, a write to a credential plugin, or any other write."""
    if str(method or "").upper() in READ_METHODS:
        return "read"
    segments = [segment for segment in str(path or "").split("/") if segment]
    if len(segments) >= 3 and segments[0] == "api" and segments[2] in CREDENTIAL_PLUGINS:
        return "credentials"
    return "build"


class OperationRefused(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code
        self.message = MESSAGES[code]


def _audit(claims, operation, target, tool_name, decision, code):
    subject = claims["sub"]
    event = {
        "event_type": "platform_operation", "decision": decision, "reason_code": code,
        "project_id": claims["prj"], "caller_ref": f"user:{claims['uid']}", "user_ref": subject,
        "mandate_ref": subject[len("mandate:"):] if subject.startswith("mandate:") else None,
        "agent_binding_ref": claims["act"], "runtime_identity_ref": f"context:{claims['ctx']}",
        "run_id": claims["run"], "operation_id": f"{operation}:{tool_name}"[:256], "target_ref": target[:256],
    }
    module = this.module
    profile = getattr(module, "governance_profile", None)
    settings = getattr(module, "admission_settings", None)
    event["policy_revision"] = getattr(profile, "policy_revision", None)
    event["environment"] = getattr(settings, "environment", None)
    this.for_module("tracing").module.security_audit_record_autocommit(
        {key: value for key, value in event.items() if value is not None})


def check_operation(operation, target, tool_name) -> None:
    """Refuse (``OperationRefused``) an operation outside the run token's scope, and record a write before it is made.
    A request without a run token is not touched. A refusal is recorded too; a write whose record cannot be written is
    refused (``AUDIT_UNAVAILABLE``). Reads in scope are not recorded."""
    claims = current_claims()
    if claims is None:
        return
    if operation not in claims["scope"]:
        try:
            _audit(claims, operation, target, tool_name, "deny", INTERNAL_SCOPE_DENIED)
        except Exception as exc:  # pylint: disable=W0703
            log.error("internal_obo: a scope refusal was not recorded (%s)", type(exc).__name__)
        raise OperationRefused(INTERNAL_SCOPE_DENIED)
    if operation == "read":
        return
    try:
        _audit(claims, operation, target, tool_name, "allow", PLATFORM_OPERATION)
    except Exception as exc:  # pylint: disable=W0703
        log.error("internal_obo: a platform operation could not be recorded (%s); refused", type(exc).__name__)
        raise OperationRefused(AUDIT_UNAVAILABLE) from None


def refusal_text(exc: OperationRefused) -> str:
    """What the model and the chat see for a refused tool call: the code, then the fixed readable text."""
    return f"{exc.code}: {exc.message}"


# --- what a payload carries ------------------------------------------------------------------

_INTERNAL_URL = re.compile(r"/app/(?:\{project_id\}|\d+)/mcp/")


def _tool_lists(payload):
    if not isinstance(payload, dict):
        return
    yield payload.get("tools")
    application = payload.get("application") if isinstance(payload.get("application"), dict) else {}
    details = application.get("version_details") if isinstance(application.get("version_details"), dict) else {}
    yield details.get("tools")
    prefetched = application.get("subagent_version_details")
    if isinstance(prefetched, dict):
        for value in prefetched.values():
            if isinstance(value, dict):
                yield value.get("tools")


def has_internal_mcp(payload) -> bool:
    """Does the payload carry an internal Elitea MCP toolkit (its URL has the internal endpoint shape)? Dict reads."""
    for tools in _tool_lists(payload):
        for tool in tools if isinstance(tools, list) else ():
            settings = tool.get("settings") if isinstance(tool, dict) else None
            url = settings.get("url") if isinstance(settings, dict) else None
            if isinstance(url, str) and _INTERNAL_URL.search(url) and str(tool.get("type", "")).startswith("mcp"):
                return True
    return False
