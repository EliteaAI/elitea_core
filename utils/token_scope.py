"""What a scoped API token may do on a request (C-1009, #7336).

A scoped token (auth_core ``scope_projects``, ``scope_operations``, an expiry) names the projects it may act in and its
operations there: ``run`` (start an agent, a toolkit tool, a model call, a chat turn; the MCP server), ``build``
(create, change or delete platform entities) and ``admin`` (administration, governance, members and roles, and any
route that names no project). Under the governed profile the auth plugin asks ``check`` for every request an API token
makes, after the token's own admission:

* GET, HEAD and OPTIONS are allowed in the token's projects;
* every other request needs its route's operation (``ROUTE_OPERATIONS``; an unknown write is ``build``, never allowed by
  default) in the token's projects;
* a route that names no project needs ``admin``;
* token management is never open to a token (tokens do not mint or delete tokens), scoped or not.

Off and audit only: nothing is read, everything passes as before. An unscoped (legacy) token: only the token-management
rule applies; its runs are refused or admitted by admission as before. A refusal is 403 with a fixed message, counted,
and audited at most once per (token, code, method, route, project) per ``AUDIT_EVERY_S``.
"""
import threading
import time
from typing import Optional

from pylon.core.tools import log

from .internal_obo import LLM_ENDPOINT, LLM_PATHS, _run_route_project

TOKEN_SCOPE_PROJECT = "TOKEN_SCOPE_PROJECT"
TOKEN_SCOPE_OPERATION = "TOKEN_SCOPE_OPERATION"
DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
MESSAGES = {
    TOKEN_SCOPE_PROJECT: "This API token is not scoped to this project. Use a token scoped to it, or sign in.",
    TOKEN_SCOPE_OPERATION: "This API token's scope does not include this operation here. Use a token scoped to it, or "
                           "sign in.",
    DEPENDENCY_UNAVAILABLE: "The API token could not be checked right now; try again.",
}
# TOKEN_SCOPE_OPERATION on a token-management route says why
TOKEN_MANAGEMENT_MESSAGE = "API tokens cannot create, list or delete API tokens; sign in to manage your tokens."
STATUS_OF = {TOKEN_SCOPE_PROJECT: 403, TOKEN_SCOPE_OPERATION: 403, DEPENDENCY_UNAVAILABLE: 503}
READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
ADMINISTRATION_MODE = "administration"

# Token management: refused to every API token under the governed profile, whatever the method
TOKEN_ROUTES = frozenset({"api.v2.auth.token"})
# Routes by Flask endpoint and their operation. A ``(endpoint, path suffix)`` key holds for that action of a route
# only (a schedule's Run now). Any write not named here is ``build``.
ROUTE_OPERATIONS = {
    # run: an agent, a skill, a toolkit tool, a model call, a chat turn
    "api.v2.elitea_core.predict": "run",
    "api.v2.elitea_core.predict_llm": "run",
    "api.v2.elitea_core.continue_predict": "run",
    "api.v2.elitea_core.skill_predict": "run",
    "api.v2.elitea_core.public_skill_predict": "run",
    "api.v2.elitea_core.test_toolkit_tool": "run",
    "api.v2.elitea_core.tools_call": "run",
    "api.v2.elitea_core.regenerate": "run",
    "api.v2.elitea_core.messages": "run",
    "api.v2.elitea_core.message": "run",
    "api.v2.elitea_core.conversations": "run",
    "api.v2.elitea_core.conversation": "run",
    "api.v2.elitea_core.participants": "run",
    "api.v2.elitea_core.participant": "run",
    "api.v2.elitea_core.entity_settings": "run",
    "api.v2.elitea_core.select_conversation": "run",
    ("api.v2.elitea_core.schedules", "/run"): "run",
    # Elitea as an MCP server
    "elitea_core.client_streamable_http": "run",
    "elitea_core.client_tag_filtered_streamable_http": "run",
    "elitea_core.client_parametrized_streamable_http": "run",
    "elitea_core.client_connected": "run",
    "elitea_core.client_message_received": "run",
    # the LLM proxy's model calls (its project is the X-Project-Id header)
    "runtime_interface_litellm.litellm_route_http__url": "run",
    # admin: governance, identities, mandates, credential administration, members and roles
    "api.v2.elitea_core.governance_settings": "admin",
    "api.v2.elitea_core.governance_settings_history": "admin",
    "api.v2.elitea_core.governance_agents": "admin",
    "api.v2.elitea_core.governance_toolkits": "admin",
    "api.v2.elitea_core.agent_identities": "admin",
    "api.v2.elitea_core.agent_identity": "admin",
    "api.v2.elitea_core.agent_identity_action": "admin",
    "api.v2.elitea_core.autonomous_mandates": "admin",
    "api.v2.elitea_core.autonomous_mandate_action": "admin",
    "api.v2.elitea_core.admin": "admin",
    "api.v2.elitea_core.admin_published_agents": "admin",
    "api.v2.elitea_core.platform_settings": "admin",
    "api.v2.elitea_core.audit": "admin",
    "api.v2.secrets.credential_grants": "admin",
    "api.v2.secrets.credential_grant_action": "admin",
    "api.v2.secrets.credential_references": "admin",
    "api.v2.secrets.credential_reference": "admin",
    "api.v2.secrets.credential_reference_action": "admin",
}
# Whole plugins whose routes are administration (members, roles, invitations, projects, runtime)
ADMIN_PREFIXES = ("api.v2.admin.", "admin.", "admin_ui.")

AUDIT_EVERY_S = 60.0
MAX_AUDITED = 4096
_AUDITED = {}   # (token id, code, method, endpoint, project) -> monotonic time until which no new row is written
_AUDITED_LOCK = threading.Lock()
_COUNTER = {}


def operation_of(endpoint, method, view_args=None, path="") -> str:
    """The operation a request needs: ``read`` for a read, else the route's (``build`` when it is not named)."""
    view_args = view_args or {}
    method = str(method or "").upper()
    endpoint = endpoint or ""
    if endpoint in TOKEN_ROUTES or endpoint.startswith(ADMIN_PREFIXES) or view_args.get("mode") == ADMINISTRATION_MODE:
        return "admin"
    named = ROUTE_OPERATIONS.get(endpoint)
    if named == "run" and endpoint == LLM_ENDPOINT and view_args.get("url") not in LLM_PATHS:
        named = "admin"   # the proxy's own administration, not a model call
    if named is None:
        for key, operation in ROUTE_OPERATIONS.items():
            if isinstance(key, tuple) and key[0] == endpoint and str(path or "").rstrip("/").endswith(key[1]):
                named = operation
    if named == "admin":
        return "admin"
    if method in READ_METHODS:
        return "read"
    return named or "build"


def decide(scope, endpoint, method, view_args=None, path="", project=None) -> Optional[str]:
    """None when a token with ``scope`` (``{"projects", "operations"}``, or None for a legacy token) may make this
    request, else the refusal code. Pure."""
    if endpoint in TOKEN_ROUTES and (scope is not None or str(method or "").upper() not in READ_METHODS):
        return TOKEN_SCOPE_OPERATION   # tokens never mint or delete tokens; a scoped one does not manage them at all
    if scope is None:
        return None
    operation = operation_of(endpoint, method, view_args, path)
    if project is None:
        return None if "admin" in scope["operations"] else TOKEN_SCOPE_OPERATION
    if project not in scope["projects"]:
        return TOKEN_SCOPE_PROJECT
    if operation != "read" and operation not in scope["operations"]:
        return TOKEN_SCOPE_OPERATION
    return None


def _first_audit(key) -> bool:
    now = time.monotonic()
    with _AUDITED_LOCK:
        until = _AUDITED.get(key)
        if until is not None and until > now:
            return False
        if len(_AUDITED) >= MAX_AUDITED:
            for stale in [item for item, ends in _AUDITED.items() if ends <= now] or list(_AUDITED)[:MAX_AUDITED // 4]:
                _AUDITED.pop(stale, None)
        _AUDITED[key] = now + AUDIT_EVERY_S
        return True


def _refuse(module, token_id, code, endpoint, method, project) -> dict:  # pylint: disable=R0913
    _COUNTER[code] = _COUNTER.get(code, 0) + 1
    if code != DEPENDENCY_UNAVAILABLE and _first_audit((token_id, code, method, endpoint, project)):
        log.info("security_metric name=token_scope_refusal_total reason_code=%s", code)
        try:
            from .admission import _record_deny  # pylint: disable=C0415
            _record_deny(module, code, project_id=project, caller_ref=f"token:{token_id}",
                         target_ref=f"{method} {endpoint or '-'}"[:256])
        except Exception as exc:  # pylint: disable=W0703
            log.error("token scope: a refusal was not recorded (%s)", type(exc).__name__)
    message = TOKEN_MANAGEMENT_MESSAGE if endpoint in TOKEN_ROUTES and code == TOKEN_SCOPE_OPERATION else MESSAGES[code]
    return {"status": STATUS_OF[code], "code": code, "message": message}


def _request_parts():
    import flask  # pylint: disable=C0415
    request = flask.request
    return request.endpoint, request.method, request.view_args or {}, request.path, request.args, request.environ


def check(module, token_id, request_parts=None) -> Optional[dict]:
    """The auth plugin's token scope checker: None, or the refusal ``{"status", "code", "message"}``. Off and audit
    only: None, nothing read."""
    from .governance import is_governed  # pylint: disable=C0415
    if not is_governed(module):
        return None
    endpoint, method, view_args, path, args, environ = request_parts or _request_parts()
    from tools import auth  # pylint: disable=C0415
    try:
        scope = auth.token_scope(token_id)
    except Exception as exc:  # pylint: disable=W0703
        log.error("token scope: the token could not be read (%s)", type(exc).__name__)
        return _refuse(module, token_id, DEPENDENCY_UNAVAILABLE, endpoint, method, None)
    # the project as the route itself reads it: the path's, the S3 API's ?project_id, the LLM proxy's X-Project-Id
    project = _run_route_project(environ, endpoint, view_args, args)
    code = decide(scope, endpoint, method, view_args, path, project)
    if code is None:
        return None
    return _refuse(module, token_id, code, endpoint, method, project)
