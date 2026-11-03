"""Internal on-behalf-of: the run token of a governed run (#7214, C-1009).

Main is the issuer and the internal Elitea MCP is the resource. When a run is admitted, Main mints one short-lived
token for it: ``sub`` the verified subject (or the mandate), ``uid`` the user the platform acts as, ``act`` the agent
binding (``development:<digest>`` for a development run), ``aud`` ``elitea:internal``, ``scope`` the binding's platform
operations intersected with the user's RBAC (computed once), ``ctx``/``prj``/``run``/``tsk`` the security context it
belongs to, ``tgt`` the projects its internal MCP toolkits address, ``exp`` the context's expiry and a ``jti``. It is
HMAC-SHA256 signed with a key derived from ``SECRETS_MASTER_KEY``; there is no revocation store: the token is only
accepted while its context passes ``validate_context`` (state, expiry and every epoch the context was admitted under).
It is a bearer: whoever holds it acts as the run (the caller checked by ``validate_context`` is built from its own
claims), which is why it never leaves the run's worker, goes only to the platform's own origins and lives no longer
than the context. A token is minted only for a run that can reach the internal MCP (``needs_run_token``).

The token travels to the worker as one task kwarg (``TOKEN_KWARG``), which the worker takes out of the payload before
anything else reads it; the SDK sends it as the bearer of the internal MCP requests of that run and of no other origin.
It is never written to a toolkit's settings, a log line, an audit event or the task meta.

On the MCP side the auth plugin hands a bearer with ``TOKEN_PREFIX`` to ``verify_request`` (registered at init): the
signature, the audience, the expiry, the route (an internal MCP endpoint, the in-process API call such an endpoint
makes, or a route of the run's own platform client, ``RUN_ROUTES``, #7229), the project it addresses (``tgt``; the run's
own project on a client route), then the context (a positive verdict is reused for a few seconds). The
request then runs as the user ``uid`` (``flask.g.auth``) with the claims kept in ``flask.g.internal_obo``: the endpoint's
own RBAC decorator still decides, and ``check_operation`` refuses an operation outside ``scope``, and any write the
explicit map (``ENDPOINT_OPERATIONS``) does not know. Every write is recorded as a ``platform_operation`` audit event
before it is made.
"""
import base64
import copy
import hashlib
import hmac
import json
import posixpath
import re
import threading
import time
import uuid
from datetime import timezone
from typing import Optional
from urllib.parse import parse_qs, unquote, urlsplit

from pylon.core.tools import log
from sqlalchemy import select
from tools import auth, config as c, db, this

from ..models.agent_identity import AgentIdentityBinding, ExecutionSecurityContext
from .internal_base_url import internal_base_url
from .mcp_versioning import INTERNAL_MCP_ENVIRON_KEY

TOKEN_PREFIX = "eio1."
TOKEN_KWARG = "internal_run_token"   # the task kwarg that carries the token to the worker (popped there first)
ISSUER = "elitea:main"
AUDIENCE = "elitea:internal"
KEY_CONTEXT = b"elitea-internal-obo-v1"
MAX_TOKEN_CHARS = 4096
CLAIM_KEYS = frozenset({"v", "iss", "aud", "sub", "uid", "act", "scope", "ctx", "prj", "tgt", "run", "tsk", "iat",
                        "exp", "jti"})
MAX_TARGETS = 16
MAX_CLOCK_SKEW_S = 60   # a token issued later than this in the future is refused
VERDICT_TTL_S = 5.0     # a positive context verdict is reused this long (never longer than the auth epoch TTL)
MAX_VERDICTS = 4096
# An expired run token is refused on every request; its audit row is written once per (ctx, jti) this long (#7294 P-12):
# a run that keeps calling with it after its end adds no row per call. The refusal and the counter are not cached
EXPIRED_AUDIT_TTL_S = 600.0
MAX_BODY_DEPTH = 8        # the walk of a tool call's arguments for project ids: deeper or larger is refused
MAX_BODY_NODES = 2000
MAX_NESTED_JSON_CHARS = 65536
UNVERIFIED_LOG_EVERY_S = 10.0   # a refused token that is not Main's is logged at most this often (the counter counts all)
# What the in-process API call of an internal MCP endpoint carries: the claims its endpoint already verified (an object
# in the WSGI environ, which no HTTP header can set), so the call is not validated a second time
VERIFIED_ENVIRON_KEY = "elitea.internal_obo.claims"
DEVELOPMENT_LABEL = "development:"   # what a development run (#7213) has where a governed run has its binding id
INTERNAL_FALLBACK_URL = "http://pylon_main:8080"   # the in-cluster origin internal toolkits use (internal_tools)
# The routes of the internal MCP (routes/mcp_sse.py, blueprint ``elitea_core``); the run token is refused elsewhere
MCP_ROUTE_ENDPOINTS = frozenset({
    "elitea_core.client_streamable_http", "elitea_core.client_tag_filtered_streamable_http",
    "elitea_core.client_parametrized_streamable_http",
})
# The routes of the run's own platform client (#7229): what the SDK and the worker call during a run, on the run token
# as its only bearer, by Flask endpoint and method. The request runs as the run's user in the run's project only, and
# each endpoint's own RBAC still decides. Any other route (tokens, governance, admin, secrets, configurations, listings,
# a public agent, another project) refuses the run token with one answer for all.
LLM_ENDPOINT = "runtime_interface_litellm.litellm_route_http__url"
S3_ENDPOINTS = frozenset({"artifacts.s3_bucket_operations", "artifacts.s3_object_operations"})
RUN_ROUTES = {
    "api.v2.elitea_core.application": frozenset({"GET"}),        # a sub-agent's name and description
    "api.v2.elitea_core.version": frozenset({"PATCH"}),          # a sub-agent's details, sealed (``seal_fetched_version``)
    "api.v2.elitea_core.tools_list": frozenset({"GET"}),         # the user's platform MCP servers
    "api.v2.elitea_core.tools_call": frozenset({"POST"}),
    "api.v2.elitea_core.tool_spend_check": frozenset({"POST"}),
    "api.v2.elitea_core.tool_spend_record": frozenset({"POST"}),
    "api.v2.configurations.models": frozenset({"GET"}),
    "api.v2.artifacts.buckets": frozenset({"GET", "POST"}),
    "api.v2.artifacts.artifacts": frozenset({"GET", "POST"}),
    "api.v2.artifacts.artifact": frozenset({"GET", "DELETE"}),
    "artifacts.s3_bucket_operations": frozenset({"GET", "HEAD"}),
    "artifacts.s3_object_operations": frozenset({"GET", "HEAD", "PUT", "DELETE"}),
    LLM_ENDPOINT: frozenset({"POST"}),
    "api.v2.secrets.secret": frozenset({"GET"}),                 # one platform-internal secret only (RUN_ROUTE_VALUES)
}
# Routes of RUN_ROUTES held to fixed values of a path argument: the checkpointer's connection string the worker falls back
# to reading when Main's prefetch did not reach it (a fan-out child); no other secret is returned to a run token
RUN_ROUTE_VALUES = {"api.v2.secrets.secret": ("secret", frozenset({"pgvector_project_connstr"}))}
# The model calls of the LLM proxy (its LLM endpoints, never the LiteLLM admin ones), as the route's ``url``
LLM_PATHS = frozenset({
    "v1/chat/completions", "v1/responses", "v1/messages", "v1/messages/count_tokens", "v1/embeddings",
    "v1/images/generations", "v1/images/edits", "v1/auto-routing/resolve",
})
ADMINISTRATION_MODE = "administration"

# Platform operations an agent binding may allow (``allowed_platform_operations``). ``read``: any GET; ``build``:
# creating, changing or deleting platform entities; ``credentials``: anything that creates, changes or returns a
# secret; ``run``: starting an agent, a skill, a toolkit tool or a model call.
OPERATIONS = ("read", "build", "credentials", "run")
# No list, or an explicitly empty one: read only (§21). Refusing reads too is not offered: it is the binding without
# the internal toolkit, which needs no token at all.
DEFAULT_BINDING_OPERATIONS = ("read",)
# Every write the internal MCP exposes as an API tool (``mcp_tool=True``), by method and path template under /api/v2/,
# and its operation. A write that is not here is refused (``INTERNAL_OPERATION_UNKNOWN``): a new MCP-exposed write
# needs its line before an agent can call it. Reads (GET) are ``read``.
ENDPOINT_OPERATIONS = {
    # credentials: create, change or return a secret
    ("POST", "artifacts/s3_credentials/{mode}/{project_id}"): "credentials",
    ("PUT", "artifacts/s3_credentials/{mode}/{project_id}/{access_key_id}"): "credentials",
    ("POST", "configurations/configurations/{project_id}"): "credentials",
    ("PUT", "configurations/configuration/{project_id}/{config_id}"): "credentials",
    ("POST", "configurations/models/{project_id}"): "credentials",
    ("POST", "secrets/secrets/{mode}/{project_id}"): "credentials",
    ("PUT", "secrets/secret/{mode}/{project_id}/{secret}"): "credentials",
    # run: start an agent, a skill or a model call
    ("POST", "elitea_core/messages/{mode}/{project_id}/{conversation_uuid}"): "run",
    ("POST", "elitea_core/continue_predict/{mode}/{project_id}/{conversation_uuid}"): "run",
    ("POST", "elitea_core/skill_predict/{mode}/{project_id}/{skill_id}"): "run",
    ("POST", "elitea_core/public_skill_predict/{mode}/{project_id}/{public_skill_id}"): "run",
    ("POST", "elitea_core/generate_project_context_draft/{mode}/{project_id}"): "run",
    ("POST", "elitea_core/generate_skill_draft/{mode}/{project_id}"): "run",
    # build: platform entities
    ("POST", "elitea_core/applications/{mode}/{project_id}"): "build",
    ("POST", "elitea_core/versions/{mode}/{project_id}/{application_id}"): "build",
    ("PUT", "elitea_core/version/{mode}/{project_id}/{application_id}/{version_id}"): "build",
    ("POST", "elitea_core/version_instruction_patch/{mode}/{project_id}/{application_id}/{version_id}"): "build",
    ("PATCH", "elitea_core/application_relation/{mode}/{project_id}/{application_id}/{version_id}"): "build",
    ("POST", "elitea_core/tools/{mode}/{project_id}"): "build",
    ("PUT", "elitea_core/tool/{mode}/{project_id}/{tool_id}"): "build",
    ("PATCH", "elitea_core/tool/{mode}/{project_id}/{tool_id}"): "build",
    ("POST", "elitea_core/skills/{mode}/{project_id}"): "build",
    ("PUT", "elitea_core/skill/{mode}/{project_id}/{skill_id}"): "build",
    ("PATCH", "elitea_core/skill/{mode}/{project_id}/{skill_id}"): "build",
    ("PUT", "elitea_core/project_context/{mode}/{project_id}/project-context"): "build",
    ("DELETE", "elitea_core/project_context/{mode}/{project_id}/project-context"): "build",
    ("POST", "elitea_core/folder/{mode}/{project_id}"): "build",
    ("PUT", "elitea_core/folder/{mode}/{project_id}/{folder_id}"): "build",
    ("POST", "elitea_core/conversations/{mode}/{project_id}"): "build",
    ("PUT", "elitea_core/conversation/{mode}/{project_id}/{conversation_id}"): "build",
    ("POST", "elitea_core/participants/{mode}/{project_id}/{conversation_id}"): "build",
    ("DELETE", "elitea_core/participant/{mode}/{project_id}/{conversation_id}/{participant_id}"): "build",
    ("PATCH", "elitea_core/entity_settings/{mode}/{project_id}/{conversation_id}/{participant_id}"): "build",
    ("POST", "artifacts/artifacts/{mode}/{project_id}/{bucket}"): "build",
    ("DELETE", "artifacts/artifacts/{mode}/{project_id}/{bucket}"): "build",
    ("POST", "artifacts/buckets/{mode}/{project_id}"): "build",
    ("PUT", "artifacts/buckets/{mode}/{project_id}"): "build",
    ("PATCH", "artifacts/buckets/{mode}/{project_id}"): "build",
    ("POST", "social/folders/{mode}/{project_id}"): "build",
    ("PUT", "social/folder/{mode}/{project_id}/{folder_id}"): "build",
    ("PUT", "social/move_to_folder/{mode}/{project_id}"): "build",
    ("PUT", "notifications/notification/{mode}/{project_id}/{notification_id}"): "build",
}
# Credential material on the model path (#7228, C9-SEC-21). Every MCP-exposed route is ``material_in`` (its request
# carries a secret value), ``material_out`` (its answer returns one) or ``none``. Derived from the OpenAPI registry: a
# request or response schema with a secret-typed field (``format: password`` or ``writeOnly``, what ``SecretStr``
# produces) is material. The lines below are the routes whose registry schema cannot show it: no response model, a
# secret value typed as plain text, or a body whose secret fields depend on the configuration type (``per_request``:
# decided from the call's own body, ``material_of_call``). A run token never executes a material route, whatever its
# scope: the agent asks the person through the credential form instead (``CREDENTIALS_VIA_FORM``).
MATERIAL_IN = "material_in"
MATERIAL_OUT = "material_out"
MATERIAL_NONE = "none"
MATERIAL_PER_REQUEST = "per_request"
DECLARED_MATERIAL = {
    ("POST", "artifacts/s3_credentials/{mode}/{project_id}"): MATERIAL_OUT,     # answers secret_access_key
    ("PUT", "artifacts/s3_credentials/{mode}/{project_id}/{access_key_id}"): MATERIAL_OUT,   # rotate: the same
    ("POST", "secrets/secrets/{mode}/{project_id}"): MATERIAL_IN,               # ``value`` is the secret
    ("PUT", "secrets/secret/{mode}/{project_id}/{secret}"): MATERIAL_IN,
    ("GET", "secrets/secret/{mode}/{project_id}/{secret}"): MATERIAL_OUT,       # answers the value (not MCP-exposed)
    ("POST", "configurations/configurations/{project_id}"): MATERIAL_PER_REQUEST,   # ``data`` per configuration type
    ("PUT", "configurations/configuration/{project_id}/{config_id}"): MATERIAL_PER_REQUEST,
    ("POST", "elitea_core/applications/{mode}/{project_id}"): MATERIAL_PER_REQUEST,   # ``webhook_secret`` (below)
}
# The body fields of a ``per_request`` route (other than a configuration's ``data``) that hold a secret value, whatever
# the registry schema says of them (``SecretString`` publishes no secret format)
PER_REQUEST_FIELDS = {("POST", "elitea_core/applications/{mode}/{project_id}"): ("webhook_secret",)}
MAX_SCHEMA_WALK = 2000   # nodes of one schema walk; a larger schema counts as material
MATERIAL_UNREVIEWED = "unreviewed"   # a field named like a secret no one classified: material until a line says
MATERIAL_UNKNOWN = "unknown"         # the registry could not be read: material (fails closed)
# The build writes whose body may carry a typed secret (#7228 review M-1): a toolkit's settings, a version's pipeline
# webhook secret or toolkit settings
TOOLKIT_WRITES = frozenset({("POST", "elitea_core/tools/{mode}/{project_id}"),
                            ("PUT", "elitea_core/tool/{mode}/{project_id}/{tool_id}")})
VERSION_WRITES = frozenset({("POST", "elitea_core/versions/{mode}/{project_id}/{application_id}"),
                            ("PUT", "elitea_core/version/{mode}/{project_id}/{application_id}/{version_id}")})
DECLARED_MATERIAL = {**DECLARED_MATERIAL, **{key: MATERIAL_PER_REQUEST for key in TOOLKIT_WRITES | VERSION_WRITES}}
PIPELINE_SECRET_KEYS = frozenset({"webhook_secret", "webhook_signing_secret"})
SECRET_REFERENCE = "{{secret."
# A field named like a secret (``secret``, ``token``, ``password``, an API/private/access key, a credential)
SECRET_NAME = re.compile(r"(secret|token|passw|api_?key|private_?key|access_?key|credential)", re.IGNORECASE)
# Names that match ``SECRET_NAME`` and were reviewed as holding no secret (a model's token budget)
REVIEWED_FIELD_NAMES = frozenset({"max_tokens"})
MAX_REDACT_NODES = 50_000

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
INTERNAL_OPERATION_UNKNOWN = "INTERNAL_OPERATION_UNKNOWN"
INTERNAL_TARGET_DENIED = "INTERNAL_TARGET_DENIED"
INTERNAL_MCP_ORIGIN_NOT_ALLOWED = "INTERNAL_MCP_ORIGIN_NOT_ALLOWED"
INTERNAL_MCP_OWN_AUTH = "INTERNAL_MCP_OWN_AUTH"   # audit only: what enforce would strip from a toolkit
DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
AUDIT_UNAVAILABLE = "AUDIT_UNAVAILABLE"
LEGACY_PAT_INTERNAL_MCP = "LEGACY_PAT_INTERNAL_MCP"
PLATFORM_OPERATION = "PLATFORM_OPERATION"
CREDENTIALS_VIA_FORM = "CREDENTIALS_VIA_FORM"
MESSAGES = {
    INTERNAL_TOKEN_INVALID: "The Elitea tools of this run could not prove who they act for; the call was refused.",
    INTERNAL_TOKEN_EXPIRED: "The Elitea tools of this run are past the run's time limit; start the run again.",
    CONTEXT_NOT_LIVE: "The run this Elitea tool call belongs to is no longer admitted; the call was refused.",
    INTERNAL_SCOPE_DENIED: "This agent is not allowed to do that on your behalf: its approved operations do not "
                           "include it. Do it yourself in Elitea, or ask an operator to extend the agent's binding.",
    AUDIT_UNAVAILABLE: "The security audit trail is unavailable; the change was not made. Try again later.",
    INTERNAL_OPERATION_UNKNOWN: "This Elitea tool is not available to agents acting on your behalf; the call was "
                                "refused. Do it yourself in Elitea.",
    INTERNAL_TARGET_DENIED: "This run's Elitea tools may not act in that project; the call was refused.",
    INTERNAL_MCP_ORIGIN_NOT_ALLOWED: "An Elitea MCP toolkit of this agent points to an address the platform does not "
                                     "serve its tools from; the run was not started. Use the built-in Elitea tools.",
    DEPENDENCY_UNAVAILABLE: "The Elitea tools of this run cannot be checked right now; try again in a moment.",
    CREDENTIALS_VIA_FORM: "An agent never handles credential values: it must ask you for the credential through the "
                          "credential form. Nothing was written.",
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
    """The platform operations the binding behind ``actor`` allows: one SELECT of its column (a development run reads
    nothing: it has no binding, the author's own RBAC decides alone). No list or an empty one allows ``read`` only."""
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
    if not stored:
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
    """The origins the worker may send the run token to: the platform's own (``APP_HOST``), the one the internal
    toolkits are addressed at (``internal_base_url``, #7278) and the in-cluster one."""
    found = []
    for url in (getattr(c, "APP_HOST", None), internal_base_url(), INTERNAL_FALLBACK_URL):
        origin = _origin(url)
        if origin and origin not in found:
            found.append(origin)
    return found


MANDATE_PREFIX = "mandate:"   # the subject ref of a run under a mandate


def _max_run_lifetime_s() -> int:
    from .admission import MAX_RUN_LIFETIME_S  # pylint: disable=C0415
    return MAX_RUN_LIFETIME_S


def mint(*, sub, uid, act, scope, ctx, prj, tgt, run, tsk, exp, now=None) -> tuple:
    """``(token, claims)``; ``TokenUnavailable`` without a key."""
    key = signing_key()
    if key is None:
        raise TokenUnavailable("no signing key")
    issued = int(now if now is not None else time.time())
    claims = {
        "v": 1, "iss": ISSUER, "aud": AUDIENCE, "sub": str(sub), "uid": int(uid), "act": str(act),
        "scope": list(scope), "ctx": str(ctx), "prj": int(prj), "tgt": sorted({int(p) for p in tgt})[:MAX_TARGETS],
        "run": str(run), "tsk": str(tsk),
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


def mint_for_admission(view, task_name, payload, internal=True) -> dict:
    """The delivery of a run Main just admitted (``ExecutionSecurityContextView``): reads only the binding's
    operations and the user's (cached) permissions. ``internal`` False (#7229): the token only serves the run's own
    platform client (``RUN_ROUTES``): no scope and no target project, so nothing is read and no MCP route accepts it."""
    if signing_key() is None:   # before any read: a deployment without the key pays nothing for it
        raise TokenUnavailable("no signing key")
    if view.subject_ref is None or view.subject_user_id is None:
        raise TokenUnavailable("no subject")
    scope = scope_for(view.binding_project_id, view.agent_binding_ref, view.subject_user_id) if internal else []
    mark_credential_form(payload, "credentials" in scope)
    # a person's run lives to the cap (the row decides liveness, #7229); a mandate's to its own deadline
    exp = view.expires_at.timestamp() if str(view.subject_ref).startswith(MANDATE_PREFIX) \
        else time.time() + _max_run_lifetime_s()
    token, claims = mint(
        sub=view.subject_ref, uid=view.subject_user_id, act=view.agent_binding_ref, scope=scope, ctx=view.context_ref,
        prj=view.project_id, tgt=target_projects(payload, view.project_id, view.subject_user_id) if internal else [],
        run=view.run_id, tsk=task_name, exp=exp)
    return delivery(token, claims)


# What a run token minted for a context admitted earlier is built from (``mint_for_context``)
MINT_COLUMNS = (
    ExecutionSecurityContext.subject, ExecutionSecurityContext.caller, ExecutionSecurityContext.agent_binding_ref,
    ExecutionSecurityContext.definition_digest, ExecutionSecurityContext.binding_project_id,
    ExecutionSecurityContext.run_id, ExecutionSecurityContext.expires_at, ExecutionSecurityContext.created_at,
)


def mint_for_context(project_id, context_ref, task_name, payload, internal=True, row=None) -> dict:  # pylint: disable=R0913
    """The delivery of a dispatch that runs under a context admitted earlier (a child, a reconcile run): one read of
    the context row, then as at admission (``internal`` as there). The token expires with the context. ``row``: the
    ``MINT_COLUMNS`` of this context the caller already read in the same dispatch (the seal's read of the parent,
    #7294 P-11); nothing is read then."""
    if signing_key() is None:
        raise TokenUnavailable("no signing key")
    try:
        context_id = uuid.UUID(str(context_ref))
    except ValueError:
        raise TokenUnavailable("bad context") from None
    if row is None:
        with db.get_session(project_id) as session:
            row = session.execute(select(*MINT_COLUMNS).where(ExecutionSecurityContext.id == context_id)).one_or_none()
    user_id = (row.caller or {}).get("user_id") if row is not None else None
    subject_ref = (row.subject or {}).get("ref") if row is not None else None
    if row is None or not isinstance(user_id, int) or isinstance(user_id, bool) or not subject_ref:
        raise TokenUnavailable("no context")
    actor = (DEVELOPMENT_LABEL + str(row.definition_digest)) if row.agent_binding_ref is None \
        else str(row.agent_binding_ref)
    binding_project_id = row.binding_project_id or project_id
    mandate = (row.subject or {}).get("kind") == "mandate"
    moment = row.expires_at if mandate else row.created_at   # a person's run lives to the cap (#7229)
    exp = (moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)).timestamp() \
        + (0 if mandate else _max_run_lifetime_s())
    scope = scope_for(binding_project_id, actor, user_id) if internal else []
    mark_credential_form(payload, "credentials" in scope)
    token, claims = mint(
        sub=subject_ref, uid=user_id, act=actor, scope=scope,
        ctx=str(context_id), prj=project_id, tgt=target_projects(payload, project_id, user_id) if internal else [],
        run=row.run_id, tsk=task_name, exp=exp)
    return delivery(token, claims)


# --- verification -----------------------------------------------------------------------------

STATUS_OF = {INTERNAL_TOKEN_INVALID: 401, INTERNAL_TOKEN_EXPIRED: 401, INTERNAL_TARGET_DENIED: 403,
             CONTEXT_NOT_LIVE: 403, DEPENDENCY_UNAVAILABLE: 503}
_VERDICTS = {}            # (ctx, jti) -> monotonic deadline of a positive context verdict
_VERDICTS_LOCK = threading.Lock()
_EXPIRED_AUDITED = {}     # (ctx, jti) -> monotonic time until which an expired token is refused without a new audit row
_COUNTER = {}
_UNVERIFIED_LOG = {"at": 0.0, "suppressed": 0}


def _refusal(code, status=None) -> dict:
    return {"ok": False, "status": status or STATUS_OF.get(code, 401), "code": code, "message": MESSAGES[code]}


def _count(result, code, line=False):
    """``internal_token_verification_total{result, reason_code}``. The counter counts every verification; a log line
    is written only when asked (``line``: a refusal of Main's own token, as rare as its audit row). Tokens that are not
    Main's are logged at most every ``UNVERIFIED_LOG_EVERY_S``, so a stream of forged bearers cannot drive log volume."""
    key = (result, code)
    _COUNTER[key] = _COUNTER.get(key, 0) + 1
    if line:
        log.info("security_metric name=internal_token_verification_total result=%s reason_code=%s", result, code)


def _count_unverified(code):
    _count("deny", code)
    now = time.monotonic()
    if now - _UNVERIFIED_LOG["at"] < UNVERIFIED_LOG_EVERY_S:
        _UNVERIFIED_LOG["suppressed"] += 1
        return
    suppressed, _UNVERIFIED_LOG["at"], _UNVERIFIED_LOG["suppressed"] = _UNVERIFIED_LOG["suppressed"], now, 0
    log.info("security_metric name=internal_token_verification_total result=deny reason_code=%s suppressed=%s",
             code, suppressed)


class TokenRejected(ValueError):
    """A token whose signature and claim shapes hold but which is refused (issue time, lifetime, expiry): its claims
    are Main's own, so the refusal can be audited with them."""

    def __init__(self, code, claims):
        super().__init__(code)
        self.claims = claims


def parse(token, key, now=None, max_lifetime_s=None) -> dict:
    """The claims of a token Main signed with ``key``, or ``ValueError(code)``: structure, signature, issuer,
    audience, issue time, lifetime and expiry. The context is not checked here."""
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
    tgt = claims["tgt"]
    if not isinstance(tgt, list) or len(tgt) > MAX_TARGETS \
            or not all(isinstance(p, int) and not isinstance(p, bool) for p in tgt):
        raise ValueError(INTERNAL_TOKEN_INVALID)
    for name in ("sub", "act", "ctx", "run", "tsk", "jti"):
        if not isinstance(claims[name], str) or not claims[name]:
            raise ValueError(INTERNAL_TOKEN_INVALID)
    moment = int(now if now is not None else time.time())
    if claims["iat"] > moment + MAX_CLOCK_SKEW_S or claims["exp"] <= claims["iat"]:
        raise TokenRejected(INTERNAL_TOKEN_INVALID, claims)
    if max_lifetime_s is not None and claims["exp"] - claims["iat"] > max_lifetime_s:
        raise TokenRejected(INTERNAL_TOKEN_INVALID, claims)
    if moment >= claims["exp"]:
        raise TokenRejected(INTERNAL_TOKEN_EXPIRED, claims)
    return claims


def _route_accepts(environ, endpoint, claims, view_args=None) -> Optional[str]:
    """``"mcp"`` on an internal MCP route, ``"inner"`` on the in-process API call of a verified one, ``"run"`` on a
    route of the run's own platform client (``RUN_ROUTES``), else None."""
    if environ.get(INTERNAL_MCP_ENVIRON_KEY) is True:
        verified = environ.get(VERIFIED_ENVIRON_KEY)
        return "inner" if isinstance(verified, dict) and verified.get("jti") == claims["jti"] else None
    if endpoint in MCP_ROUTE_ENDPOINTS:
        return "mcp"
    view_args = view_args or {}
    fixed = RUN_ROUTE_VALUES.get(endpoint)
    if environ.get("REQUEST_METHOD") in RUN_ROUTES.get(endpoint, ()) and view_args.get("mode") != ADMINISTRATION_MODE \
            and (endpoint != LLM_ENDPOINT or view_args.get("url") in LLM_PATHS) \
            and (fixed is None or view_args.get(fixed[0]) in fixed[1]):
        return "run"
    return None


def _run_route_project(environ, endpoint, view_args, args) -> Optional[int]:
    """The project a request of the run's own client acts in, as the route itself reads it: the path's
    ``project_id``, the S3 API's ``?project_id``, the LLM proxy's ``X-Project-Id`` (else ``OpenAI-Organization``).
    None when it names none or an unreadable one (refused: the proxy would pick the user's own project)."""
    if endpoint == LLM_ENDPOINT:
        value = environ.get("HTTP_X_PROJECT_ID") or environ.get("HTTP_OPENAI_ORGANIZATION")
    elif endpoint in S3_ENDPOINTS:
        value = (args or {}).get("project_id")
    else:
        value = (view_args or {}).get("project_id")
    try:
        return None if isinstance(value, bool) else int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _addressed_projects(route, view_args, args) -> list:
    """The projects a request acts in: the route's ``project_id`` and, on the MCP route, ``scope_project_id``."""
    found = []
    for value in ((view_args or {}).get("project_id"), (args or {}).get("scope_project_id") if route == "mcp" else None):
        if value is None:
            continue
        try:
            found.append(int(value))
        except (TypeError, ValueError):
            found.append(None)   # an unreadable project is not one of the token's
    return found


def _request_parts():
    try:
        import flask  # pylint: disable=C0415
        if flask.has_request_context():
            return flask.request.endpoint, flask.request.view_args, flask.request.args
    except Exception:  # pylint: disable=W0703
        pass
    return None, None, None


def _verdict_ttl() -> float:
    return max(0.0, min(VERDICT_TTL_S, float(getattr(auth, "epoch_ttl", VERDICT_TTL_S) or 0)))


def _cached_verdict(claims) -> bool:
    key = (claims["ctx"], claims["jti"])
    with _VERDICTS_LOCK:
        deadline = _VERDICTS.get(key)
        if deadline is not None and deadline > time.monotonic():
            return True
        _VERDICTS.pop(key, None)
    return False


def _remember_verdict(claims):
    ttl = _verdict_ttl()
    if ttl <= 0:
        return
    with _VERDICTS_LOCK:
        if len(_VERDICTS) >= MAX_VERDICTS:
            now = time.monotonic()
            for key in [key for key, deadline in _VERDICTS.items() if deadline <= now] or list(_VERDICTS)[:MAX_VERDICTS // 4]:
                _VERDICTS.pop(key, None)
        _VERDICTS[(claims["ctx"], claims["jti"])] = time.monotonic() + ttl


def _first_expired_refusal(claims) -> bool:
    """True the first time in ``EXPIRED_AUDIT_TTL_S`` an expired token of this (ctx, jti) is refused (it is then
    audited); bounded like the verdict cache."""
    key, now = (claims["ctx"], claims["jti"]), time.monotonic()
    with _VERDICTS_LOCK:
        until = _EXPIRED_AUDITED.get(key)
        if until is not None and until > now:
            return False
        if len(_EXPIRED_AUDITED) >= MAX_VERDICTS:
            for stale in [item for item, ends in _EXPIRED_AUDITED.items() if ends <= now] \
                    or list(_EXPIRED_AUDITED)[:MAX_VERDICTS // 4]:
                _EXPIRED_AUDITED.pop(stale, None)
        _EXPIRED_AUDITED[key] = now + EXPIRED_AUDIT_TTL_S
        return True


def _refuse_verified(module, claims, code):
    """A refusal of a token whose signature holds: counted and audited (the claims are Main's own). An expired token
    that was refused (and audited) a moment ago is only counted (``_first_expired_refusal``)."""
    if code == INTERNAL_TOKEN_EXPIRED and not _first_expired_refusal(claims):
        _count("deny", code)
        return _refusal(code)
    _count("deny", code, line=True)
    try:
        _audit(claims, "token", "-", "-", "deny", code, module=module)
    except Exception as exc:  # pylint: disable=W0703
        log.error("internal_obo: a token refusal was not recorded (%s)", type(exc).__name__)
        if code == INTERNAL_TOKEN_EXPIRED:   # not audited after all: the next refusal of this token writes the row
            with _VERDICTS_LOCK:
                _EXPIRED_AUDITED.pop((claims["ctx"], claims["jti"]), None)
    return _refusal(code)


def verify_request(token, environ, endpoint=None, module=None, now=None, view_args=None, args=None) -> dict:
    """The auth plugin's verifier of ``TOKEN_PREFIX`` bearers: ``{"ok": True, "user_id"}`` (and ``flask.g.internal_obo``
    set to the claims), or ``{"ok": False, "status", "code", "message"}``. Never raises, never logs the token.

    In order: signature and claims (a bad MAC is only counted: its claims are not Main's), the route, the projects the
    request addresses against ``tgt``, then the context: ``validate_context`` without moving its state, a positive verdict
    reused for ``VERDICT_TTL_S`` (bounded by the auth epoch TTL). A context that cannot be checked is 503, not a
    refusal of the run."""
    module = module or this.module
    if endpoint is None and view_args is None and args is None:
        endpoint, view_args, args = _request_parts()
    try:   # no token outlives the run cap; whether its run is still live is the context row's (#7229)
        claims = parse(token, signing_key(), now=now, max_lifetime_s=_max_run_lifetime_s() + MAX_CLOCK_SKEW_S)
    except TokenRejected as exc:
        return _refuse_verified(module, exc.claims, str(exc))
    except ValueError as exc:
        _count_unverified(str(exc))   # not Main's claims (or not readable ones): counted, not audited
        return _refusal(str(exc))
    route = _route_accepts(environ, endpoint, claims, view_args)
    if route is None:
        return _refuse_verified(module, claims, INTERNAL_TOKEN_INVALID) | {"status": 403}
    if route == "run":
        # the run's own client acts in the run's project only (#7229); ``tgt`` is for the internal MCP
        if _run_route_project(environ, endpoint, view_args, args) != claims["prj"]:
            return _refuse_verified(module, claims, INTERNAL_TARGET_DENIED)
    elif any(project not in claims["tgt"] for project in _addressed_projects(route, view_args, args)):
        return _refuse_verified(module, claims, INTERNAL_TARGET_DENIED)
    if route in ("mcp", "run") and not _cached_verdict(claims):
        from .admission import validate_context  # pylint: disable=C0415
        caller = {"kind": "indexer_worker", "task_name": claims["tsk"], "run_id": claims["run"]}
        verdict = validate_context(module, claims["prj"], claims["ctx"], caller, touch=False)
        if not isinstance(verdict, dict) or not verdict.get("ok"):
            code = DEPENDENCY_UNAVAILABLE if isinstance(verdict, dict) \
                and verdict.get("reason_code") == DEPENDENCY_UNAVAILABLE else CONTEXT_NOT_LIVE
            _count("deny", code, line=True)   # validate_context recorded the decision itself
            return _refusal(code)
        _remember_verdict(claims)
    _count("allow", "VERIFIED")
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

def endpoint_key(path) -> str:
    """A path template as the map keys it: without the ``/api/v2/`` prefix and surrounding slashes."""
    text = str(path or "").strip("/")
    return text[len("api/v2/"):] if text.startswith("api/v2/") else text


def operation_of(method, path) -> Optional[str]:
    """The platform operation of an API tool, from its method and path template: ``read`` for a GET (``credentials``
    for a read that returns a secret, #7228), the explicit map for a write, None for a write the map does not know
    (refused)."""
    verb = str(method or "").upper()
    if verb in READ_METHODS:
        return "credentials" if material_of(verb, path) in (MATERIAL_OUT, MATERIAL_UNREVIEWED, MATERIAL_UNKNOWN) \
            else "read"
    return ENDPOINT_OPERATIONS.get((verb, endpoint_key(path)))


# --- credential material (#7228) --------------------------------------------------------------------------------

def _secret_typed(schema, components=None) -> bool:
    """Does a schema hold a secret-typed field anywhere (``format: password`` or ``writeOnly``), ``$ref`` into
    ``components`` followed? Bounded: a walk past ``MAX_SCHEMA_WALK`` nodes counts as secret."""
    components, nodes, seen = components or {}, [0], set()

    def walk(node):
        nodes[0] += 1
        if nodes[0] > MAX_SCHEMA_WALK:
            return True
        if isinstance(node, dict):
            if node.get("format") == "password" or node.get("writeOnly") is True:
                return True
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/components/schemas/"):
                name = ref.rsplit("/", 1)[1]
                if name not in seen:
                    seen.add(name)
                    if walk(components.get(name)):
                        return True
            return any(walk(value) for key, value in node.items() if key != "$ref")
        if isinstance(node, list):
            return any(walk(value) for value in node)
        return False

    return walk(schema)


def _secret_named(schema, components=None) -> list:
    """The property names of a schema (``$ref`` followed, bounded) that look like a secret (``SECRET_NAME``) and are not
    reviewed as harmless (``REVIEWED_FIELD_NAMES``): a plain-text secret the schema cannot type."""
    components, nodes, seen, found = components or {}, [0], set(), set()

    def walk(node):
        nodes[0] += 1
        if nodes[0] > MAX_SCHEMA_WALK:
            found.add("<schema too large>")
            return
        if isinstance(node, dict):
            for name in (node.get("properties") or {}) if isinstance(node.get("properties"), dict) else ():
                if isinstance(name, str) and SECRET_NAME.search(name) and name not in REVIEWED_FIELD_NAMES:
                    found.add(name)
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/components/schemas/"):
                name = ref.rsplit("/", 1)[1]
                if name not in seen:
                    seen.add(name)
                    walk(components.get(name))
            for key, value in node.items():
                if key != "$ref":
                    walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(schema)
    return sorted(found)


def schema_material(request_schema, response_schema, components=None) -> str:
    """The class a route's own schemas give it: ``material_in`` / ``material_out`` (a secret-typed field),
    ``unreviewed`` (a field named like a secret that no one classified: refused to a run token until a line says what
    it is), ``none``."""
    if request_schema is not None and _secret_typed(request_schema, components):
        return MATERIAL_IN
    if response_schema is not None and _secret_typed(response_schema, components):
        return MATERIAL_OUT
    if any(schema is not None and _secret_named(schema, components) for schema in (request_schema, response_schema)):
        return MATERIAL_UNREVIEWED
    return MATERIAL_NONE


def _operation_schemas(operation) -> tuple:
    operation = operation if isinstance(operation, dict) else {}
    body = (((operation.get("requestBody") or {}).get("content") or {}).get("application/json") or {}).get("schema")
    answer = (((((operation.get("responses") or {}).get("200") or {}).get("content") or {})
               .get("application/json") or {}).get("schema"))
    return body, answer


_MATERIALS = {}   # id(spec) -> {(METHOD, key): class}: the registry hands out the same spec until it changes


def _registry_materials() -> dict:
    """``{(METHOD, endpoint key): class}`` of every route of the OpenAPI registry, from its schemas (cached per spec)."""
    from tools import openapi_registry  # pylint: disable=C0415
    spec = openapi_registry.get_combined_spec(full=True)
    cached = _MATERIALS.get("spec")
    if cached is not None and cached[0] is spec:
        return cached[1]
    components = (spec.get("components") or {}).get("schemas") or {}
    found = {}
    for path, operations in (spec.get("paths") or {}).items():
        for method, operation in (operations or {}).items():
            if isinstance(operation, dict):
                found[(method.upper(), endpoint_key(path))] = schema_material(*_operation_schemas(operation), components)
    _MATERIALS["spec"] = (spec, found)
    return found


def material_of(method, path) -> str:
    """The material class of a route: its declared line, else its registry schemas. A registry that cannot be read is
    ``unknown`` (fails closed: refused and not listed to a run token)."""
    key = (str(method or "").upper(), endpoint_key(path))
    if key in DECLARED_MATERIAL:
        return DECLARED_MATERIAL[key]
    try:
        return _registry_materials().get(key, MATERIAL_NONE)
    except Exception as exc:  # pylint: disable=W0703
        log.warning("internal_obo: the registry schemas could not be read (%s); material assumed", type(exc).__name__)
        return MATERIAL_UNKNOWN


def _configuration_holds_secret(config_type, data) -> bool:
    """Does a configuration body's ``data`` carry a value at a secret field of its type? A type the configurations
    plugin does not know, or data the walker refuses, counts as carrying one."""
    from .credential_decision import CredentialDecisionError  # pylint: disable=C0415
    from .credential_slots import configuration_secret_fields, strip_secret_fields  # pylint: disable=C0415
    if not isinstance(data, dict) or not data:
        return False
    if not isinstance(config_type, str) or not config_type:
        return True
    paths = configuration_secret_fields([config_type]).get(config_type)
    if not isinstance(paths, (list, tuple)):
        return True
    try:
        return bool(strip_secret_fields(copy.deepcopy(data), paths))
    except CredentialDecisionError:
        return True


def _stored_configuration_type(project_id, config_id):
    from tools import rpc_tools  # pylint: disable=C0415
    found = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_by_id(int(project_id), int(config_id))
    return (found or {}).get("type")


def _plain_value(value) -> bool:
    """A value typed in: a non-empty string that is not a ``{{secret.…}}`` reference (a selector is a dict)."""
    return isinstance(value, str) and bool(value.strip()) and not value.strip().startswith(SECRET_REFERENCE)


def _toolkit_secret_names(tool_type, project_id, user_id):
    """``(names, known)``: the fields of a toolkit type that hold a secret (``secret: true`` or ``format: password`` in
    its schema), and whether the type has a schema at all."""
    from .application_tools import find_toolkit_schema_by_type_everywhere  # pylint: disable=C0415
    schema, _external = find_toolkit_schema_by_type_everywhere(tool_type, project_id, user_id, {})
    properties = (schema or {}).get("properties") or {}
    names = {name for name, spec in properties.items() if isinstance(spec, dict) and (
        spec.get("secret") is True or spec.get("format") == "password"
        or any(isinstance(option, dict) and option.get("format") == "password" for option in spec.get("anyOf") or ()))}
    return names, bool(schema)


def toolkit_holds_secret(tool_type, settings, project_id, user_id) -> bool:
    """Does a toolkit's ``settings`` carry a typed secret: a value at a secret field of its type, or in a header-like
    map (``DICT_SECRET_FIELDS``)? References (``{{secret.…}}``) and configuration selectors (dicts) are not. A type with
    no schema counts as carrying one when any setting holds a typed string."""
    from .credential_slots import DICT_SECRET_FIELDS  # pylint: disable=C0415
    if not isinstance(settings, dict) or not settings:
        return False
    for name in DICT_SECRET_FIELDS:
        found = settings.get(name)
        if isinstance(found, str):
            try:
                found = json.loads(found) if found.strip() else {}
            except ValueError:
                return True
        if isinstance(found, dict) and any(_plain_value(value) for value in found.values()):
            return True
    names, known = _toolkit_secret_names(tool_type, project_id, user_id) if isinstance(tool_type, str) and tool_type \
        else (set(), False)
    if not known:
        return any(_plain_value(value) for value in settings.values())
    return any(_plain_value(settings.get(name)) for name in names)


def _pipeline_holds_secret(pipeline_settings) -> bool:
    """A version's ``pipeline_settings`` with a typed webhook secret (``PIPELINE_SECRET_KEYS`` at any depth, bounded)."""
    nodes = [0]

    def walk(node):
        nodes[0] += 1
        if nodes[0] > MAX_SCHEMA_WALK:
            return True
        if isinstance(node, dict):
            return any((key in PIPELINE_SECRET_KEYS and _plain_value(value)) or walk(value) for key, value in node.items())
        if isinstance(node, list):
            return any(walk(value) for value in node)
        return False

    return walk(pipeline_settings)


def _version_holds_secret(version, project_id, user_id) -> bool:
    version = version if isinstance(version, dict) else {}
    if _pipeline_holds_secret(version.get("pipeline_settings")):
        return True
    tools = version.get("tools") if isinstance(version.get("tools"), list) else []
    return any(isinstance(tool, dict) and toolkit_holds_secret(tool.get("type"), tool.get("settings"), project_id, user_id)
               for tool in tools)


def _stored_toolkit_type(project_id, tool_id):
    from ..models.elitea_tools import EliteATool  # pylint: disable=C0415
    with db.get_session(int(project_id)) as session:
        return session.execute(select(EliteATool.type).where(EliteATool.id == int(tool_id))).scalar_one_or_none()


def material_of_call(method, path, path_params, body, user_id=None) -> bool:  # pylint: disable=R0911
    """Is this call of an MCP API tool credential material? Its route's class; a ``per_request`` route is decided from
    the call's own body: a configuration's ``data`` with a value at a secret field of its type (the body's ``type`` on
    create, the stored type on update; a model definition that points at an existing credential is not), a toolkit's
    ``settings`` with a typed secret (``toolkit_holds_secret``), a version's (or an agent's first versions')
    ``pipeline_settings`` webhook secret or toolkit settings, an agent's ``webhook_secret``. Anything that cannot be
    decided counts as material."""
    found = material_of(method, path)
    if found != MATERIAL_PER_REQUEST:
        return found in (MATERIAL_IN, MATERIAL_OUT, MATERIAL_UNREVIEWED, MATERIAL_UNKNOWN)
    body = body if isinstance(body, dict) else {}
    key = (str(method or "").upper(), endpoint_key(path))
    path_params = path_params or {}
    project_id = path_params.get("project_id")
    try:
        if key in TOOLKIT_WRITES:
            tool_type = body.get("type") or (_stored_toolkit_type(project_id, path_params.get("tool_id"))
                                             if path_params.get("tool_id") is not None else None)
            return toolkit_holds_secret(tool_type, body.get("settings"), project_id, user_id)
        if key in VERSION_WRITES:
            return _version_holds_secret(body, project_id, user_id)
        fields = PER_REQUEST_FIELDS.get(key)
        if fields is not None:
            if any(body.get(name) not in (None, "", {}) for name in fields):
                return True
            versions = body.get("versions") if isinstance(body.get("versions"), list) else []
            return any(_version_holds_secret(version, project_id, user_id) for version in versions)
        data = body.get("data")
        if not isinstance(data, dict) or not data:
            return False
        if key[0] == "POST":
            config_type = body.get("type")
        else:
            config_type = _stored_configuration_type(project_id, path_params.get("config_id"))
        return _configuration_holds_secret(config_type, data)
    except Exception as exc:  # pylint: disable=W0703
        log.warning("internal_obo: a request body could not be classified (%s); refused", type(exc).__name__)
        return True


def listed_for_run(method, path) -> bool:
    """Is an MCP API tool listed to a run token? Not when it can only ever be material (#7228), or is not reviewed, or
    the registry cannot be read: the model is not shown a tool it may never call. ``per_request`` routes stay."""
    return material_of(method, path) not in (MATERIAL_IN, MATERIAL_OUT, MATERIAL_UNREVIEWED, MATERIAL_UNKNOWN)


def _redact_settings(settings, tool_type, project_id, user_id) -> dict:
    from .credential_slots import DICT_SECRET_FIELDS, GOVERNED_VALUE  # pylint: disable=C0415
    try:
        names = _toolkit_secret_names(tool_type, project_id, user_id)[0] if isinstance(tool_type, str) else set()
    except Exception:  # pylint: disable=W0703
        names = set()
    cleaned = dict(settings)
    for name, value in settings.items():
        if name in DICT_SECRET_FIELDS:
            if isinstance(value, dict):
                cleaned[name] = {key: GOVERNED_VALUE if _plain_value(item) else item for key, item in value.items()}
            elif isinstance(value, str) and value.strip():
                cleaned[name] = GOVERNED_VALUE
        elif _plain_value(value) and (name in names or (SECRET_NAME.search(name) and name not in REVIEWED_FIELD_NAMES)):
            cleaned[name] = GOVERNED_VALUE
    return cleaned


def redact_for_run(answer, project_id, user_id):
    """(#7228, C9-SEC-94) A read's answer as a run token gets it: every toolkit in it (``{type, settings}``) with the
    values at its type's secret fields, at fields named like a secret and in every header-like map replaced by the
    validator marker; references and selectors kept. Bounded walk; the answer is not changed in place."""
    nodes = [0]

    def walk(node):
        nodes[0] += 1
        if nodes[0] > MAX_REDACT_NODES:
            raise ValueError("answer too large to redact")
        if isinstance(node, dict):
            result = {key: walk(value) for key, value in node.items()}
            if isinstance(node.get("type"), str) and isinstance(node.get("settings"), dict):
                result["settings"] = _redact_settings(result["settings"], node["type"], project_id, user_id)
            return result
        if isinstance(node, list):
            return [walk(value) for value in node]
        return node

    return walk(answer)


class OperationRefused(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.code = code
        self.message = MESSAGES[code]


def _audit(claims, operation, target, tool_name, decision, code, module=None):
    subject = claims["sub"]
    event = {
        "event_type": "platform_operation", "decision": decision, "reason_code": code,
        "project_id": claims["prj"], "caller_ref": f"user:{claims['uid']}", "user_ref": subject,
        "mandate_ref": subject[len("mandate:"):] if subject.startswith("mandate:") else None,
        "agent_binding_ref": claims["act"], "runtime_identity_ref": f"context:{claims['ctx']}",
        "run_id": claims["run"], "operation_id": f"{operation}:{tool_name}"[:256], "target_ref": str(target)[:256],
    }
    module = module or this.module
    profile = getattr(module, "governance_profile", None)
    settings = getattr(module, "admission_settings", None)
    event["policy_revision"] = getattr(profile, "policy_revision", None)
    event["environment"] = getattr(settings, "environment", None)
    this.for_module("tracing").module.security_audit_record_autocommit(
        {key: value for key, value in event.items() if value is not None})


def check_operation(operation, target, tool_name, material=False) -> None:
    """Refuse (``OperationRefused``) an operation outside the run token's scope, or a write the map does not know
    (``operation`` None), and record a write before it is made. A request without a run token is not touched. A refusal
    is recorded too; a write whose record cannot be written is refused (``AUDIT_UNAVAILABLE``). Reads in scope are not
    recorded. ``material`` (#7228): the call writes or returns a credential value (``material_of_call``); in scope or
    not, it never runs on a run token (``CREDENTIALS_VIA_FORM``, recorded)."""
    claims = current_claims()
    if claims is None:
        return
    if operation is None or operation not in claims["scope"] or material:
        code = INTERNAL_OPERATION_UNKNOWN if operation is None else (
            INTERNAL_SCOPE_DENIED if operation not in claims["scope"] else CREDENTIALS_VIA_FORM)
        try:
            _audit(claims, operation or "unknown", target, tool_name, "deny", code)
        except Exception as exc:  # pylint: disable=W0703
            log.error("internal_obo: a scope refusal was not recorded (%s)", type(exc).__name__)
        raise OperationRefused(code)
    if operation == "read":
        return
    try:
        _audit(claims, operation, target, tool_name, "allow", PLATFORM_OPERATION)
    except Exception as exc:  # pylint: disable=W0703
        log.error("internal_obo: a platform operation could not be recorded (%s); refused", type(exc).__name__)
        raise OperationRefused(AUDIT_UNAVAILABLE) from None


def projects_named_in(values) -> list:
    """Every value of a ``project_id`` key in a tool call's arguments, at any depth (nested JSON given as a string is
    read too, as the call decodes it). ``ValueError`` when the arguments are deeper or larger than the walk allows, or
    a project id is not a number: the call is then refused rather than half-checked."""
    found, nodes = [], [0]

    def walk(value, depth):
        nodes[0] += 1
        if depth > MAX_BODY_DEPTH or nodes[0] > MAX_BODY_NODES:
            raise ValueError("arguments too deep or too large")
        if isinstance(value, str) and value[:1] in ("{", "[") and len(value) <= MAX_NESTED_JSON_CHARS:
            try:
                value = json.loads(value)
            except ValueError:
                return
        if isinstance(value, dict):
            for key, item in value.items():
                if key == "project_id" and item is not None and not isinstance(item, (dict, list)):
                    if isinstance(item, bool) or not str(item).strip().isdigit():
                        raise ValueError("project_id is not a number")
                    found.append(int(str(item).strip()))
                else:
                    walk(item, depth + 1)
        elif isinstance(value, list):
            for item in value:
                walk(item, depth + 1)

    walk(values, 0)
    return found


def check_targets(values, target, tool_name) -> None:
    """Refuse (``INTERNAL_TARGET_DENIED``) a call under a run token whose arguments name a project outside the
    token's ``tgt`` (e.g. a participant's ``entity_meta.project_id``); recorded. No run token: nothing is read."""
    claims = current_claims()
    if claims is None:
        return
    try:
        outside = [project for project in projects_named_in(values) if project not in claims["tgt"]]
    except ValueError:
        outside = [None]
    if not outside:
        return
    try:
        _audit(claims, "target", target, tool_name, "deny", INTERNAL_TARGET_DENIED)
    except Exception as exc:  # pylint: disable=W0703
        log.error("internal_obo: a target refusal was not recorded (%s)", type(exc).__name__)
    raise OperationRefused(INTERNAL_TARGET_DENIED)


def unmapped_writes(api_tools) -> list:
    """The MCP-exposed writes (``openapi_registry.get_mcp_api_tools()``) the operation map does not know: each is
    refused at run time. Checked at startup so a map that lags the registry shows up in the log, not in a chat."""
    missing = []
    for tool in api_tools or []:
        method = str(tool.get("method", "")).upper()
        if method and method not in READ_METHODS and operation_of(method, tool.get("path")) is None:
            missing.append(f"{method} {endpoint_key(tool.get('path'))}")
    return sorted(set(missing))


def record_would_deny_material(module, project_id, user_id, target, tool_name) -> None:
    """Audit only (#7228): a credential-material call on the internal MCP, which enforce refuses on a run token, is
    recorded as would-deny ``CREDENTIALS_VIA_FORM``; the call goes on. Never raises."""
    profile = getattr(module, "governance_profile", None)
    settings = getattr(module, "admission_settings", None)
    event = {"event_type": "platform_operation", "decision": "allow", "reason_code": CREDENTIALS_VIA_FORM,
             "would_deny": True, "project_id": project_id if isinstance(project_id, int) else None,
             "caller_ref": f"user:{user_id}" if isinstance(user_id, int) else None,
             "operation_id": f"credentials:{tool_name}"[:256], "target_ref": str(target)[:256],
             "policy_revision": getattr(profile, "policy_revision", None),
             "environment": getattr(settings, "environment", None)}
    try:
        this.for_module("tracing").module.security_audit_record_autocommit(
            {key: value for key, value in event.items() if value is not None})
    except Exception as exc:  # pylint: disable=W0703
        log.error("internal_obo: a would-deny was not recorded (%s)", type(exc).__name__)


def unclassified_material(api_tools) -> list:
    """The MCP-exposed routes (``openapi_registry.get_mcp_api_tools()``) that need a reviewer: a schema field named like
    a secret that no line classifies (``unreviewed``; refused to a run token meanwhile), a registry that cannot be read,
    or a secret-typed field on a route whose operation is not ``credentials``. Checked at startup beside
    ``unmapped_writes``."""
    found = []
    for tool in api_tools or []:
        method = str(tool.get("method", "")).upper()
        if not method:
            continue
        material = material_of(method, tool.get("path"))
        if material in (MATERIAL_UNREVIEWED, MATERIAL_UNKNOWN) or (
                material in (MATERIAL_IN, MATERIAL_OUT) and operation_of(method, tool.get("path")) != "credentials"):
            found.append(f"{method} {endpoint_key(tool.get('path'))}")
    return sorted(set(found))


def refusal_text(exc: OperationRefused) -> str:
    """What the model and the chat see for a refused tool call: the code, then the fixed readable text."""
    return f"{exc.code}: {exc.message}"


# --- what a payload carries ------------------------------------------------------------------

# The internal endpoint shape, on the URL's path as the client sends it (``client_path``; the SDK matches the same):
# /app/<project>/mcp, then / or nothing
_INTERNAL_PATH = re.compile(r"^/app/(?P<project>\d+|\{project_id\})/mcp(?:/|$)")
_AUTH_HEADERS = frozenset({"authorization", "cookie", "proxy-authorization"})


class ForeignInternalMcp(Exception):
    """An internal-shaped MCP toolkit on an origin the platform does not serve its tools from."""

    def __init__(self):
        super().__init__(INTERNAL_MCP_ORIGIN_NOT_ALLOWED)
        self.code = INTERNAL_MCP_ORIGIN_NOT_ALLOWED


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
                # Main builds the entry as {name, description, version_details} (subagent_prefetch)
                if isinstance(value.get("version_details"), dict):
                    yield value["version_details"].get("tools")


def client_path(path) -> str:
    """A URL path as an HTTP client and the server end up seeing it: percent-decoded (repeatedly, at most 3 times),
    backslashes as slashes, repeated slashes collapsed, ``.`` and ``..`` resolved. Matching the internal shape on this,
    not on the text of the setting, is what keeps ``/x/../app/2/mcp`` or ``/app/2/%6Dcp`` from passing unrecognised."""
    text = str(path or "")
    for _ in range(3):
        decoded = unquote(text)
        if decoded == text:
            break
        text = decoded
    text = re.sub(r"/{2,}", "/", "/" + text.replace("\\", "/").lstrip("/"))
    resolved = posixpath.normpath(text)
    resolved = "/" + resolved.lstrip("/")
    return resolved + "/" if text.endswith("/") and resolved != "/" else resolved


def _all_mcp_tools(payload):
    """``(tool, settings, url parts, internal shape match or None)`` of every MCP toolkit with a URL."""
    for tools in _tool_lists(payload):
        for tool in tools if isinstance(tools, list) else ():
            if not isinstance(tool, dict) or not str(tool.get("type", "")).startswith("mcp"):
                continue
            settings = tool.get("settings")
            url = settings.get("url") if isinstance(settings, dict) else None
            if not isinstance(url, str):
                continue
            parts = urlsplit(url)
            yield tool, settings, parts, _INTERNAL_PATH.match(client_path(parts.path))


def _mcp_tools(payload):
    """``(tool, settings, url parts)`` of every MCP toolkit whose URL has the internal endpoint shape."""
    for tool, settings, parts, match in _all_mcp_tools(payload):
        if match:
            yield tool, settings, parts


def has_internal_mcp(payload) -> bool:
    """Does the payload carry an internal Elitea MCP toolkit (its URL has the internal endpoint shape)? Dict reads."""
    return next(_mcp_tools(payload), None) is not None


def may_fetch_subagents(payload) -> bool:
    """Can the runtime fetch a sub-agent's details itself (the prefetch did not cover it)? Then its internal toolkits,
    injected by Main into that response, are not visible here, and the run still gets a token. Dict reads."""
    application = payload.get("application") if isinstance(payload, dict) else None
    prefetched = (application or {}).get("subagent_version_details") if isinstance(application, dict) else None
    prefetched = prefetched if isinstance(prefetched, dict) else {}
    for tools in _tool_lists(payload):
        for tool in tools if isinstance(tools, list) else ():
            if not isinstance(tool, dict) or tool.get("type") != "application":
                continue
            settings = tool.get("settings") if isinstance(tool.get("settings"), dict) else {}
            if f"{settings.get('application_id')}:{settings.get('application_version_id')}" not in prefetched:
                return True
    return False


CREDENTIAL_FORM_SETTING = "credential_form"


def mark_credential_form(payload, allowed) -> None:
    """(#7228) The internal MCP toolkits of a run whose token scope holds ``credentials`` carry
    ``credential_form: true``: the SDK then gives the model the shape-only ``request_credential`` tool (the person types
    the value in the chat's credential form, never the model). Any other MCP toolkit, or a run without that scope, has the
    key removed, whatever its stored settings said. Called where the run token is minted (enforce only)."""
    origins = set(internal_origins()) if allowed else set()
    for tool, settings, parts, match in list(_all_mcp_tools(payload)):
        wanted = bool(allowed and match and _origin_of(parts) in origins)
        if settings.get(CREDENTIAL_FORM_SETTING, False) is wanted and (wanted or CREDENTIAL_FORM_SETTING not in settings):
            continue
        new_settings = {key: value for key, value in settings.items() if key != CREDENTIAL_FORM_SETTING}
        if wanted:
            new_settings[CREDENTIAL_FORM_SETTING] = True
        tool["settings"] = new_settings


def needs_run_token(payload) -> bool:
    """A run token is minted only for a run that can reach the internal MCP: an internal toolkit in the payload, or a
    sub-agent the runtime may fetch itself (its details carry Main-injected internal toolkits)."""
    return has_internal_mcp(payload) or may_fetch_subagents(payload)


def _private_project(user_id) -> Optional[int]:
    """The user's private project (where Main injects most internal toolkits), for a sub-agent fetched at run time."""
    try:
        from tools import rpc_tools  # pylint: disable=C0415
        project = rpc_tools.RpcMixin().rpc.timeout(5).admin_get_user_private_project(user_id)
        return int(project.id) if project else None
    except Exception as exc:  # pylint: disable=W0703
        log.warning("internal_obo: the private project could not be read (%s)", type(exc).__name__)
        return None


def target_projects(payload, project_id, user_id) -> list:
    """``tgt``: the projects the run's internal toolkits address (each URL's project and ``scope_project_id``; the
    tool's own project for a template URL). A run that may fetch sub-agents also gets its own project and the user's
    private project, where Main injects those sub-agents' toolkits."""
    found = set()
    for tool, settings, parts in _mcp_tools(payload):
        project = _INTERNAL_PATH.match(client_path(parts.path)).group("project")
        if project.isdigit():
            found.add(int(project))
        else:
            owner = settings.get("project_id") or tool.get("project_id") or project_id
            if isinstance(owner, int) or (isinstance(owner, str) and owner.isdigit()):
                found.add(int(owner))
        for value in parse_qs(parts.query).get("scope_project_id", []):
            if value.isdigit():
                found.add(int(value))
    if may_fetch_subagents(payload):
        found.add(int(project_id))
        private = _private_project(user_id)
        if private is not None:
            found.add(private)
    return sorted(found)


def _origin_of(parts) -> str:
    return f"{parts.scheme}://{parts.netloc}".lower()


def _auth_header_keys(headers) -> list:
    if isinstance(headers, str):
        try:
            headers = json.loads(headers) if headers.strip() else {}
        except ValueError:
            return ["unreadable"]
    if not isinstance(headers, dict):
        return []
    return [key for key in headers if isinstance(key, str) and key.lower() in _AUTH_HEADERS]


def inspect_internal_mcp(payload) -> list:
    """Audit only: what ``police_internal_mcp`` would do under enforce, as would-deny codes, without changing anything.
    ``INTERNAL_MCP_ORIGIN_NOT_ALLOWED`` for an internal-shaped toolkit on another origin; ``INTERNAL_MCP_OWN_AUTH`` for
    a toolkit on the platform's own origin that brings its own credentials on a path that is not the plain internal
    shape (a disguised path, or another route of the platform). The plain injected toolkits are not reported here: their
    personal token is what ``LEGACY_PAT_INTERNAL_MCP`` already reports."""
    allowed, codes = set(internal_origins()), []
    for _tool, settings, parts, match in _all_mcp_tools(payload):
        on_platform = _origin_of(parts) in allowed
        if match and not on_platform:
            codes.append(INTERNAL_MCP_ORIGIN_NOT_ALLOWED)
        elif on_platform and (_auth_header_keys(settings.get("headers")) or "personal_token" in settings) \
                and not _INTERNAL_PATH.match(parts.path):
            codes.append(INTERNAL_MCP_OWN_AUTH)
    return sorted(set(codes))


def police_internal_mcp(payload) -> int:
    """Under enforce, before dispatch: an MCP toolkit may reach the platform only with the run token. Any MCP toolkit on
    the platform's own origins, whatever its path, loses its own ``Authorization``/``Cookie``/``Proxy-Authorization``
    headers and personal token (the run token replaces them on the internal routes); an internal-shaped one (after
    ``client_path``) on another origin raises ``ForeignInternalMcp``. The number of toolkits changed."""
    allowed = set(internal_origins())
    changed = 0
    for tool, settings, parts, match in list(_all_mcp_tools(payload)):
        on_platform = _origin_of(parts) in allowed
        if match and not on_platform:
            raise ForeignInternalMcp()
        if not on_platform:
            continue
        headers = settings.get("headers")
        if isinstance(headers, str):
            try:
                headers = json.loads(headers) if headers.strip() else {}
            except ValueError:
                headers = {}
        cleaned = {key: value for key, value in (headers or {}).items()
                   if not (isinstance(key, str) and key.lower() in _AUTH_HEADERS)} if isinstance(headers, dict) else {}
        if cleaned != settings.get("headers") or "personal_token" in settings:
            new_settings = dict(settings)
            new_settings["headers"] = cleaned
            new_settings.pop("personal_token", None)
            tool["settings"] = new_settings
            changed += 1
    return changed
