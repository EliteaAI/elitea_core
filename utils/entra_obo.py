"""External on-behalf-of exchange for Entra-delegated credentials (#6756, C-1009 step 4).

A credential reference of kind ``entra_delegated`` holds no value: it is a descriptor of which app to exchange through,
for which tenant, resource and scopes. At credential release Main asks auth_oidc for the run's person's Entra-issued
access token (the custody of their Microsoft sign-in), exchanges it at Entra for a token of the descriptor's resource
on behalf of that person (OAuth 2.0 on-behalf-of), and hands that token to the worker as the slot's value through the
sealed release reply. The exchanged token is kept only in this process's memory, for its own lifetime: never in a store,
a table, a payload, an event or a log line.

The app client comes through one seam, ``app_client``: an admin-managed OAuth app (#7149, ``resolve_app_client``)
when the descriptor names one, or the platform's own API client (``oauth_app_id = "platform"``) configured on this
plugin, its secret read from the environment.

Every check fails closed and answers a fixed code (``DelegationRefused``); nothing here repeats a dependency's text.
"""
import base64
import binascii
import hashlib
import hmac
import json
import os
import secrets
import re
import threading
import time
from typing import Optional
from urllib.parse import urlsplit

import requests
from pylon.core.tools import log

GRAPH_RESOURCE = "https://graph.microsoft.com"
AUTHORITY_HOST = "login.microsoftonline.com"
PLATFORM_APP = "platform"                  # the interim app: the platform's own API client (env-configured)
DEFAULT_SECRET_ENV = "ELITEA_API_CLIENT_SECRET"
CREDENTIAL_TYPE = "sharepoint"             # the credential type an admin app must serve (#7149)
OBO_GRANT = "urn:ietf:params:oauth:grant-type:jwt-bearer"
ASSERTION_RPC = "auth_oidc_obo_assertion"
HTTP_TIMEOUT_S = 5                         # read timeout of one POST, never more than the time left
CONNECT_TIMEOUT_S = 3
RPC_TIMEOUT_S = 5
# One deadline per release request, every delegated slot of it included: the worker waits WORKER_WAIT_S plus
# WORKER_WAIT_PER_SLOT_S per slot for the reply (indexer_worker credential_resolver), Main keeps MARGIN_S of it
WORKER_WAIT_S, WORKER_WAIT_PER_SLOT_S, MARGIN_S = 15, 0.5, 3
BUDGET_S = WORKER_WAIT_S - MARGIN_S        # the budget of a request of one slot
MIN_CALL_S = 2                             # a call is started only with this much left
RETRY_NEEDS_S = 4                          # the refresh and second exchange are tried only with this much left
MIN_REMAINING_S = 900                      # a cached token is served only with at least this much life left
DEFAULT_CUSTODY_KEY_ENV = "ELITEA_OBO_CUSTODY_KEY"   # shared with auth_oidc only: signs each assertion request
MIN_CUSTODY_KEY_CHARS = 32
SLOT_MARGIN_S = 60                         # the slot stops being usable this long before the token does
MAX_CACHE_ENTRIES = 1024
PROBE_TTL_S = 60
MAX_TOKEN_CHARS = 16_384
ENTRA_ISSUER = re.compile(r"^https://(login\.microsoftonline\.com|sts\.windows\.net)/[0-9a-fA-F-]{36}(/v2\.0)?/?$")
ASSERTION_SOURCES = frozenset({"login", "connected_account"})
_GUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def request_budget(slots) -> float:
    """Seconds one release request of ``slots`` slots may spend on its exchanges, together."""
    return WORKER_WAIT_S + WORKER_WAIT_PER_SLOT_S * max(1, int(slots)) - MARGIN_S


def deadline_for(slots) -> float:
    """The ``time.monotonic()`` by which every exchange of a release request of ``slots`` slots must be done."""
    return time.monotonic() + request_budget(slots)

# codes (the messages live in admission.MESSAGES)
SUBJECT_MISMATCH = "DENIED_SUBJECT_MISMATCH"
MANDATE_UNSUPPORTED = "DENIED_MANDATE_DELEGATION_UNSUPPORTED"
CALLER_UNSUPPORTED = "DENIED_DELEGATION_UNSUPPORTED_CALLER"
ASSERTION_UNAVAILABLE = "DENIED_ASSERTION_UNAVAILABLE"
SOURCE_UNSUPPORTED = "DENIED_ASSERTION_SOURCE_UNSUPPORTED"
APP_UNAVAILABLE = "DENIED_OAUTH_APP_UNAVAILABLE"
EXCHANGE_FAILED = "DELEGATION_EXCHANGE_FAILED"
UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
STALE = "STALE_CONTEXT"
# a mandate run acting with its owner's offline consent (#7131): none, withdrawn, or refused by Microsoft
CONSENT_REQUIRED = "DENIED_MANDATE_CONSENT_REQUIRED"
CONSENT_EXPIRED = "DENIED_MANDATE_CONSENT_EXPIRED"
CODES = (SUBJECT_MISMATCH, MANDATE_UNSUPPORTED, CALLER_UNSUPPORTED, ASSERTION_UNAVAILABLE, SOURCE_UNSUPPORTED,
         APP_UNAVAILABLE, EXCHANGE_FAILED, CONSENT_REQUIRED, CONSENT_EXPIRED)
OFFLINE_RPC = "auth_oidc_obo_offline_assertion"


class DelegationRefused(Exception):
    """The delegated value cannot be produced; ``code`` says why (no secret, no dependency text)."""

    def __init__(self, code):
        super().__init__(code)
        self.code = code


# --- the subject of the run ------------------------------------------------------------------

def run_subject(subject, tenant, caller) -> dict:
    """``{issuer, tid, oid}`` of the person a run acts for, from the explicit fields of its context row; refused when
    the run is not a signed-in person's (a mandate, an API token, a system run) or the fields are not there (a row
    admitted before they were stored). ``subject.ref`` is never parsed."""
    subject = subject if isinstance(subject, dict) else {}
    if subject.get("kind") == "mandate":
        raise DelegationRefused(MANDATE_UNSUPPORTED)
    if subject.get("kind") != "delegation" or not isinstance(caller, dict) or caller.get("kind") != "user":
        raise DelegationRefused(CALLER_UNSUPPORTED)
    found = {"issuer": subject.get("issuer"), "tid": tenant, "oid": subject.get("oid")}
    if not all(isinstance(value, str) and value for value in found.values()):
        raise DelegationRefused(STALE)   # admitted before the explicit fields were stored: start the run again
    return found


def bind_subject(person: dict, delegation: dict) -> None:
    """The reference's descriptor and custody must be the run's person's: its tenant, and for a personal reference its
    custodian. Raises ``DelegationRefused``."""
    if delegation.get("tenant_id", "").lower() != person["tid"].lower():
        raise DelegationRefused(SUBJECT_MISMATCH)
    if delegation.get("ownership_class") == "personal" and \
            delegation.get("custodian_subject_ref") != f"idp:{person['tid']}:{person['oid']}":
        raise DelegationRefused(SUBJECT_MISMATCH)


def _claims(token) -> Optional[dict]:
    """The payload claims of a JWT, read for their structure only (the token came from auth_oidc's custody, which got
    it from the token endpoint over TLS); ``None`` for anything that is not one."""
    if not isinstance(token, str) or not 0 < len(token) <= MAX_TOKEN_CHARS or token.count(".") != 2:
        return None
    body = token.split(".")[1]
    try:
        claims = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    except (ValueError, binascii.Error):
        return None
    return claims if isinstance(claims, dict) else None


def broker_issuers(module) -> tuple:
    """Issuers (exact strings) of the realms that broker the login to Entra (step 4b, ``entra_obo_broker_issuers``):
    their identities carry Entra's own ``tid``/``oid`` mapped by the realm, and auth_oidc keeps the brokered Entra
    sign-in under them. Empty by default: only an Entra issuer is a Microsoft sign-in."""
    found = _plugin_config(module).get("entra_obo_broker_issuers") or ()
    return tuple(found) if isinstance(found, (list, tuple)) else ()   # a plain string would match by substring


# --- the app client: the one seam ------------------------------------------------------------

def _plugin_config(module) -> dict:
    descriptor = getattr(module, "descriptor", None)
    config = getattr(descriptor, "config", None)
    return config if isinstance(config, dict) else {}


def _resolver():
    """``resolve_app_client`` of the configurations plugin (#7149), or ``None`` where it is not installed."""
    try:
        from ...configurations.oauth_apps import resolve_app_client  # pylint: disable=C0415,E0402
    except Exception:  # pylint: disable=W0703
        return None
    return resolve_app_client


def app_client(module, delegation: dict, project_id) -> tuple:
    """``(client_id, client_secret)`` of the app the descriptor exchanges through. The secret is used for one POST to
    the Entra authority and dropped; it is never logged or returned to a caller."""
    app_id = delegation.get("oauth_app_id")
    if app_id == PLATFORM_APP:
        config = _plugin_config(module)
        client_id = config.get("entra_obo_client_id")
        secret = os.environ.get(config.get("entra_obo_client_secret_env") or DEFAULT_SECRET_ENV)
        if not (isinstance(client_id, str) and client_id and secret):
            raise DelegationRefused(APP_UNAVAILABLE)
        return client_id, secret
    resolve = _resolver()
    if resolve is None:
        raise DelegationRefused(APP_UNAVAILABLE)
    try:
        app = resolve(app_id, project_id, CREDENTIAL_TYPE)
    except Exception as exc:  # pylint: disable=W0703
        log.info("entra obo: the oauth app was not resolved (%s)", type(exc).__name__)
        raise DelegationRefused(APP_UNAVAILABLE) from None
    endpoint = urlsplit(str((app or {}).get("token_endpoint") or ""))
    if endpoint.scheme != "https" or endpoint.hostname != AUTHORITY_HOST or not app.get("client_id") \
            or not app.get("client_secret"):
        raise DelegationRefused(APP_UNAVAILABLE)   # an app's secret goes only to the Entra authority
    return app["client_id"], app["client_secret"]


# --- the assertion: auth_oidc's custody ------------------------------------------------------

def sign_request(key, tid, oid, refresh, iat, nonce) -> str:
    """The signature auth_oidc checks: only Main and auth_oidc hold ``key``, so a process that merely reaches the bus
    (a worker running toolkit code) cannot ask for anyone's assertion."""
    message = "|".join(("v1", tid, oid, "1" if refresh else "0", str(iat), nonce)).encode("utf-8")
    return hmac.new(key.encode("utf-8"), message, hashlib.sha256).hexdigest()


def custody_key(module) -> Optional[str]:
    """The key that signs Main's requests to auth_oidc, or ``None`` when it is missing or too short."""
    key = os.environ.get(_plugin_config(module).get("entra_obo_custody_key_env") or DEFAULT_CUSTODY_KEY_ENV) or ""
    return key if len(key) >= MIN_CUSTODY_KEY_CHARS else None


def sign_fields(key, fields, iat, nonce) -> str:
    """The signature of a request of another kind than the assertion's (its version tag first, #7131): auth_oidc checks
    it over the same fields, then ``iat`` and ``nonce``."""
    message = "|".join((*fields, str(iat), nonce)).encode("utf-8")
    return hmac.new(key.encode("utf-8"), message, hashlib.sha256).hexdigest()


SEAL_INFO = b"elitea-obo-offline-seal-v1"   # auth_oidc derives the same subkey (#7131)
SEAL_PREFIX = "s1."
MAX_SEALED_CHARS = 2 * MAX_TOKEN_CHARS


def seal_key(key) -> Optional[bytes]:
    """The AES-256-GCM key that seals a refresh token on the bus: HKDF-SHA256 of the request key with its own info
    string (never the HMAC key itself), or None without a request key."""
    if not key:
        return None
    from cryptography.hazmat.primitives import hashes  # pylint: disable=C0415
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF  # pylint: disable=C0415
    return HKDF(algorithm=hashes.SHA256(), length=32, salt=None, info=SEAL_INFO).derive(key.encode("utf-8"))


def seal_aad(direction, tid, oid, holder, nonce) -> bytes:
    """What a sealed refresh token is bound to: its direction (``grant-reply``, ``redeem-req``, ``redeem-reply``) and
    the request it belongs to."""
    return "|".join(("v1-seal", direction, tid, oid, holder, nonce)).encode("utf-8")


def seal(key, plaintext, aad) -> str:
    """``s1.<base64url(nonce || ciphertext)>``: AES-256-GCM with a fresh 96-bit nonce."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # pylint: disable=C0415
    nonce = os.urandom(12)
    return SEAL_PREFIX + base64.urlsafe_b64encode(nonce + AESGCM(key).encrypt(
        nonce, plaintext.encode("utf-8"), aad)).decode("ascii")


def open_sealed(key, sealed, aad) -> Optional[str]:
    """The plaintext of ``seal``, or None for anything that does not open with this key and binding."""
    if not key or not isinstance(sealed, str) or not sealed.startswith(SEAL_PREFIX) or len(sealed) > MAX_SEALED_CHARS:
        return None
    from cryptography.exceptions import InvalidTag  # pylint: disable=C0415
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # pylint: disable=C0415
    try:
        raw = base64.urlsafe_b64decode(sealed[len(SEAL_PREFIX):].encode("ascii"))
        if len(raw) < 12 + 16:
            return None
        return AESGCM(key).decrypt(raw[:12], raw[12:], aad).decode("utf-8")
    except (InvalidTag, ValueError, binascii.Error, UnicodeDecodeError):
        return None


def fetch_assertion(module, tid, oid, refresh=False, timeout=RPC_TIMEOUT_S) -> dict:
    """The person's Entra access token from auth_oidc (RPC): ``{"ok": True, "assertion"}`` or ``{"ok": False,
    "code"}``. ``refresh`` asks auth_oidc to renew it with the stored refresh token first. The request is signed with
    the custody key and carries a single-use nonce."""
    rpc = getattr(getattr(module, "context", None), "rpc_manager", None)
    key = os.environ.get(_plugin_config(module).get("entra_obo_custody_key_env") or DEFAULT_CUSTODY_KEY_ENV) or ""
    if rpc is None or len(key) < MIN_CUSTODY_KEY_CHARS:
        log.error("entra obo: the custody service or its request key is not configured")
        raise DelegationRefused(UNAVAILABLE)
    iat, nonce = int(time.time()), secrets.token_urlsafe(16)
    try:
        return getattr(rpc.timeout(max(1, int(timeout))), ASSERTION_RPC)(
            tid=tid, oid=oid, refresh=refresh, iat=iat, nonce=nonce,
            signature=sign_request(key, tid, oid, refresh, iat, nonce))
    except Exception as exc:  # pylint: disable=W0703
        log.error("entra obo: the custody service did not answer (%s)", type(exc).__name__)
        raise DelegationRefused(UNAVAILABLE) from None


def _assertion(module, person, refresh, left, client_id) -> tuple:
    """``(assertion, custody end)``: the person's Entra access token, and when their custody ends (epoch seconds, or
    ``None`` from an auth_oidc that does not say). No custody drops every token cached for the person (#7281)."""
    if left() < MIN_CALL_S:
        raise DelegationRefused(UNAVAILABLE)
    answer = fetch_assertion(module, person["tid"], person["oid"], refresh, timeout=min(RPC_TIMEOUT_S, left()))
    if not isinstance(answer, dict):
        raise DelegationRefused(UNAVAILABLE)
    if answer.get("ok") is not True:
        if answer.get("code") in (None, "ASSERTION_UNAVAILABLE"):
            forget_subject(person["tid"], person["oid"])   # signed out or expired: nothing minted from it serves on
            raise DelegationRefused(ASSERTION_UNAVAILABLE)
        raise DelegationRefused(UNAVAILABLE)
    token = answer.get("assertion")
    claims = _claims(token)
    if claims is None:
        raise DelegationRefused(ASSERTION_UNAVAILABLE)
    if (str(claims.get("tid", "")).lower(), claims.get("oid")) != (person["tid"].lower(), person["oid"]):
        raise DelegationRefused(SUBJECT_MISMATCH)   # custody under the wrong person is never exchanged
    if claims.get("aud") not in (client_id, f"api://{client_id}"):
        raise DelegationRefused(APP_UNAVAILABLE)    # a login token for another app cannot be exchanged by this one
    ends = answer.get("custody_expires_at")
    return token, ends if isinstance(ends, int) and not isinstance(ends, bool) else None


# --- the exchange ----------------------------------------------------------------------------

def _session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False   # no proxy or netrc from the environment
    session.max_redirects = 0
    session.proxies = {}
    return session


def _post(url, data, timeout=HTTP_TIMEOUT_S) -> requests.Response:
    """One form POST to the Entra authority: https, the authority host only, no redirect, a timeout."""
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.hostname != AUTHORITY_HOST or parts.port not in (None, 443):
        raise DelegationRefused(APP_UNAVAILABLE)
    with _session() as session:
        return session.post(url, data=data, timeout=timeout, allow_redirects=False, verify=True)


def _exchange(url, client, assertion, scopes, left) -> tuple:
    """``("ok", token, expires_at)``, ``("invalid_grant", None, None)`` or raises ``DelegationRefused``."""
    try:
        response = _post(url, {
            "grant_type": OBO_GRANT, "client_id": client[0], "client_secret": client[1], "assertion": assertion,
            "scope": " ".join(scopes), "requested_token_use": "on_behalf_of"},
            timeout=(min(CONNECT_TIMEOUT_S, left), min(HTTP_TIMEOUT_S, left)))
    except DelegationRefused:
        raise
    except Exception as exc:  # pylint: disable=W0703
        log.error("entra obo: the token endpoint did not answer (%s)", type(exc).__name__)
        raise DelegationRefused(UNAVAILABLE) from None
    try:
        body = response.json() if response.content else {}
    except ValueError:
        body = {}
    body = body if isinstance(body, dict) else {}
    if response.status_code == 200:
        token, expires_in = body.get("access_token"), body.get("expires_in")
        try:
            expires_in = int(expires_in)
        except (TypeError, ValueError):
            expires_in = 0
        if not isinstance(token, str) or not token or expires_in <= 0 or \
                str(body.get("token_type", "")).lower() != "bearer":
            raise DelegationRefused(EXCHANGE_FAILED)
        return "ok", token, time.time() + expires_in
    error = body.get("error") if isinstance(body.get("error"), str) else None
    codes = [code for code in body.get("error_codes") or [] if isinstance(code, int)][:4]
    log.warning("entra obo: the exchange was refused (status=%s error=%s codes=%s)", response.status_code,
                error if error and re.fullmatch(r"[a-z_]{1,40}", error) else "other", codes)
    if response.status_code >= 500 or response.status_code in (408, 429):
        raise DelegationRefused(UNAVAILABLE)
    if error == "invalid_grant":
        return "invalid_grant", None, None
    if error in ("invalid_client", "unauthorized_client"):
        raise DelegationRefused(APP_UNAVAILABLE)
    raise DelegationRefused(EXCHANGE_FAILED)


# (tid, oid, client id, resource, scopes[, "offline:<consent>"]) -> (token, expires_at, custody end or None): this
# process only. A token redeemed from a mandate owner's offline consent (#7131) carries the consent in its key
_CACHE = {}
_PROBES = {}  # (reference id, tid, oid) -> (code or None, until)
_LOCK = threading.Lock()


def forget() -> None:
    """Drop every cached token and probe outcome (tests, and a process that wants to)."""
    with _LOCK:
        _CACHE.clear()
        _PROBES.clear()


def forget_material(material_ref) -> int:
    """Drop the tokens minted from one offline consent (#7131: withdrawn or replaced). The number dropped."""
    tag = f"offline:{material_ref}"
    with _LOCK:
        mine = [key for key in _CACHE if len(key) > 5 and key[5] == tag]
        for key in mine:
            del _CACHE[key]
    return len(mine)


def forget_subject(tid, oid) -> int:
    """Drop the cached tokens and probe outcomes of one person (they signed out: auth_oidc's logout event). The number
    of cached tokens dropped."""
    if not (isinstance(tid, str) and isinstance(oid, str)):
        return 0
    tid = tid.lower()
    with _LOCK:
        mine = [key for key in _CACHE if key[0] == tid and key[1] == oid]
        for key in mine:
            del _CACHE[key]
        for key in [key for key in _PROBES if key[1] == tid and key[2] == oid]:
            del _PROBES[key]
    return len(mine)


def check_config(module) -> bool:
    """At startup: with the platform client configured, the custody request key must be there too (``ERROR``
    otherwise; every delegated release then refuses). ``True`` when nothing is configured or both are."""
    config = _plugin_config(module)
    if not config.get("entra_obo_client_id"):
        return True
    key = os.environ.get(config.get("entra_obo_custody_key_env") or DEFAULT_CUSTODY_KEY_ENV) or ""
    if len(key) < MIN_CUSTODY_KEY_CHARS:
        log.error("entra obo: entra_obo_client_id is set but the custody request key is missing or shorter than %s "
                  "characters; Entra-delegated credentials are refused", MIN_CUSTODY_KEY_CHARS)
        return False
    return True


def _cached(key, now) -> Optional[tuple]:
    """``(token, expires_at)`` while the token has life left and the custody it was minted from has not ended."""
    with _LOCK:
        hit = _CACHE.get(key)
        if hit is not None and hit[1] - now >= MIN_REMAINING_S and (hit[2] is None or hit[2] > now):
            return hit[:2]
        _CACHE.pop(key, None)
        return None


def _keep(key, token, expires_at, custody_ends, now) -> None:
    with _LOCK:
        if len(_CACHE) >= MAX_CACHE_ENTRIES:
            for stale in [item for item, (_, until, ends) in _CACHE.items()
                          if until - now < MIN_REMAINING_S or (ends is not None and ends <= now)]:
                del _CACHE[stale]
            while len(_CACHE) >= MAX_CACHE_ENTRIES:
                del _CACHE[next(iter(_CACHE))]   # the oldest
        _CACHE[key] = (token, expires_at, custody_ends)


def acquire(module, person: dict, delegation: dict, project_id, deadline=None) -> tuple:  # pylint: disable=R0914
    """``(token, expires_at epoch seconds)`` of the descriptor's resource on behalf of ``person`` (``run_subject``),
    for the reference ``delegation`` (descriptor + ``ownership_class`` + ``custodian_subject_ref``) of project
    ``project_id``. From the cache when it has one with life left, else one assertion RPC and one exchange; an
    ``invalid_grant`` refreshes the assertion once and exchanges once more. ``deadline``: the ``time.monotonic()`` the
    whole release request must be done by (``deadline_for``); one request of one slot when absent. Raises
    ``DelegationRefused``."""
    if delegation.get("assertion_source") != "login":
        raise DelegationRefused(SOURCE_UNSUPPORTED)   # a connected Microsoft account is not built (4b chose the broker)
    if person.get("issuer") is not None and not ENTRA_ISSUER.match(person["issuer"]) and \
            person["issuer"] not in broker_issuers(module):
        raise DelegationRefused(SOURCE_UNSUPPORTED)   # a Keycloak (or other) login holds no Entra token to exchange
    bind_subject(person, delegation)
    tenant = str(delegation.get("tenant_id", "")).lower()
    if not _GUID.match(tenant):   # the authority path is built from it: checked here, not only at the descriptor's create
        raise DelegationRefused(SUBJECT_MISMATCH)
    scopes = sorted(delegation.get("scopes") or ())
    client = app_client(module, delegation, project_id)
    key = (person["tid"].lower(), person["oid"], client[0], delegation.get("resource"), tuple(scopes))
    now = time.time()
    hit = _cached(key, now)
    if hit is not None:
        return hit
    url = f"https://{AUTHORITY_HOST}/{tenant}/oauth2/v2.0/token"
    deadline = deadline if deadline is not None else time.monotonic() + BUDGET_S
    left = lambda: max(0.0, deadline - time.monotonic())  # noqa: E731

    custody = {}

    def exchange(refresh):
        assertion, custody["ends"] = _assertion(module, person, refresh, left, client[0])
        if left() < MIN_CALL_S:
            raise DelegationRefused(UNAVAILABLE)   # not started: it would end after the worker stopped waiting
        return _exchange(url, client, assertion, scopes, left())

    outcome, token, expires_at = exchange(False)
    if outcome == "invalid_grant":   # the assertion expired (or was revoked): renew it once, try once more
        if left() < RETRY_NEEDS_S:
            raise DelegationRefused(UNAVAILABLE)   # no time left for it within the worker's wait
        log.info("entra obo: the assertion was refreshed once")
        outcome, token, expires_at = exchange(True)
        if outcome != "ok":
            raise DelegationRefused(EXCHANGE_FAILED)
    _keep(key, token, expires_at, custody["ends"], now)
    return token, expires_at


def _offline_assertion(module, person, material, left, client_id) -> str:
    """The owner's Entra access token, redeemed from their offline consent by auth_oidc (one signed RPC). The refresh
    token crosses the bus only sealed (AES-256-GCM, a subkey of the request key, bound to this request), both ways; a
    rotated one is handed to ``material.rotate``."""
    key = custody_key(module)
    rpc = getattr(getattr(module, "context", None), "rpc_manager", None)
    if rpc is None or key is None:
        log.error("entra obo: the custody service or its request key is not configured")
        raise DelegationRefused(UNAVAILABLE)
    if left() < MIN_CALL_S:
        raise DelegationRefused(UNAVAILABLE)
    token = material.refresh_token.get_secret_value() if hasattr(material.refresh_token, "get_secret_value") \
        else material.refresh_token
    if not isinstance(token, str) or not token:
        raise DelegationRefused(CONSENT_REQUIRED)
    iat, nonce, sealing = int(time.time()), secrets.token_urlsafe(16), seal_key(key)
    sealed = seal(sealing, token, seal_aad("redeem-req", person["tid"], person["oid"], material.holder, nonce))
    token = None
    digest = hashlib.sha256(sealed.encode("utf-8")).hexdigest()
    fields = ("v1-offline", person["tid"], person["oid"], material.holder, material.source, digest)
    try:
        answer = getattr(rpc.timeout(max(1, int(min(RPC_TIMEOUT_S, left())))), OFFLINE_RPC)(
            tid=person["tid"], oid=person["oid"], holder=material.holder, source=material.source,
            sealed_refresh_token=sealed, iat=iat, nonce=nonce, signature=sign_fields(key, fields, iat, nonce))
    except Exception as exc:  # pylint: disable=W0703
        log.error("entra obo: the custody service did not answer (%s)", type(exc).__name__)
        raise DelegationRefused(UNAVAILABLE) from None
    if not isinstance(answer, dict) or answer.get("ok") is not True:
        code = answer.get("code") if isinstance(answer, dict) else None
        if code == "CONSENT_REFUSED":
            raise DelegationRefused(CONSENT_EXPIRED)
        if code == "REQUEST_REFUSED":
            log.error("entra obo: auth_oidc refused the signed offline request (is the request key the same?)")
        raise DelegationRefused(UNAVAILABLE)
    assertion = answer.get("assertion")
    claims = _claims(assertion)
    if claims is None:
        raise DelegationRefused(UNAVAILABLE)
    if (str(claims.get("tid", "")).lower(), claims.get("oid")) != (person["tid"].lower(), person["oid"]):
        raise DelegationRefused(SUBJECT_MISMATCH)
    if claims.get("aud") not in (client_id, f"api://{client_id}"):
        raise DelegationRefused(APP_UNAVAILABLE)
    if answer.get("sealed_refresh_token") is not None:
        rotated = open_sealed(sealing, answer["sealed_refresh_token"],
                              seal_aad("redeem-reply", person["tid"], person["oid"], material.holder, nonce))
        if rotated:
            material.rotate(rotated)
        else:
            log.warning("entra obo: a rotated consent did not open; the current one is kept")
    return assertion


def acquire_offline(module, person: dict, delegation: dict, project_id, material, deadline=None) -> tuple:  # pylint: disable=R0913
    """``acquire`` for a mandate run acting with its owner's offline consent (#7131): ``person`` is the owner's subject
    frozen in the mandate, ``material`` their consent read for this mandate (``mandate_identity.release_person``). The
    same descriptor checks and exchange; the assertion is redeemed from the consent, not from a live sign-in. Tokens are
    cached apart from the owner's own sign-in (the key names the consent), so withdrawing it stops them and a run of the
    owner's never gets one. Raises ``DelegationRefused``."""
    if delegation.get("assertion_source") != "login":
        raise DelegationRefused(SOURCE_UNSUPPORTED)
    if person.get("issuer") is not None and not ENTRA_ISSUER.match(person["issuer"]) and \
            person["issuer"] not in broker_issuers(module):
        raise DelegationRefused(SOURCE_UNSUPPORTED)
    bind_subject(person, delegation)
    tenant = str(delegation.get("tenant_id", "")).lower()
    if not _GUID.match(tenant):
        raise DelegationRefused(SUBJECT_MISMATCH)
    scopes = sorted(delegation.get("scopes") or ())
    client = app_client(module, delegation, project_id)
    key = (person["tid"].lower(), person["oid"], client[0], delegation.get("resource"), tuple(scopes),
           f"offline:{material.ref}")
    now = time.time()
    hit = _cached(key, now)
    if hit is not None:
        return hit
    deadline = deadline if deadline is not None else time.monotonic() + BUDGET_S
    left = lambda: max(0.0, deadline - time.monotonic())  # noqa: E731
    assertion = _offline_assertion(module, person, material, left, client[0])
    if left() < MIN_CALL_S:
        raise DelegationRefused(UNAVAILABLE)
    outcome, token, expires_at = _exchange(
        f"https://{AUTHORITY_HOST}/{tenant}/oauth2/v2.0/token", client, assertion, scopes, left())
    if outcome != "ok":   # a freshly redeemed assertion: nothing to renew
        raise DelegationRefused(EXCHANGE_FAILED)
    _keep(key, token, expires_at, None, now)
    return token, expires_at


def probe(module, reference_id, person: dict, delegation: dict, project_id) -> Optional[str]:
    """Can ``person`` currently obtain a token through this descriptor? ``None`` when yes, else the code. One exchange
    (or a cached token); the outcome, never the token, is kept for ``PROBE_TTL_S``."""
    key, now = (str(reference_id), person["tid"].lower(), person["oid"]), time.time()
    with _LOCK:
        kept = _PROBES.get(key)
        if kept is not None and kept[1] > now:
            return kept[0]
    try:
        acquire(module, person, delegation, project_id)
        code = None
    except DelegationRefused as refused:
        code = refused.code
    with _LOCK:
        if len(_PROBES) >= MAX_CACHE_ENTRIES:
            for stale in [item for item, (_, until) in _PROBES.items() if until <= now]:
                del _PROBES[stale]
            while len(_PROBES) >= MAX_CACHE_ENTRIES:
                del _PROBES[next(iter(_PROBES))]
        _PROBES[key] = (code, now + PROBE_TTL_S)
    return code


def app_available(module, delegation: dict, project_id) -> bool:
    """Can the descriptor's app be resolved now (the credential onboarding asks before it stores a descriptor)? The
    client secret is resolved and dropped here; nothing of it is returned."""
    try:
        app_client(module, delegation, project_id)
        return True
    except DelegationRefused:
        return False
