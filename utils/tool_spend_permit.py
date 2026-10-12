""" Signed single-use permits: issued by tool_spend_check, required by tool_spend_record (#6980) """

import base64
import hashlib
import hmac
import json
import time
import uuid

PERMIT_TTL_SECONDS = 600
PERMIT_CONTEXT = b"tool-spend-permit-v1"
CLAIM_KEYS = ("pid", "uid", "feature", "backend", "op", "cfg_uuid", "exempt", "max_units", "unit",
              "run_id", "nonce", "iat", "exp")


class PermitError(Exception):
    """Permit is missing, forged, expired or does not match the request."""


def permit_key(hmac_key):
    """Derived from the platform event_node HMAC key, so every pod verifies the same permits."""
    if not hmac_key:
        raise PermitError("tool spend permit key is not configured")
    raw = hmac_key.encode() if isinstance(hmac_key, str) else bytes(hmac_key)
    return hmac.new(raw, PERMIT_CONTEXT, hashlib.sha256).digest()


def _b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(text):
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def issue_permit(key, now=None, **claims):
    issued = int(now if now is not None else time.time())
    body = dict(claims, nonce=uuid.uuid4().hex, iat=issued, exp=issued + PERMIT_TTL_SECONDS)
    missing = [name for name in CLAIM_KEYS if name not in body]
    if missing:
        raise ValueError(f"permit claims missing {missing}")
    payload = _b64(json.dumps(body, sort_keys=True, separators=(",", ":")).encode())
    mac = _b64(hmac.new(key, payload.encode(), hashlib.sha256).digest())
    return f"{payload}.{mac}", body


def verify_permit(key, token, expected, now=None):
    """Claims of a valid permit; every key in `expected` must match exactly."""
    if not isinstance(token, str) or token.count(".") != 1:
        raise PermitError("permit is missing or malformed")
    payload, mac = token.split(".")
    good = _b64(hmac.new(key, payload.encode(), hashlib.sha256).digest())
    if not hmac.compare_digest(good, mac):
        raise PermitError("permit signature is invalid")
    try:
        claims = json.loads(_unb64(payload))
    except (ValueError, TypeError) as exc:
        raise PermitError("permit payload is unreadable") from exc
    if int(now if now is not None else time.time()) >= int(claims.get("exp", 0)):
        raise PermitError("permit has expired")
    for name, value in expected.items():
        if claims.get(name) != value:
            raise PermitError(f"permit claim {name} does not match")
    return claims
