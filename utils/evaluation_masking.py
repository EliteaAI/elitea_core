"""Mask secrets in what an eval trajectory stores (#6809, design Q-S5).

A trajectory lives in ``eval_case_execution`` and anyone who can view the run can read it, until the
run is deleted. The live trace (``MessageTraceStep``) instead follows the conversation's permissions
and retention. So tool inputs, tool outputs and errors are masked when the trajectory is built:

* a value under a sensitive key becomes :data:`MASK_VALUE`, at any depth;
* a known secret format inside free text (an OpenAI key, a bearer token, a GitHub PAT, ...) is
  replaced where it occurs, so the rest of the output stays readable.

Key masking does not catch every secret in free-text output. The per-step cap and the suite's
``store_tool_outputs`` opt-out are the actual mitigation.

**Debt (#6886):** the key set and the patterns are copied from
``tracing/utils/payload_capture.py``, not imported: elitea_core does not depend on tracing. Two
matchers can drift. Replace both with one shared helper when #6886 lands (design §12).

Matching is stricter than tracing's on purpose. Tracing masks a key that *contains* any entry, so
``issue_key``, ``author``, ``session_id`` and ``primary_key`` are masked there. A stored trajectory
is read for its arguments, and an expected-argument check accepts a masked value as present, so
masking those would hide them and let any value pass. Here a key is split into words first.

Stdlib only, no ORM.
"""
import json
import re
from typing import Any

#: Copied from ``tracing/utils/payload_capture.py`` ``DEFAULT_SENSITIVE_KEYS``. Matched by word, not
#: substring (:func:`is_sensitive_key`), so ``github_token`` and ``X-Api-Key`` match, ``author`` does not.
SENSITIVE_KEYS = frozenset({
    'password', 'passwd', 'pwd', 'secret', 'token', 'api_key', 'apikey',
    'api-key', 'auth', 'authorization', 'bearer', 'credential', 'credentials',
    'private_key', 'privatekey', 'access_token', 'refresh_token', 'session',
    'cookie', 'x-api-key', 'x-auth-token', 'jwt', 'key', 'cert', 'certificate',
})
MASK_VALUE = '***MASKED***'
#: Copied from ``PayloadCapture._add_default_patterns``.
SECRET_PATTERNS = tuple(re.compile(p) for p in (
    r'sk-[a-zA-Z0-9]{20,}',
    r'Bearer\s+[a-zA-Z0-9._-]+',
    r'Basic\s+[a-zA-Z0-9+/=]+',
    r'ghp_[a-zA-Z0-9]{36}',
    r'gho_[a-zA-Z0-9]{36}',
    r'xox[baprs]-[a-zA-Z0-9-]+',
))
_MAX_DEPTH = 20


_CAMEL_BOUNDARY = re.compile(r'(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])')
_SEPARATORS = re.compile(r'[^a-z0-9]+')


def _words(key: str) -> tuple:
    """``'X-Api-Key'`` → ``('x', 'api', 'key')``; ``'issueKey'`` → ``('issue', 'key')``."""
    return tuple(w for w in _SEPARATORS.split(_CAMEL_BOUNDARY.sub('_', key).lower()) if w)


_SENSITIVE_WORDS = tuple(_words(k) for k in SENSITIVE_KEYS)
#: Words too common to mask wherever they occur: ``key`` (``issue_key``, ``primary_key``), ``auth``
#: (``auth_type``), ``session`` (``session_id``), ``cert`` (``cert_path``). Each is masked as the
#: whole key, or as the last word after a qualifier that makes it a secret.
_AMBIGUOUS_WORDS = {
    'key': frozenset({'api', 'private', 'secret', 'access', 'signing', 'encryption', 'ssh', 'client',
                      'master', 'license', 'account', 'subscription'}),
    'auth': frozenset({'basic', 'http', 'proxy'}),
    'session': frozenset(),
    'cert': frozenset({'client', 'ssl', 'tls'}),
}


def is_sensitive_key(key) -> bool:
    """Is ``key`` one of :data:`SENSITIVE_KEYS`, compared word by word?

    The key's words must contain an entry's words in order (``refresh_token``, ``x-api-key``,
    ``GitHub_Token``), except for :data:`_AMBIGUOUS_WORDS`, which need the whole key or a qualifier."""
    if not isinstance(key, str):
        return False
    words = _words(key)
    if not words:
        return False
    for sensitive in _SENSITIVE_WORDS:
        if len(sensitive) == 1 and sensitive[0] in _AMBIGUOUS_WORDS:
            word = sensitive[0]
            if words == sensitive or (len(words) >= 2 and words[-1] == word
                                      and words[-2] in _AMBIGUOUS_WORDS[word]):
                return True
            continue
        size = len(sensitive)
        if any(words[i:i + size] == sensitive for i in range(len(words) - size + 1)):
            return True
    return False


def mask_text(text: str) -> str:
    """``text`` with every known secret format replaced by :data:`MASK_VALUE`."""
    for pattern in SECRET_PATTERNS:
        text = pattern.sub(MASK_VALUE, text)
    return text


def mask_value(value: Any, _depth: int = 0) -> Any:
    """A copy of ``value`` with sensitive keys masked at any depth and secrets masked in strings."""
    if isinstance(value, str):
        return mask_text(value)
    if _depth >= _MAX_DEPTH:
        return value
    if isinstance(value, dict):
        return {k: (MASK_VALUE if is_sensitive_key(k) and v is not None else mask_value(v, _depth + 1))
                for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [mask_value(v, _depth + 1) for v in value]
    return value


def mask_payload(value: Any) -> Any:
    """:func:`mask_value`, but a string holding a JSON object or array is masked as structure.

    Tool outputs often arrive as serialized JSON (``'{"token": "..."}'``). Key masking cannot see
    keys inside a string, so the string is parsed, masked and re-serialized. Anything that does not
    parse stays text."""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:1] in ('{', '['):
            try:
                parsed = json.loads(stripped)
            except ValueError:
                return mask_text(value)
            if isinstance(parsed, (dict, list)):
                return json.dumps(mask_value(parsed), ensure_ascii=False, default=str)
        return mask_text(value)
    return mask_value(value)
