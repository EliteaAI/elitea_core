"""Input guardrail detector (#6886).

Twin of ``elitea_sdk/runtime/guardrails/input_detector.py``: pylon_main does not import
the SDK at runtime (see utils/run_id.py), so both copies run against
``input_guardrail_vectors.json`` and a drift in either one fails a test. Change both or
neither.

The one rule that matters here: a detected value never leaves ``inspect()``. A
Detection carries offsets, never text, and ``redacted`` only ever holds the input with
the value already replaced. Nothing in this module logs input text.
"""

import base64
import hashlib
import json
import math
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Pattern, Tuple

ACTIONS = ('off', 'record', 'warn', 'redact', 'block')
# 'none' is the result action when nothing matched; it is not a configurable action
ACTION_SEVERITY = {'none': 0, 'off': 0, 'record': 1, 'warn': 2, 'redact': 3, 'block': 4}
SEGMENT_KINDS = ('current_turn', 'history', 'instructions', 'subagent_task', 'hitl_edit')
CATEGORIES = ('credentials', 'personal_data')
DEFAULT_CATEGORY_ACTIONS = {'credentials': 'block', 'personal_data': 'off'}
DEFAULT_MAX_INSPECT_CHARS = 200_000
DEFAULT_TIMEOUT_MS = 200

ERROR_TIMEOUT = 'timeout'
ERROR_INTERNAL = 'internal'


@dataclass(frozen=True)
class Segment:
    kind: str
    index: int
    text: str


@dataclass(frozen=True)
class Detection:
    category: str
    rule_id: str
    segment_kind: str
    segment_index: int
    start: int
    end: int


@dataclass(frozen=True)
class InspectionResult:
    action: str = 'none'
    detections: Tuple[Detection, ...] = ()
    # Only segments that changed, keyed by (kind, index)
    redacted: Dict[Tuple[str, int], str] = field(default_factory=dict)
    not_inspected: Tuple[str, ...] = ()
    error: Optional[str] = None
    elapsed_ms: int = 0

    @property
    def categories(self) -> List[str]:
        return sorted({d.category for d in self.detections})

    @property
    def rules(self) -> List[str]:
        return sorted({f'{d.category}.{d.rule_id}' for d in self.detections})


@dataclass(frozen=True)
class Policy:
    enabled: bool = False
    on_error: str = 'fail_open'
    timeout_ms: int = DEFAULT_TIMEOUT_MS
    max_inspect_chars: int = DEFAULT_MAX_INSPECT_CHARS
    categories: Dict[str, str] = field(default_factory=lambda: dict(DEFAULT_CATEGORY_ACTIONS))
    allow_patterns: Tuple[Pattern, ...] = ()
    exempt_project_ids: Tuple[int, ...] = ()

    @property
    def active(self) -> bool:
        return self.enabled and any(a != 'off' for a in self.categories.values())

    def action_for(self, category: str) -> str:
        return self.categories.get(category, 'off')


def policy_from_dict(raw: Optional[Dict[str, Any]]) -> Policy:
    """Build a Policy from the ``input_guardrail`` config block.

    Unknown actions fall back to the category default rather than raising: the admin
    schema validates on save, and a bad value here must not take the runtime down.
    Invalid allow patterns are dropped, and the error names the index, never the text.
    """
    if not raw:
        return Policy()
    categories = dict(DEFAULT_CATEGORY_ACTIONS)
    for name, action in (raw.get('categories') or {}).items():
        if name in CATEGORIES and action in ACTIONS:
            categories[name] = action
    allow: List[Pattern] = []
    for pattern in raw.get('allow_patterns') or []:
        try:
            allow.append(re.compile(pattern))
        except (re.error, TypeError):
            continue
    exempt = []
    for project_id in raw.get('exempt_project_ids') or []:
        try:
            exempt.append(int(project_id))
        except (TypeError, ValueError):
            continue
    return Policy(
        enabled=bool(raw.get('enabled', False)),
        on_error='fail_closed' if raw.get('on_error') == 'fail_closed' else 'fail_open',
        timeout_ms=_positive_int(raw.get('timeout_ms'), DEFAULT_TIMEOUT_MS),
        max_inspect_chars=_positive_int(raw.get('max_inspect_chars'), DEFAULT_MAX_INSPECT_CHARS),
        categories=categories,
        allow_patterns=tuple(allow),
        exempt_project_ids=tuple(exempt),
    )


def _positive_int(value, default: int) -> int:
    try:
        value = int(value)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


# --- Validators: precision guards that a regex alone cannot express ---------------

_PLACEHOLDER_VALUE = re.compile(
    r'^(\*+|x+|\.+|<[^>]*>|\$\{[^}]*\}|\{\{[^}]*\}\}|%[^%]+%|changeme|password|secret|'
    r'example|redacted|\[redacted[^\]]*\]|null|none|true|false|undefined)$',
    re.IGNORECASE,
)


def _shannon_entropy(value: str) -> float:
    if not value:
        return 0.0
    counts: Dict[str, int] = {}
    for ch in value:
        counts[ch] = counts.get(ch, 0) + 1
    length = len(value)
    return -sum(c / length * math.log2(c / length) for c in counts.values())


# Code, not a literal: a call or subscript, a dotted attribute, an env-var name
_CODE_LIKE_VALUE = re.compile(
    r'[(){}\[\]]|^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)+$|^[A-Z][A-Z0-9_]+$|^\$\w+$'
)


def _password_value_ok(match: re.Match) -> bool:
    value = match.group('value').strip('"\'')
    if len(value) < 8 or _PLACEHOLDER_VALUE.match(value) or _CODE_LIKE_VALUE.search(value):
        return False
    return _shannon_entropy(value) >= 3.0


def _jwt_ok(match: re.Match) -> bool:
    header = match.group('header')
    try:
        decoded = json.loads(base64.urlsafe_b64decode(header + '=' * (-len(header) % 4)))
        return isinstance(decoded, dict) and 'alg' in decoded
    except Exception:
        return False


def _luhn_ok(match: re.Match) -> bool:
    digits = [int(c) for c in re.sub(r'\D', '', match.group(0))]
    if not 13 <= len(digits) <= 19 or len(set(digits)) == 1:
        return False
    total = 0
    for i, digit in enumerate(reversed(digits)):
        if i % 2:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0


def _iban_ok(match: re.Match) -> bool:
    iban = re.sub(r'\s', '', match.group(0)).upper()
    if not 15 <= len(iban) <= 34:
        return False
    rearranged = iban[4:] + iban[:4]
    try:
        return int(''.join(str(int(c, 36)) for c in rearranged)) % 97 == 1
    except ValueError:
        return False


@dataclass(frozen=True)
class Rule:
    rule_id: str
    category: str
    pattern: Pattern
    validator: Optional[Any] = None
    # Named group whose span is replaced; None replaces the whole match. Keeps a key
    # name like "password=" readable after redaction.
    span_group: Optional[str] = None


RULES: Tuple[Rule, ...] = (
    Rule('private_key_block', 'credentials', re.compile(
        r'-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----'
        r'[\s\S]+?-----END (?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY-----')),
    Rule('aws_access_key', 'credentials', re.compile(r'\b(?:AKIA|ASIA)[A-Z0-9]{16}\b')),
    Rule('aws_secret_assignment', 'credentials', re.compile(
        r'aws_secret_access_key["\']?\s*[:=]\s*["\']?(?P<value>[A-Za-z0-9/+=]{40})(?![A-Za-z0-9/+=])',
        re.IGNORECASE), span_group='value'),
    Rule('github_token', 'credentials', re.compile(
        r'\b(?:gh[pousr]_[A-Za-z0-9]{36}|github_pat_[A-Za-z0-9_]{82})\b')),
    Rule('anthropic_key', 'credentials', re.compile(r'\bsk-ant-[A-Za-z0-9_-]{32,}')),
    Rule('openai_key', 'credentials', re.compile(r'\bsk-(?!ant-)(?:proj-)?[A-Za-z0-9_-]{20,}')),
    Rule('slack_token', 'credentials', re.compile(r'\bxox[abprs]-[A-Za-z0-9-]{10,}')),
    Rule('google_api_key', 'credentials', re.compile(r'\bAIza[0-9A-Za-z_-]{35}(?![0-9A-Za-z_-])')),
    Rule('azure_storage_conn', 'credentials', re.compile(
        r'AccountKey=(?P<value>[A-Za-z0-9+/=]{60,})'), span_group='value'),
    Rule('jwt', 'credentials', re.compile(
        r'\b(?P<header>eyJ[A-Za-z0-9_-]{5,})\.eyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{10,}'),
        validator=_jwt_ok),
    Rule('auth_header', 'credentials', re.compile(
        r'Authorization\s*:\s*(?:Bearer|Basic|Token)\s+(?P<value>[A-Za-z0-9._~+/=-]{16,})',
        re.IGNORECASE), span_group='value'),
    Rule('password_assignment', 'credentials', re.compile(
        r'\b(?:password|passwd|pwd|secret|api[_-]?key|access[_-]?token|auth[_-]?token|token)'
        r'["\']?\s*[:=]\s*(?P<value>"[^"\s]{8,}"|\'[^\'\s]{8,}\'|[^\s"\',;]{8,})',
        re.IGNORECASE), validator=_password_value_ok, span_group='value'),
    Rule('email', 'personal_data', re.compile(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}\b')),
    Rule('phone_e164', 'personal_data', re.compile(r'(?<![\w+])\+[1-9]\d{7,14}\b')),
    Rule('payment_card', 'personal_data', re.compile(r'\b(?:\d[ -]?){12,18}\d\b'), validator=_luhn_ok),
    Rule('iban', 'personal_data', re.compile(r'\b[A-Z]{2}\d{2}(?:[ ]?[A-Z0-9]){11,30}\b'), validator=_iban_ok),
)


def segment_hash(text: str) -> str:
    """Identity of an already-inspected segment, so a later checkpoint skips it."""
    return hashlib.sha256(text.encode('utf-8', 'surrogatepass')).hexdigest()


def placeholder(category: str) -> str:
    return f'[REDACTED:{category}]'


class _Timeout(Exception):
    pass


def _find(segment: Segment, policy: Policy, deadline: float) -> List[Detection]:
    found: List[Detection] = []
    taken: List[Tuple[int, int]] = []
    for rule in RULES:
        if policy.action_for(rule.category) == 'off':
            continue
        if time.monotonic() > deadline:
            raise _Timeout()
        for match in rule.pattern.finditer(segment.text):
            if rule.validator is not None and not rule.validator(match):
                continue
            start, end = match.span(rule.span_group) if rule.span_group else match.span()
            # Earlier rules are more specific (a private key contains base64 that
            # could look like a token); an overlapping later hit is the same value
            if any(start < t_end and t_start < end for t_start, t_end in taken):
                continue
            if any(p.search(segment.text[start:end]) for p in policy.allow_patterns):
                continue
            taken.append((start, end))
            found.append(Detection(rule.category, rule.rule_id, segment.kind, segment.index, start, end))
    return found


def _redact(text: str, detections: Iterable[Detection]) -> str:
    for d in sorted(detections, key=lambda d: d.start, reverse=True):
        text = text[:d.start] + placeholder(d.category) + text[d.end:]
    return text


def _max_action(actions: Iterable[str]) -> str:
    return max(actions, key=lambda a: ACTION_SEVERITY[a], default='none')


def inspect(
    segments: List[Segment],
    policy: Policy,
    not_inspected: Iterable[str] = (),
) -> InspectionResult:
    """Inspect segments against the policy. Never raises: failures come back as
    ``error`` and the caller applies ``on_error``."""
    started = time.monotonic()
    not_inspected = tuple(not_inspected)

    def elapsed() -> int:
        return int((time.monotonic() - started) * 1000)

    if not policy.active:
        return InspectionResult(not_inspected=not_inspected)
    try:
        if sum(len(s.text) for s in segments) > policy.max_inspect_chars:
            return InspectionResult(not_inspected=not_inspected, error=ERROR_TIMEOUT, elapsed_ms=elapsed())
        deadline = started + policy.timeout_ms / 1000.0
        detections: List[Detection] = []
        redacted: Dict[Tuple[str, int], str] = {}
        for segment in segments:
            if not segment.text:
                continue
            found = _find(segment, policy, deadline)
            detections.extend(found)
            to_redact = [d for d in found if policy.action_for(d.category) == 'redact']
            if to_redact:
                redacted[(segment.kind, segment.index)] = _redact(segment.text, to_redact)
        action = _max_action(policy.action_for(d.category) for d in detections)
        return InspectionResult(
            action=action,
            detections=tuple(detections),
            redacted=redacted,
            not_inspected=not_inspected,
            elapsed_ms=elapsed(),
        )
    except _Timeout:
        return InspectionResult(not_inspected=not_inspected, error=ERROR_TIMEOUT, elapsed_ms=elapsed())
    except Exception:
        # No exception text: a regex error message can quote the input it ran on
        return InspectionResult(not_inspected=not_inspected, error=ERROR_INTERNAL, elapsed_ms=elapsed())


def effective_action(result: InspectionResult, policy: Policy) -> str:
    """The action to take, with ``on_error`` folded in for a failed inspection."""
    if result.error:
        return 'block' if policy.on_error == 'fail_closed' else 'none'
    return result.action


def audit_actions(result: InspectionResult, policy: Policy) -> List[Dict[str, Any]]:
    """Audit entries in the ``input:<category>.<rule>:<action>`` format. Values excluded."""
    if result.error:
        return [{
            'action': f'input:error.{result.error}:{policy.on_error}',
            'segment_kind': None,
            'is_error': True,
        }]
    seen = set()
    entries = []
    for d in result.detections:
        key = (d.category, d.rule_id, d.segment_kind)
        if key in seen:
            continue
        seen.add(key)
        entries.append({
            'action': f'input:{d.category}.{d.rule_id}:{policy.action_for(d.category)}',
            'segment_kind': d.segment_kind,
            'is_error': False,
        })
    return entries


def summary(result: InspectionResult, policy: Policy) -> Dict[str, Any]:
    """The only shape that leaves the guardrail: rule ids, counts and offsets-free metadata."""
    return {
        'action': effective_action(result, policy),
        'categories': result.categories,
        'rules': result.rules,
        'segments': sorted({d.segment_kind for d in result.detections}),
        'detection_count': len(result.detections),
        'not_inspected': list(result.not_inspected),
        'error': result.error,
        'on_error': policy.on_error if result.error else None,
        'elapsed_ms': result.elapsed_ms,
        'audit': audit_actions(result, policy),
    }
