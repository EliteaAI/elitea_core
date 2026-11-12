"""Decision rules of the environment-wide scheduled evaluation, as pure functions.

Everything the tick and drain decide that does not need a database lives here, so it is tested
without one: which projects and agents are in scope, which dataset an agent is evaluated on, why
an agent is "Not Evaluated", and the Passed / Warning / Failed verdict.

Inputs are plain dicts and lists, never ORM rows. Stdlib only, no plugin imports (same convention
as ``evaluation_scoring``).
"""

import hashlib
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Tuple

PERSONAL_PROJECT_PREFIX = 'project_user_'

SCOPE_ALL = 'all'
SCOPE_ALLOW = 'allow'
SCOPE_DENY = 'deny'

POLICY_AGENT_FIRST = 'agent_first'
POLICY_AGENT_ONLY = 'agent_only'
POLICY_PLATFORM_ONLY = 'platform_only'

TIER_AGENT_DEFAULT = 'agent_default'
TIER_AGENT_SUITE = 'agent_suite'
TIER_AGENT_ONLY_SUITE = 'agent_only_suite'
TIER_PROJECT = 'project'
TIER_PLATFORM = 'platform'

HITL_REQUIRED = 'HITL_REQUIRED'
OAUTH_REQUIRED = 'OAUTH_REQUIRED'
AGENT_BROKEN = 'AGENT_BROKEN'
TIMEOUT = 'TIMEOUT'
OWNER_UNAVAILABLE = 'OWNER_UNAVAILABLE'
BUDGET_BLOCKED = 'BUDGET_BLOCKED'
NO_DATASET = 'NO_DATASET'
OTHER = 'OTHER'
REASON_CODES = (HITL_REQUIRED, OAUTH_REQUIRED, AGENT_BROKEN, TIMEOUT, OWNER_UNAVAILABLE,
                BUDGET_BLOCKED, NO_DATASET, OTHER)

PASSED = 'passed'
WARNING = 'warning'
FAILED = 'failed'
NOT_EVALUATED = 'not_evaluated'

# Whether it runs, and when, is the Runtime config's eval_auto_run_enabled and _cron: one switch.
DEFAULT_SETTINGS = {
    'project_scope_mode': SCOPE_ALL,
    'project_ids': [],
    'include_personal_projects': False,
    'dataset_policy': POLICY_AGENT_FIRST,
    'max_agents_per_job': 200,
    'max_cases_per_agent': 10,
    'max_concurrent_runs': 2,
    'case_concurrency': 1,
    'fail_threshold': 50.0,
    'coverage_threshold': 80.0,
    'classify_threshold': 0.5,
    'reevaluate_after_days': 28,
    # {'start': 'HH:MM', 'end': 'HH:MM'} in server time, or None for "any time"
    'window': None,
}

# (minimum, maximum) of each numeric setting; a stored value outside it is clamped.
_BOUNDS = {
    'max_agents_per_job': (1, 10000),
    'max_cases_per_agent': (1, 1000),
    'max_concurrent_runs': (1, 50),
    'case_concurrency': (1, 8),
    'fail_threshold': (0.0, 100.0),
    'coverage_threshold': (0.0, 100.0),
    'classify_threshold': (0.0, 1.0),
    'reevaluate_after_days': (0, 365),
}
_CHOICES = {
    'project_scope_mode': (SCOPE_ALL, SCOPE_ALLOW, SCOPE_DENY),
    'dataset_policy': (POLICY_AGENT_FIRST, POLICY_AGENT_ONLY, POLICY_PLATFORM_ONLY),
}

_OPERATORS = {
    '>=': lambda score, goal: score >= goal,
    '>': lambda score, goal: score > goal,
    '<=': lambda score, goal: score <= goal,
    '<': lambda score, goal: score < goal,
    '==': lambda score, goal: score == goal,
}

# Case statuses the post-run classifier maps to a reason, in the order they are checked.
_PREDICT_FAILURES = ('predict_error', 'predict_exception', 'error')
_LOAD_FAILURE_MARK = 'could not load agent version'

# Blocking risk-scan codes (schedule_risk.BLOCKING_CODES) and what each means here.
_SCAN_REASONS = {
    'human_input_step': HITL_REQUIRED,
    'personal_signin_required': OAUTH_REQUIRED,
    'missing_credential': AGENT_BROKEN,
    'delegated_credential': OAUTH_REQUIRED,
}
_SCAN_ORDER = ('human_input_step', 'personal_signin_required', 'delegated_credential', 'missing_credential')


# ----------------------------------------------------------------------------
# Settings
# ----------------------------------------------------------------------------

def _parse_hhmm(value) -> Optional[int]:
    if not isinstance(value, str):
        return None
    parts = value.split(':')
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        return None
    hours, minutes = int(parts[0]), int(parts[1])
    if hours > 23 or minutes > 59:
        return None
    return hours * 60 + minutes


def _clean_window(value) -> Optional[dict]:
    if not isinstance(value, dict):
        return None
    start, end = _parse_hhmm(value.get('start')), _parse_hhmm(value.get('end'))
    if start is None or end is None or start == end:
        return None
    return {'start': value['start'], 'end': value['end']}


def effective_config(stored: Optional[dict]) -> dict:
    """The stored settings over the defaults, with unknown keys dropped and bad values replaced.

    Never raises: a malformed value falls back to its default, so a bad admin save cannot stop
    the job from reading its own configuration.
    """
    stored = stored if isinstance(stored, dict) else {}
    settings = dict(DEFAULT_SETTINGS)
    for key, default in DEFAULT_SETTINGS.items():
        if key not in stored:
            continue
        value = stored[key]
        if key in _BOUNDS:
            low, high = _BOUNDS[key]
            try:
                number = type(default)(value)
            except (TypeError, ValueError):
                continue
            settings[key] = min(max(number, low), high)
        elif key in _CHOICES:
            if value in _CHOICES[key]:
                settings[key] = value
        elif key == 'project_ids':
            if isinstance(value, list):
                settings[key] = sorted({int(item) for item in value
                                        if isinstance(item, int) or (isinstance(item, str) and item.isdigit())})
        elif key == 'window':
            settings[key] = _clean_window(value)
        elif isinstance(default, bool):
            settings[key] = bool(value)
    return settings


def invalid_settings(settings: dict) -> list:
    """The keys of an admin save that :func:`effective_config` would replace with their default.

    Read-time fallback keeps a bad stored value from stopping the job, but on save it would silently
    swap the stored value for the default (an unknown scope mode would widen an allow-list to every
    project), so a save rejects them instead. Out-of-range numbers are not listed: they are clamped.
    """
    invalid = []
    for key, value in settings.items():
        if key in _BOUNDS:
            default = DEFAULT_SETTINGS[key]
            try:
                type(default)(value)
            except (TypeError, ValueError):
                invalid.append(key)
            else:
                if isinstance(value, bool):
                    invalid.append(key)
        elif key in _CHOICES:
            if value not in _CHOICES[key]:
                invalid.append(key)
        elif key == 'project_ids':
            if not isinstance(value, list) or not all(
                    (isinstance(item, int) and not isinstance(item, bool)) or (isinstance(item, str) and item.isdigit())
                    for item in value):
                invalid.append(key)
        elif key == 'window':
            if value is not None and _clean_window(value) is None:
                invalid.append(key)
        elif key in DEFAULT_SETTINGS and isinstance(DEFAULT_SETTINGS[key], bool):
            if not isinstance(value, bool):
                invalid.append(key)
    return sorted(invalid)


def in_window(window: Optional[dict], now: datetime) -> bool:
    """True when ``now`` falls in the launch window. A window may span midnight (22:00-06:00)."""
    if not window:
        return True
    start, end = _parse_hhmm(window.get('start')), _parse_hhmm(window.get('end'))
    if start is None or end is None:
        return True
    minute = now.hour * 60 + now.minute
    if start < end:
        return start <= minute < end
    return minute >= start or minute < end


# ----------------------------------------------------------------------------
# Scope and eligibility
# ----------------------------------------------------------------------------

def like_pattern(term: str) -> str:
    """A substring LIKE pattern for ``term``, with its wildcards escaped by a backslash."""
    escaped = term.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
    return f'%{escaped}%'


def is_personal_project(name: Optional[str]) -> bool:
    return bool(name) and name.startswith(PERSONAL_PROJECT_PREFIX)


def project_in_scope(project: dict, settings: dict) -> bool:
    """Whether a project (``{'id', 'name'}``) is enumerated by the job."""
    if not settings.get('include_personal_projects') and is_personal_project(project.get('name')):
        return False
    mode = settings.get('project_scope_mode', SCOPE_ALL)
    ids = set(settings.get('project_ids') or [])
    if mode == SCOPE_ALLOW:
        return project.get('id') in ids
    if mode == SCOPE_DENY:
        return project.get('id') not in ids
    return True


def needs_evaluation(version_id: Optional[int], last: Optional[dict], now: datetime,
                     reevaluate_after_days: int) -> Tuple[bool, str]:
    """Whether an agent is queued again, and why: ``new`` | ``changed`` | ``stale`` | ``fresh``.

    ``last`` is the agent's latest finished item (``{'application_version_id', 'finished_at'}``).
    An unchanged agent with a recent result keeps it ("carried forward") and costs nothing.
    """
    if not last or last.get('finished_at') is None:
        return True, 'new'
    if last.get('application_version_id') != version_id:
        return True, 'changed'
    if now - last['finished_at'] >= timedelta(days=reevaluate_after_days):
        return True, 'stale'
    return False, 'fresh'


def oldest_first(candidates: List[dict]) -> List[dict]:
    """Never-evaluated agents first, then by the age of their last result."""
    return sorted(candidates, key=lambda item: (item.get('last_finished_at') is not None,
                                                item.get('last_finished_at') or datetime.min))


def stable_case_sample(case_ids: Iterable[int], cap: int, salt: str = '') -> List[int]:
    """At most ``cap`` case ids, the same ones on every tick while the dataset is unchanged.

    The order is a hash of the id, not the id itself, so a capped sample of a dataset built in
    sections is not just its first section. Returned in the dataset's own order.
    """
    ids = list(case_ids)
    if cap is None or len(ids) <= cap:
        return ids
    chosen = set(sorted(ids, key=lambda case_id: hashlib.sha256(f'{salt}:{case_id}'.encode()).hexdigest())[:cap])
    return [case_id for case_id in ids if case_id in chosen]


# ----------------------------------------------------------------------------
# Platform probes
# ----------------------------------------------------------------------------

# An agent's copy is capped by the dataset case limit too, so more probes would never all run.
MAX_PROBES = 10
MAX_PROBE_LENGTH = 2000


def clean_probes(inputs) -> List[str]:
    """The admin's probe list, trimmed and de-duplicated in order.

    Raises ``ValueError`` on anything but a list of strings, an empty result, more than
    :data:`MAX_PROBES`, or a probe over :data:`MAX_PROBE_LENGTH` characters.
    """
    if not isinstance(inputs, list) or not all(isinstance(text, str) for text in inputs):
        raise ValueError('probes must be a list of strings')
    probes = list(dict.fromkeys(text.strip() for text in inputs if text.strip()))
    if not probes:
        raise ValueError('at least one probe is required')
    if len(probes) > MAX_PROBES:
        raise ValueError(f'at most {MAX_PROBES} probes are allowed')
    if any(len(text) > MAX_PROBE_LENGTH for text in probes):
        raise ValueError(f'a probe is at most {MAX_PROBE_LENGTH} characters')
    return probes


def probe_keys(probes: List[str], existing: dict) -> List[str]:
    """A stable key per probe: the seeded key when its text is unchanged, else a fresh ``custom_<n>``.

    ``existing`` maps each stored key to its text. Keeping the seeded key for unchanged wording
    is what lets a later re-seed refresh it instead of adding a second copy.
    """
    by_text = {text: key for key, text in existing.items()}
    used, keys = set(), []
    counter = 0
    for text in probes:
        key = by_text.get(text)
        if key is None or key in used:
            counter += 1
            while f'custom_{counter}' in used or f'custom_{counter}' in existing:
                counter += 1
            key = f'custom_{counter}'
        used.add(key)
        keys.append(key)
    return keys


def health_check_inputs(probes: List[str], starters, max_starters: int, cap: int) -> List[str]:
    """The cases of an agent's health check: the probes, then its first conversation starters."""
    picked = [text for text in (starters or []) if isinstance(text, str) and text.strip()][:max_starters]
    return list(dict.fromkeys(list(probes) + picked))[:cap]


# ----------------------------------------------------------------------------
# Dataset resolution
# ----------------------------------------------------------------------------

def resolve_dataset_tier(policy: str, suites: List[dict], default_dataset_id: Optional[int] = None,
                         project_dataset_id: Optional[int] = None) -> dict:
    """Which suite or dataset an agent is evaluated on; the first match wins.

    ``suites`` are the agent's suites as ``{'id', 'dataset_id', 'auto_evaluation'}``. Returns
    ``{'tier', 'suite_id', 'dataset_id'}``; ``suite_id`` None means "generate a suite with the
    platform dimensions on ``dataset_id``" (or on the platform dataset for the platform tier).
    ``tier`` None means Not Evaluated with ``NO_DATASET``.
    """
    platform = {'tier': TIER_PLATFORM, 'suite_id': None, 'dataset_id': None}
    if policy == POLICY_PLATFORM_ONLY:
        return platform
    with_dataset = [suite for suite in suites if suite.get('dataset_id')]
    if default_dataset_id:
        on_default = [suite for suite in with_dataset if suite['dataset_id'] == default_dataset_id]
        # A flagged suite on the default dataset keeps the team's targets; else any suite on it.
        on_default.sort(key=lambda suite: (not suite.get('auto_evaluation'), suite['id']))
        return {'tier': TIER_AGENT_DEFAULT, 'suite_id': on_default[0]['id'] if on_default else None,
                'dataset_id': default_dataset_id}
    flagged = sorted((suite for suite in with_dataset if suite.get('auto_evaluation')), key=lambda s: s['id'])
    if flagged:
        return {'tier': TIER_AGENT_SUITE, 'suite_id': flagged[0]['id'], 'dataset_id': flagged[0]['dataset_id']}
    if len(with_dataset) == 1:
        return {'tier': TIER_AGENT_ONLY_SUITE, 'suite_id': with_dataset[0]['id'],
                'dataset_id': with_dataset[0]['dataset_id']}
    if policy == POLICY_AGENT_ONLY:
        return {'tier': None, 'suite_id': None, 'dataset_id': None}
    if project_dataset_id:
        return {'tier': TIER_PROJECT, 'suite_id': None, 'dataset_id': project_dataset_id}
    return platform


# ----------------------------------------------------------------------------
# Not Evaluated: pre-flight and post-run
# ----------------------------------------------------------------------------

def preflight_reason(owner_code: Optional[str], budget_scope: Optional[str],
                     review: Optional[dict]) -> Optional[Tuple[str, str]]:
    """``(reason_code, detail)`` when the agent cannot be evaluated before spending a token.

    Checked in order: the owner's access, the budget door, then the risk scan of the version.
    """
    if owner_code:
        return OWNER_UNAVAILABLE, f'The agent owner cannot run it ({owner_code})'
    if budget_scope:
        who = 'project' if budget_scope == 'project' else "owner's"
        return BUDGET_BLOCKED, f'The {who} budget is exhausted'
    review = review or {}
    blocking = review.get('blocking') or []
    for code in _SCAN_ORDER:
        hit = next((item for item in blocking if item.get('code') == code), None)
        if hit is not None:
            return _SCAN_REASONS[code], hit.get('message') or code
    if blocking:
        return OTHER, blocking[0].get('message') or blocking[0].get('code') or 'blocked'
    incomplete = next((item for item in review.get('notices') or [] if item.get('code') == 'scan_incomplete'), None)
    if incomplete is not None:
        return OTHER, incomplete.get('message') or 'The version could not be fully checked'
    return None


def case_outcome(execution: dict) -> str:
    """One case execution reduced to what the classifier counts.

    ``execution`` is ``{'status', 'trajectory'}``; a tool step waiting for a sign-in reads as
    ``action_required`` whatever the case status says, because the run cannot sign in.
    """
    trajectory = execution.get('trajectory') or {}
    steps = trajectory.get('steps') or [] if isinstance(trajectory, dict) else []
    if any(isinstance(step, dict) and step.get('status') == 'action_required' for step in steps):
        return 'action_required'
    status = execution.get('status') or 'ok'
    if status == 'guardrail_paused':
        pause = (trajectory.get('pause') or {}) if isinstance(trajectory, dict) else {}
        return 'hitl' if (pause.get('pause_type') or 'hitl') == 'hitl' else 'paused'
    if status in _PREDICT_FAILURES:
        return 'broken'
    return status


def classify_run(run: dict, executions: List[dict], threshold: float) -> Optional[Tuple[str, str]]:
    """``(reason_code, detail)`` when a run's cases did not really evaluate the agent, else None.

    ``run`` is ``{'status', 'error', 'meta'}``. A reason wins once its share of the cases reaches
    ``threshold``; below that the run is scored and the misses only lower its coverage.
    """
    error = run.get('error') or ''
    meta = run.get('meta') or {}
    if any(_LOAD_FAILURE_MARK in (item.get('error') or '') for item in executions) or _LOAD_FAILURE_MARK in error:
        return AGENT_BROKEN, 'The agent version could not be loaded'
    if meta.get('stop_reason') == 'budget_exhausted':
        return BUDGET_BLOCKED, 'The budget ran out during the run'
    total = len(executions)
    if not total:
        if run.get('status') == 'errored':
            return AGENT_BROKEN, error[:500] or 'The run failed before any case ran'
        return None
    outcomes = [case_outcome(item) for item in executions]
    for outcome, reason, text in (
        ('hitl', HITL_REQUIRED, 'waited for a person'),
        ('action_required', OAUTH_REQUIRED, 'waited for a sign-in'),
        ('timeout', TIMEOUT, 'timed out'),
        ('broken', AGENT_BROKEN, 'failed with an error'),
    ):
        count = outcomes.count(outcome)
        if count and count / total >= threshold:
            return reason, f'{count} of {total} cases {text}'
    return None


# ----------------------------------------------------------------------------
# Verdict
# ----------------------------------------------------------------------------

def target_met(native_score: Optional[float], operator: Optional[str], target) -> Optional[bool]:
    """Same rule as ``enhancement_gap_selection.evaluate_target_met``: None when not decidable."""
    if native_score is None or operator is None or target is None or target == '':
        return None
    try:
        score, goal = float(native_score), float(target)
    except (TypeError, ValueError):
        return None
    comparator = _OPERATORS.get(operator)
    return comparator(score, goal) if comparator else None


def summarize_results(snapshot: dict, results: List[dict]) -> dict:
    """Coverage and per-binding target outcome of a finished run.

    ``results`` are the run's result rows (``dataset_case_id``, ``dimension_id``, ``platform_key``,
    ``status``, ``native_score``, ``normalized_score``). Coverage is the share of the snapshot's
    cases with at least one ``ok`` score. A binding meets its target when its mean native score
    across the scored cases does, as the scorecard's per-binding row shows it.
    """
    snapshot = snapshot or {}
    case_ids = [case.get('id') for case in snapshot.get('cases') or []]
    scored_cases = {row.get('dataset_case_id') for row in results
                    if row.get('status') == 'ok' and row.get('normalized_score') is not None}
    coverage = round(100.0 * len(scored_cases & set(case_ids)) / len(case_ids), 2) if case_ids else 0.0
    bindings = []
    for binding in snapshot.get('bindings') or []:
        key = (binding.get('dimension_id'), binding.get('platform_key'))
        scores = [float(row['native_score']) for row in results
                  if (row.get('dimension_id'), row.get('platform_key')) == key
                  and row.get('status') == 'ok' and row.get('native_score') is not None]
        mean = sum(scores) / len(scores) if scores else None
        bindings.append({
            'dimension_id': key[0], 'platform_key': key[1], 'weight': binding.get('weight'),
            'mean_native': mean,
            'met': target_met(mean, binding.get('target_operator'), binding.get('target')),
        })
    return {'coverage': coverage, 'bindings': bindings}


def verdict(headline: Optional[float], coverage: float, bindings: List[dict], settings: dict) -> Tuple[str, str]:
    """``(status, detail)``: Passed, Warning or Failed for a run that did evaluate the agent.

    Failed below ``fail_threshold``. Otherwise Warning on partial coverage or a missed target of
    a weighted binding, else Passed. A binding with no target, or weight 0, cannot fail it.
    """
    if headline is None:
        return NOT_EVALUATED, 'No case was scored'
    if headline < settings['fail_threshold']:
        return FAILED, f"Score {headline:g} is below {settings['fail_threshold']:g}"
    if coverage < settings['coverage_threshold']:
        return WARNING, f"Only {coverage:g}% of the cases were scored"
    missed = [binding for binding in bindings
              if binding.get('met') is False and (binding.get('weight') is None or binding['weight'] > 0)]
    if missed:
        return WARNING, f'{len(missed)} target(s) missed'
    return PASSED, 'All targets met'
