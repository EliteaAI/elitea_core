"""Hides the chat text of a history run's private cases from everyone but the chat's author and the
project admins.

A history run copies user turns into its snapshot, and the judge's rationale and the recorded
trajectory can quote them. The scores stay visible to every reader of the run; only the text is
replaced. Pure functions over the dicts the run, results and case-execution endpoints return; the
admin check is the one wrapper that calls out.
"""
from typing import Callable, Iterable, Optional, Set

PRIVATE_PLACEHOLDER = 'Private conversation — content hidden'
# Allow-lists: what a hidden case keeps. Anything else is text the chat may have put there, so a
# key added later is hidden by default. A kept string is a placeholder; other values are dropped.
_CASE_KEPT = ('id', 'order_index')
_CASE_TEXT = ('input', 'output')
_VERDICT_KEPT = ('passed', 'status', 'native_score', 'normalized_score', 'not_inspected', 'execution_time',
                 'reason_code', 'ok', 'verdict')
_EXECUTION_KEPT = ('id', 'run_id', 'dataset_case_id', 'case_index', 'status', 'trajectory_state',
                   'trajectory_state_reason', 'created_at')


def _is_count(value) -> bool:
    return value is None or isinstance(value, (bool, int, float))


def _same_user(left, right) -> bool:
    # The author id comes from JSON participant meta and may be a string; the viewer id is an int.
    return left is not None and right is not None and str(left) == str(right)


def _masked(values: dict, kept: Iterable[str] = ()) -> dict:
    """``values`` with the ``kept`` scalars as they are, other strings replaced, the rest dropped."""
    kept = set(kept)
    out = {}
    for key, value in (values or {}).items():
        if key in kept and (_is_count(value) or isinstance(value, str)):
            out[key] = value
        elif isinstance(value, str):
            out[key] = PRIVATE_PLACEHOLDER if value else value
    return out


def private_case_ids(snapshot: Optional[dict], viewer_id: Optional[int]) -> Set[int]:
    """The ids of the history cases from private chats the viewer did not write."""
    snapshot = snapshot or {}
    if snapshot.get('trigger_type') != 'history':
        return set()
    hidden = set()
    for case in snapshot.get('cases') or []:
        source = case.get('source') or {}
        if source.get('is_private') and not _same_user(source.get('author_id'), viewer_id):
            hidden.add(case.get('id'))
    return hidden


def hidden_case_ids(snapshot: Optional[dict], viewer_id: Optional[int],
                    is_admin: Callable[[], bool]) -> Set[int]:
    """:func:`private_case_ids`, empty for a project admin; ``is_admin`` is asked only when needed."""
    hidden = private_case_ids(snapshot, viewer_id)
    if hidden and is_admin():
        return set()
    return hidden


def redact_snapshot(snapshot: dict, hidden: Iterable[int]) -> dict:
    hidden = set(hidden)
    if not hidden or not snapshot:
        return snapshot
    cases = []
    for case in snapshot.get('cases') or []:
        if case.get('id') in hidden:
            # ``source`` names the chat and its author, so only the private flag is left of it.
            case = {**{key: case.get(key) for key in _CASE_KEPT if key in case},
                    **{key: PRIVATE_PLACEHOLDER if case.get(key) else case.get(key) for key in _CASE_TEXT},
                    'expected_output': None, 'structure': None,
                    'source': {'is_private': True}, 'redacted': True}
        cases.append(case)
    return {**snapshot, 'cases': cases}


def redact_result(result: dict, hidden: Iterable[int]) -> dict:
    """One result row: its scores kept, the judge's text and the evidence it saw replaced."""
    if result.get('dataset_case_id') not in set(hidden):
        return result
    return {**result, 'verdict': _masked(result.get('verdict'), _VERDICT_KEPT),
            'evidence': _masked(result.get('evidence')), 'redacted': True}


def redact_execution(row: dict, hidden: Iterable[int]) -> dict:
    """One case execution: its state and numeric counters kept; the trajectory and the metrics that
    name tools, models or nodes dropped."""
    if row.get('dataset_case_id') not in set(hidden):
        return row
    metrics = {key: value for key, value in (row.get('metrics') or {}).items() if _is_count(value)}
    return {**{key: row.get(key) for key in _EXECUTION_KEPT if key in row},
            'trajectory': None, 'metrics': metrics, 'redacted': True}


def redact_human_score(row: dict, hidden: Iterable[int]) -> dict:
    """One human annotation: the score kept, the reviewer's note replaced."""
    if row.get('dataset_case_id') not in set(hidden):
        return row
    return {**row, 'note': PRIVATE_PLACEHOLDER if row.get('note') else row.get('note'), 'redacted': True}


def viewer_hidden_case_ids(project_id: int, snapshot: Optional[dict], viewer_id: Optional[int]) -> Set[int]:
    """:func:`hidden_case_ids` for the current request's user."""
    def is_admin() -> bool:
        from .project_settings_access import can_manage_project_settings  # pylint: disable=C0415
        return can_manage_project_settings(project_id, viewer_id)
    return hidden_case_ids(snapshot, viewer_id, is_admin)


def without_hidden_cases(run: dict, hidden: Iterable[int]) -> dict:
    """The run read for enhancement, minus the hidden cases: their text, rationale and scores.

    Enhancement sends what it reads to an LLM and returns what the LLM wrote, so a placeholder would
    still leave the scores to reason over and a proposal citing cases the viewer cannot open.
    """
    hidden = set(hidden)
    if not hidden:
        return run
    snapshot = run.get('snapshot') or {}
    return {
        **run,
        'snapshot': {**snapshot,
                     'cases': [case for case in snapshot.get('cases') or [] if case.get('id') not in hidden]},
        'results': [row for row in run.get('results') or [] if row.get('dataset_case_id') not in hidden],
        'human_scores': [row for row in run.get('human_scores') or []
                         if row.get('dataset_case_id') not in hidden],
    }
