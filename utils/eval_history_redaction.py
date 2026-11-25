"""Hides the chat text of a history run's private cases from everyone but the chat's author and the
project admins.

A history run copies user turns into its snapshot, and the judge's rationale and the recorded
trajectory can quote them. The scores stay visible to every reader of the run; only the text is
replaced. Pure functions over the dicts the run, results and case-execution endpoints return; the
admin check is the one wrapper that calls out.
"""
from typing import Callable, Iterable, Optional, Set

PRIVATE_PLACEHOLDER = 'Private conversation — content hidden'
_CASE_TEXT = ('input', 'output')
_VERDICT_TEXT = ('rationale', 'stdout', 'stderr', 'error', 'note')
_EVIDENCE_TEXT = ('input', 'output', 'expected_output')
# Structured evidence that carries tool inputs and outputs: dropped rather than rewritten.
_EVIDENCE_DROPPED = ('trajectory', 'expected_trajectory', 'structure')


def private_case_ids(snapshot: Optional[dict], viewer_id: Optional[int]) -> Set[int]:
    """The ids of the history cases from private chats the viewer did not write."""
    snapshot = snapshot or {}
    if snapshot.get('trigger_type') != 'history':
        return set()
    hidden = set()
    for case in snapshot.get('cases') or []:
        source = case.get('source') or {}
        if source.get('is_private') and (viewer_id is None or source.get('author_id') != viewer_id):
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
            case = {**case, **{key: PRIVATE_PLACEHOLDER for key in _CASE_TEXT if case.get(key)},
                    'redacted': True}
        cases.append(case)
    return {**snapshot, 'cases': cases}


def redact_result(result: dict, hidden: Iterable[int]) -> dict:
    """One result row: its scores kept, the judge's text and the evidence it saw replaced."""
    if result.get('dataset_case_id') not in set(hidden):
        return result
    verdict = {key: (PRIVATE_PLACEHOLDER if key in _VERDICT_TEXT and value else value)
               for key, value in (result.get('verdict') or {}).items()}
    evidence = {key: (PRIVATE_PLACEHOLDER if key in _EVIDENCE_TEXT and value else value)
                for key, value in (result.get('evidence') or {}).items() if key not in _EVIDENCE_DROPPED}
    return {**result, 'verdict': verdict, 'evidence': evidence, 'redacted': True}


def redact_execution(row: dict, hidden: Iterable[int]) -> dict:
    """One case execution: its counters kept, the trajectory (tool inputs and outputs) dropped."""
    if row.get('dataset_case_id') not in set(hidden):
        return row
    return {**row, 'trajectory': None, 'redacted': True}


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
