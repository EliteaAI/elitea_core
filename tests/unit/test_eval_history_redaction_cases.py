"""Who sees the chat text of a history run: the scores are shown to every reader, the text of a case
from a private chat only to its author and the project admins."""
import importlib.util
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def redaction():
    spec = importlib.util.spec_from_file_location('eval_history_redaction_under_test',
                                                  PLUGIN_ROOT / 'utils' / 'eval_history_redaction.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _snapshot(trigger='history'):
    return {'trigger_type': trigger, 'cases': [
        {'id': 1, 'input': 'my salary is 5000', 'output': 'Noted.', 'source': {'is_private': True, 'author_id': 42}},
        {'id': 2, 'input': 'shared question', 'output': 'Answer', 'source': {'is_private': False, 'author_id': 42}},
        {'id': 3, 'input': 'other private', 'output': '', 'source': {'is_private': True, 'author_id': 7}},
    ]}


def test_private_cases_of_other_authors_are_hidden(redaction):
    assert redaction.private_case_ids(_snapshot(), 42) == {3}
    assert redaction.private_case_ids(_snapshot(), 7) == {1}
    assert redaction.private_case_ids(_snapshot(), None) == {1, 3}


def test_author_id_from_json_meta_matches_an_int_viewer(redaction):
    snapshot = {'trigger_type': 'history', 'cases': [
        {'id': 1, 'input': 'x', 'source': {'is_private': True, 'author_id': '42'}}]}
    assert redaction.private_case_ids(snapshot, 42) == set()
    assert redaction.private_case_ids(snapshot, '42') == set()
    assert redaction.private_case_ids(snapshot, 7) == {1}
    no_author = {'trigger_type': 'history', 'cases': [{'id': 1, 'source': {'is_private': True}}]}
    assert redaction.private_case_ids(no_author, None) == {1}


def test_only_history_runs_are_redacted(redaction):
    assert redaction.private_case_ids(_snapshot('scheduled'), 99) == set()
    assert redaction.private_case_ids(None, 99) == set()


def test_admin_sees_everything_and_is_asked_only_when_needed(redaction):
    calls = []

    def is_admin():
        calls.append(1)
        return True

    assert redaction.hidden_case_ids(_snapshot(), 99, is_admin) == set()
    assert calls == [1]
    redaction.hidden_case_ids(_snapshot('scheduled'), 99, is_admin)
    assert calls == [1]
    assert redaction.hidden_case_ids(_snapshot(), 99, lambda: False) == {1, 3}


def test_redact_snapshot(redaction):
    redacted = redaction.redact_snapshot(_snapshot(), {1, 3})
    first, second, third = redacted['cases']
    assert first['input'] == first['output'] == redaction.PRIVATE_PLACEHOLDER and first['redacted'] is True
    # Neither the chat nor its author is named for a hidden case.
    assert first['source'] == {'is_private': True}
    assert set(first) == {'id', 'input', 'output', 'expected_output', 'structure', 'source', 'redacted'}
    assert second['input'] == 'shared question' and 'redacted' not in second
    # An empty output stays empty rather than reading as hidden content.
    assert third['output'] == ''
    original = _snapshot()
    assert redaction.redact_snapshot(original, set()) is original


def test_redact_result_keeps_scores(redaction):
    result = {'dataset_case_id': 1, 'native_score': 4, 'normalized_score': 75, 'status': 'ok',
              'verdict': {'rationale': 'The user said their salary is 5000', 'passed': True, 'stdout': '',
                          'judge_raw': {'quote': 'salary 5000'}, 'summary': 'salary 5000', 'status': 'ok'},
              'evidence': {'input': 'my salary is 5000', 'output': 'Noted.', 'trajectory': {'steps': [1]},
                           'usage': {'tokens': 3}, 'tool_sequence': ['get_salary']}}
    redacted = redaction.redact_result(result, {1})
    assert redacted['normalized_score'] == 75 and redacted['native_score'] == 4
    # Allow-list: an unknown text key is replaced and an unknown structure dropped.
    assert redacted['verdict'] == {'rationale': redaction.PRIVATE_PLACEHOLDER, 'passed': True, 'stdout': '',
                                   'summary': redaction.PRIVATE_PLACEHOLDER, 'status': 'ok'}
    assert redacted['evidence'] == {'input': redaction.PRIVATE_PLACEHOLDER, 'output': redaction.PRIVATE_PLACEHOLDER}
    assert redaction.redact_result({**result, 'dataset_case_id': 2}, {1})['verdict']['rationale'].startswith('The user')


def test_redact_execution_keeps_only_counters(redaction):
    row = {'id': 9, 'dataset_case_id': 3, 'status': 'ok', 'trajectory_state': 'recorded',
           'trajectory': {'steps': [{'tool_inputs': 'secret'}]},
           'metrics': {'tool_calls': 1, 'step_limit_hit': False, 'tool_sequence': ['get_salary'],
                       'agents': [{'name': 'HR', 'models': ['m']}], 'node_path': ['a']},
           'extra': 'unknown text'}
    redacted = redaction.redact_execution(row, {3})
    assert redacted['trajectory'] is None and redacted['redacted']
    assert redacted['metrics'] == {'tool_calls': 1, 'step_limit_hit': False}
    assert 'extra' not in redacted and redacted['status'] == 'ok' and redacted['id'] == 9
    assert redaction.redact_execution(row, {1}) is row


def test_redact_human_score_hides_the_note(redaction):
    row = {'dataset_case_id': 3, 'normalized_score': 50, 'note': 'quotes the salary', 'reviewer_id': 5}
    redacted = redaction.redact_human_score(row, {3})
    assert redacted['note'] == redaction.PRIVATE_PLACEHOLDER and redacted['normalized_score'] == 50
    assert redaction.redact_human_score({**row, 'note': None}, {3})['note'] is None
    assert redaction.redact_human_score(row, {1}) is row


def test_enhancement_read_leaves_hidden_cases_out(redaction):
    run = {
        'run_id': 5,
        'snapshot': _snapshot(),
        'results': [{'dataset_case_id': 1, 'score': 0.2}, {'dataset_case_id': 2, 'score': 0.4},
                    {'dataset_case_id': 3, 'score': 0.1}],
        'human_scores': [{'dataset_case_id': 3, 'dimension_id': 'd1', 'score': 0.0}],
    }
    trimmed = redaction.without_hidden_cases(run, {1, 3})
    assert [case['id'] for case in trimmed['snapshot']['cases']] == [2]
    assert [row['dataset_case_id'] for row in trimmed['results']] == [2]
    assert trimmed['human_scores'] == []
    assert trimmed['snapshot']['trigger_type'] == 'history' and trimmed['run_id'] == 5
    # The input is not changed, and nothing hidden returns it as is.
    assert len(run['snapshot']['cases']) == 3
    assert redaction.without_hidden_cases(run, set()) is run
