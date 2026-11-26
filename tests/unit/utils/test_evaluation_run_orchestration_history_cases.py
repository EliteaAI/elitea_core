"""History runs: sampled chat turns become snapshot cases, and each case carries the execution its
stored turn recorded, so the run scores it without invoking the agent."""
import pathlib
import sys
from datetime import datetime

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def orch(utils_path):
    load_utils_module(utils_path, 'evaluation_scoring')
    load_utils_module(utils_path, 'evaluation_ai_judge')
    load_utils_module(utils_path, 'evaluation_execution')
    load_utils_module(utils_path, 'evaluation_usage')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


CREATED = datetime(2026, 11, 20, 10, 0)


def _turn(id_, **overrides):
    turn = {
        'id': id_, 'conversation_id': 3, 'user_id': 42, 'is_private': True, 'version_id': 9,
        'created_at': CREATED, 'input': 'Use key sk-abcdefghijklmnopqrstuvwx please', 'output': 'Done.',
        'group_ids': [id_ + 100], 'latency_ms': 1500,
    }
    turn.update(overrides)
    return turn


def _tool_row(name):
    return {'kind': 'tool_call', 'tool_name': name, 'tool_inputs': {}, 'tool_output': 'ok',
            'finish_reason': 'stop', 'is_error': False, 'started_at': CREATED, 'finished_at': CREATED,
            'parent_agent_name': None, 'parent_agent_call_id': None, 'attrs': None}


def test_cases_from_history_shape(orch):
    cases = orch.cases_from_history([_turn(1), _turn(2, input=None, output=None, is_private=False)])
    assert [c['id'] for c in cases] == [1, 2]
    assert [c['order_index'] for c in cases] == [0, 1]
    first = cases[0]
    assert first['expected_output'] is None and first['structure'] is None
    assert first['source'] == {
        'conversation_id': 3, 'group_ids': [101], 'created_at': CREATED.isoformat(), 'version_id': 9,
        'author_id': 42, 'is_private': True, 'latency_ms': 1500,
    }
    assert cases[1]['input'] == '' and cases[1]['output'] == ''
    assert cases[1]['source']['is_private'] is False


def test_cases_from_history_masks_text(orch):
    case = orch.cases_from_history([_turn(1)])[0]
    assert 'abcdefghijklmnop' not in case['input']
    assert case['input'].startswith('Use key')


def test_snapshot_keeps_case_source(orch):
    cases = orch.cases_from_history([_turn(1)])
    snapshot = orch.build_run_snapshot(
        suite={'id': 1, 'name': 'S'}, dimensions=[], bindings=[], cases=cases,
        application_id=10, application_version_id=9, trigger_type=orch.TRIGGER_HISTORY)
    assert snapshot['cases'][0]['source']['conversation_id'] == 3
    assert snapshot['trigger_type'] == 'history'


def test_without_usage_bindings(orch):
    bindings = [
        {'dimension_id': 1, 'evidence_scope': {'output': True}},
        {'dimension_id': 2, 'evidence_scope': {'usage': True, 'output': True}},
        {'dimension_id': 3},
    ]
    assert [b['dimension_id'] for b in orch.without_usage_bindings(bindings)] == [1, 3]


def test_attach_recorded_executions(orch):
    cases = orch.cases_from_history([_turn(1, group_ids=[101, 102]), _turn(2, group_ids=[200])])
    cases.append({'id': 99, 'input': 'q', 'output': 'a'})
    rows = {101: [_tool_row('search')], 102: [_tool_row('read_file')]}
    attached = orch.attach_recorded_executions(cases, rows)
    first = attached[0]['_execution']
    assert first['case_index'] == 0 and first['trajectory_state'] == 'recorded'
    assert first['trajectory']['tool_sequence'] == ['search', 'read_file']
    assert first['metrics']['latency_ms'] == 1500
    # A turn with no trace rows still has a recorded, empty trajectory.
    assert attached[1]['_execution']['trajectory']['steps'] == []
    assert attached[1]['_execution']['case_index'] == 1
    # A case that did not come from history is left as it was.
    assert '_execution' not in attached[2]


def test_attached_execution_becomes_a_row(orch):
    attached = orch.attach_recorded_executions(orch.cases_from_history([_turn(1)]), {101: [_tool_row('search')]})
    row = orch.case_execution_row(attached[0], 0)
    assert row is not None and row['dataset_case_id'] == 1
