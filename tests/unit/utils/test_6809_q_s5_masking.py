"""#6809 design Q-S5: what a stored trajectory may hold.

Secrets are masked when the trajectory is built (by key at any depth, and by known format inside
text), before the per-step cap, so a cap cannot leave half a secret. A suite's
``meta.store_tool_outputs = false`` keeps tool names and masked inputs but drops every output; the
flag is frozen into the run snapshot.

Run via:
    python tests/run_tests.py unit/utils/test_6809_q_s5_masking.py -v
"""

import json
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402

masking = sys.modules['plugins.elitea_core.utils.evaluation_masking']
checks = sys.modules['plugins.elitea_core.utils.evaluation_trajectory_checks']
MASK = masking.MASK_VALUE
OPENAI_KEY = 'sk-' + 'A1b2C3d4' * 4
PAT = 'ghp_' + 'x' * 36


@pytest.fixture(scope='module')
def execution(utils_path):
    load_utils_module(utils_path, 'evaluation_usage')
    return load_utils_module(utils_path, 'evaluation_execution')


@pytest.fixture(scope='module')
def runner(utils_path, execution):
    return load_utils_module(utils_path, 'evaluation_agent_runner')


@pytest.fixture(scope='module')
def judge(utils_path):
    return load_utils_module(utils_path, 'evaluation_ai_judge')


@pytest.fixture(scope='module')
def orchestration(utils_path, execution, judge):
    load_utils_module(utils_path, 'evaluation_scoring')
    return load_utils_module(utils_path, 'evaluation_run_orchestration')


@pytest.fixture(scope='module')
def cv(utils_path):
    load_utils_module(utils_path, 'evaluation_code_screen')
    return load_utils_module(utils_path, 'code_validation')


def _entry(name, inputs=None, output='done', *, start='2026-10-12T10:00:00', error=None, **extra):
    return {'tool_name': name, 'tool_inputs': inputs, 'tool_output': output, 'error': error,
            'finish_reason': 'error' if error else 'stop', 'timestamp_start': start,
            'timestamp_finish': start, 'metadata': {}, **extra}


def _envelope(*entries, text='answer'):
    return {'result': {
        'thinking_steps': [{'text': text, 'message': {}, 'generation_info': {},
                            'timestamp_start': '2026-10-12T09:59:59'}],
        'tool_calls_dict': {str(i): e for i, e in enumerate(entries)},
        'chat_history': [{'role': 'assistant', 'content': text}],
    }}


def _tools(out):
    return [s for s in out['trajectory']['steps'] if s['kind'] == 'tool']


# --- the helper ------------------------------------------------------------------------------

def test_sensitive_keys_are_masked_at_any_depth():
    value = {'query': 'open issues', 'GitHub_Token': 'abc', 'headers': {'X-Api-Key': 'k', 'accept': 'json'},
             'items': [{'password': 'p', 'name': 'n'}], 'secret': None}
    assert masking.mask_value(value) == {
        'query': 'open issues', 'GitHub_Token': MASK, 'headers': {'X-Api-Key': MASK, 'accept': 'json'},
        'items': [{'password': MASK, 'name': 'n'}], 'secret': None}


def test_secret_formats_are_replaced_inside_text():
    text = f'called with {OPENAI_KEY} and header Bearer abc.def-123; token {PAT} done'
    assert masking.mask_text(text) == f'called with {MASK} and header {MASK}; token {MASK} done'


def test_serialized_json_is_masked_as_structure():
    masked = json.loads(masking.mask_payload('{"user": "u", "access_token": "t", "rows": [1]}'))
    assert masked == {'user': 'u', 'access_token': MASK, 'rows': [1]}
    assert masking.mask_payload('{not json, Bearer abc}') == '{not json, ' + MASK + '}'
    assert masking.mask_payload(None) is None and masking.mask_payload(3) == 3


def test_the_key_set_is_the_tracing_one():
    """Copied, not imported (design §12 debt, #6886); a drift shows up here first."""
    tracing = TESTS_DIR.parents[1] / 'tracing' / 'utils' / 'payload_capture.py'
    if not tracing.exists():
        pytest.skip('tracing plugin not checked out next to elitea_core')
    source = tracing.read_text()
    block = source.split('DEFAULT_SENSITIVE_KEYS = {', 1)[1].split('}', 1)[0]
    assert {k.strip().strip("'\"") for k in block.replace('\n', ' ').split(',') if k.strip()} \
        == set(masking.SENSITIVE_KEYS)
    for pattern in masking.SECRET_PATTERNS:
        assert repr(pattern.pattern)[1:-1] in source or pattern.pattern in source


# --- the builder -----------------------------------------------------------------------------

def test_trajectory_masks_inputs_outputs_errors_and_llm_text(execution):
    out = execution.extract_execution(_envelope(
        _entry('jira', {'jql': 'project = X', 'api_key': 'live-key'},
               output=f'{{"issues": [], "session": "s1", "note": "{OPENAI_KEY}"}}'),
        _entry('http', {'url': 'https://h'}, output='', start='2026-10-12T10:00:01',
               error=f'401 for Bearer {PAT}'),
        text=f'I will use {OPENAI_KEY}'), status='ok')
    jira, http = _tools(out)
    assert jira['tool_inputs'] == {'jql': 'project = X', 'api_key': MASK}
    assert json.loads(jira['tool_output']) == {'issues': [], 'session': MASK, 'note': MASK}
    assert http['error'] == f'401 for {MASK}' and http['status'] == 'error'
    llm = out['trajectory']['steps'][0]
    assert llm['text'] == f'I will use {MASK}'
    assert OPENAI_KEY not in json.dumps(out) and PAT not in json.dumps(out)


def test_masking_runs_before_the_cap(execution):
    """A secret straddling the 4 KB cap must not survive as a visible prefix."""
    output = 'a' * (execution.MAX_STEP_TEXT - 10) + OPENAI_KEY + ' tail'
    step = _tools(execution.extract_execution(_envelope(_entry('read', output=output)), status='ok'))[0]
    assert 'sk-' not in step['tool_output']
    assert step['tool_output'].startswith('a' * 100) and step['tool_output'].endswith('[truncated]')


def test_calls_differing_only_in_a_masked_value_are_not_redundant(execution):
    out = execution.extract_execution(_envelope(
        _entry('login', {'user': 'a', 'password': 'one'}),
        _entry('login', {'user': 'a', 'password': 'two'}, start='2026-10-12T10:00:01'),
        _entry('login', {'user': 'a', 'password': 'two'}, start='2026-10-12T10:00:02'),
    ), status='ok')
    assert out['metrics']['redundant_calls'] == 1  # only the true repeat
    assert all('_identity' not in s for s in out['trajectory']['steps'])


def test_store_tool_outputs_false_drops_outputs_but_keeps_names_and_inputs(execution):
    out = execution.extract_execution(_envelope(
        _entry('search', {'q': 'x', 'token': 't'}, output='private rows'),
        _entry('fail', {}, output='', start='2026-10-12T10:00:01', error='boom'),
    ), status='ok', store_tool_outputs=False)
    search, fail = _tools(out)
    assert search['tool_output'] is None and search['output_omitted'] is True
    assert search['tool_inputs'] == {'q': 'x', 'token': MASK}
    assert fail['status'] == 'error' and fail['error'] == 'boom'
    assert out['trajectory']['tool_sequence'] == ['search', 'fail']
    assert out['trajectory']['tool_outputs_stored'] is False
    assert out['metrics']['tool_errors'] == 1


def test_outputs_are_stored_by_default(execution):
    out = execution.extract_execution(_envelope(_entry('search', output='rows')), status='ok')
    step = _tools(out)[0]
    assert step['tool_output'] == 'rows' and 'output_omitted' not in step
    assert out['trajectory']['tool_outputs_stored'] is True


# --- the flag's path: suite meta → snapshot → runner -----------------------------------------

def test_snapshot_freezes_the_flag(orchestration):
    common = dict(dimensions=[], bindings=[], cases=[], application_id=1, application_version_id=2)
    frozen = lambda **suite: orchestration.build_run_snapshot(  # noqa: E731
        suite={'id': 1, 'name': 's', **suite}, **common)['suite']['store_tool_outputs']
    assert frozen(store_tool_outputs=False) is False
    assert frozen(store_tool_outputs=True) is True
    assert frozen(store_tool_outputs=None) is True
    assert frozen() is True


def test_run_agent_applies_the_flag(runner):
    envelope = _envelope(_entry('search', output='rows'))
    kept = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: envelope)
    dropped = runner.run_agent(1, {'agent_type': 'openai'}, {'input': 'q'}, predict=lambda **kw: envelope,
                               store_tool_outputs=False)
    assert _tools(kept['execution'])[0]['tool_output'] == 'rows'
    assert _tools(dropped['execution'])[0]['tool_output'] is None


def test_agent_runner_reads_the_frozen_flag(orchestration, monkeypatch):
    """``_make_agent_runner`` passes the snapshot's flag; a pre-flag snapshot keeps outputs."""
    seen = []
    runner_mod = sys.modules['plugins.elitea_core.utils.evaluation_agent_runner']
    app_utils = type(sys)('plugins.elitea_core.utils.application_utils')
    app_utils.get_application_version_details_expanded = lambda *a: {'agent_type': 'openai'}
    monkeypatch.setitem(sys.modules, app_utils.__name__, app_utils)
    monkeypatch.setattr(runner_mod, 'run_agent',
                        lambda *a, **kw: seen.append(kw['store_tool_outputs']) or {'status': 'ok'})
    for suite in ({'store_tool_outputs': False}, {}):
        run = orchestration._make_agent_runner(
            1, {'suite': suite, 'application_id': 1, 'application_version_id': 2, 'dataset_id': 3},
            user_id=1)
        run({'input': 'q'})
    assert seen == [False, True]


# --- readers ---------------------------------------------------------------------------------

def test_judge_is_told_the_output_was_not_stored(execution, judge):
    out = execution.extract_execution(_envelope(_entry('search', output='rows')), status='ok',
                                      store_tool_outputs=False)
    text = judge.render_trajectory(out['trajectory'])
    assert 'output: (not stored for this suite)' in text and 'rows' not in text


def test_judge_does_not_claim_an_opt_out_for_a_size_cut(judge):
    trajectory = {'steps': [{'kind': 'tool', 'tool_name': 't', 'tool_output': None,
                             'output_omitted': True, 'status': 'ok'}],
                  'truncated': True, 'tool_outputs_stored': True}
    assert 'not stored' not in judge.render_trajectory(trajectory)


def _run_tool_match(cv, trajectory, expected):
    prelude = cv.build_validation_prelude(checks.script_for('trajectory.tool_match'), output='o',
                                          trajectory=trajectory, expected_trajectory=expected)
    ns = {}
    exec('\n'.join(prelude.splitlines()[:-1]), ns)  # noqa: S102 - trusted, shipped script
    return ns.get('result')


@pytest.mark.parametrize('args_match', ['subset', 'exact'])
def test_a_masked_argument_matches_on_presence(cv, args_match):
    trajectory = {'steps': [{'kind': 'tool', 'tool_name': 'login', 'status': 'ok',
                             'tool_inputs': {'user': 'a', 'password': MASK}}]}
    expected = {'match': 'superset', 'forbidden': [], 'allow_repeat': [],
                'tools': [{'name': 'login', 'args': {'user': 'a', 'password': 'hunter2'},
                           'args_match': args_match}]}
    assert _run_tool_match(cv, trajectory, expected) == 1.0
    expected['tools'][0]['args']['user'] = 'b'
    assert _run_tool_match(cv, trajectory, expected) == 0.0


def test_exact_args_still_reject_a_missing_or_extra_key(cv):
    trajectory = {'steps': [{'kind': 'tool', 'tool_name': 'login', 'status': 'ok',
                             'tool_inputs': {'user': 'a', 'password': MASK}}]}
    expected = {'match': 'superset', 'forbidden': [], 'allow_repeat': [],
                'tools': [{'name': 'login', 'args': {'user': 'a'}, 'args_match': 'exact'}]}
    assert _run_tool_match(cv, trajectory, expected) == 0.0


def test_the_script_mask_constant_is_the_helper_one():
    assert f"_MASKED = '{MASK}'" in checks.script_for('trajectory.tool_match')
