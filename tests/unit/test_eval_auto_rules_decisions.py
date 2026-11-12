"""Unit tests for ``utils/eval_auto_rules.py``: the scheduled evaluation's scope, dataset tier,
Not Evaluated reasons and verdict. The module is stdlib-only, so it is loaded by path."""
import importlib.util
import pathlib
from datetime import datetime, timedelta

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def rules():
    spec = importlib.util.spec_from_file_location(
        'eval_auto_rules_under_test', PLUGIN_ROOT / 'utils' / 'eval_auto_rules.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def settings(rules):
    return rules.effective_config({})


# --- settings ---------------------------------------------------------------

def test_effective_config_defaults(rules):
    cfg = rules.effective_config(None)
    assert 'enabled' not in cfg
    assert cfg['max_concurrent_runs'] == 2
    assert cfg['max_cases_per_agent'] == 10
    assert cfg['include_personal_projects'] is False


def test_effective_config_clamps_and_drops_bad_values(rules):
    cfg = rules.effective_config({
        'max_concurrent_runs': 500, 'fail_threshold': 'x', 'dataset_policy': 'nope',
        'project_ids': [3, '4', 'a', 3], 'unknown': 1, 'window': {'start': '25:00', 'end': '06:00'},
        'enabled': True,
    })
    assert cfg['max_concurrent_runs'] == 50
    assert cfg['fail_threshold'] == 50.0
    assert cfg['dataset_policy'] == 'agent_first'
    assert cfg['project_ids'] == [3, 4]
    assert 'unknown' not in cfg
    assert cfg['window'] is None
    # The Runtime config's eval_auto_run_enabled is the one switch.
    assert 'enabled' not in cfg


def test_a_save_rejects_wrong_types_and_unknown_choices(rules):
    assert rules.invalid_settings({
        'max_concurrent_runs': 'abc', 'project_scope_mode': 'bogus', 'project_ids': [1, 'a'],
        'window': {'start': '25:00', 'end': '06:00'}, 'include_personal_projects': 'yes',
        'case_concurrency': True,
    }) == ['case_concurrency', 'include_personal_projects', 'max_concurrent_runs', 'project_ids',
           'project_scope_mode', 'window']


def test_a_save_clamps_out_of_range_numbers_and_ignores_unknown_keys(rules):
    assert rules.invalid_settings({
        'max_concurrent_runs': 0, 'fail_threshold': 500, 'project_ids': [4, '5'], 'window': None,
        'project_scope_mode': 'allow', 'unknown': 'x', 'enabled': True,
    }) == []


@pytest.mark.parametrize('hour,minute,expected', [(23, 0, True), (2, 30, True), (6, 0, False), (12, 0, False)])
def test_window_spans_midnight(rules, hour, minute, expected):
    window = {'start': '22:00', 'end': '06:00'}
    assert rules.in_window(window, datetime(2026, 11, 11, hour, minute)) is expected


def test_no_window_is_always_open(rules):
    assert rules.in_window(None, datetime(2026, 11, 11, 12, 0)) is True


# --- scope ------------------------------------------------------------------

def test_personal_projects_excluded_by_default(rules, settings):
    assert rules.project_in_scope({'id': 9, 'name': 'project_user_42'}, settings) is False
    assert rules.project_in_scope({'id': 9, 'name': 'Team'}, settings) is True
    assert rules.project_in_scope({'id': 9, 'name': 'project_user_42'},
                                  {**settings, 'include_personal_projects': True}) is True


def test_allow_and_deny_lists(rules, settings):
    allow = {**settings, 'project_scope_mode': 'allow', 'project_ids': [2]}
    deny = {**settings, 'project_scope_mode': 'deny', 'project_ids': [2]}
    assert rules.project_in_scope({'id': 2, 'name': 'a'}, allow) is True
    assert rules.project_in_scope({'id': 3, 'name': 'b'}, allow) is False
    assert rules.project_in_scope({'id': 2, 'name': 'a'}, deny) is False
    assert rules.project_in_scope({'id': 3, 'name': 'b'}, deny) is True


def test_needs_evaluation(rules):
    now = datetime(2026, 11, 11)
    assert rules.needs_evaluation(5, None, now, 28) == (True, 'new')
    assert rules.needs_evaluation(6, {'application_version_id': 5, 'finished_at': now}, now, 28) == (True, 'changed')
    old = {'application_version_id': 5, 'finished_at': now - timedelta(days=28)}
    assert rules.needs_evaluation(5, old, now, 28) == (True, 'stale')
    recent = {'application_version_id': 5, 'finished_at': now - timedelta(days=3)}
    assert rules.needs_evaluation(5, recent, now, 28) == (False, 'fresh')


def test_oldest_first_puts_never_evaluated_first(rules):
    items = [{'id': 1, 'last_finished_at': datetime(2026, 11, 1)},
             {'id': 2, 'last_finished_at': None},
             {'id': 3, 'last_finished_at': datetime(2026, 10, 1)}]
    assert [item['id'] for item in rules.oldest_first(items)] == [2, 3, 1]


def test_stable_case_sample(rules):
    ids = list(range(1, 51))
    first = rules.stable_case_sample(ids, 10, salt='p1:a7')
    assert len(first) == 10
    assert first == rules.stable_case_sample(ids, 10, salt='p1:a7')
    assert first == sorted(first)
    assert first != ids[:10]
    assert rules.stable_case_sample([1, 2], 10) == [1, 2]


# --- dataset tier -----------------------------------------------------------

def test_platform_only_ignores_suites(rules):
    out = rules.resolve_dataset_tier('platform_only', [{'id': 1, 'dataset_id': 5, 'auto_evaluation': True}], 5)
    assert out['tier'] == 'platform'


def test_default_dataset_uses_suite_on_it(rules):
    suites = [{'id': 1, 'dataset_id': 7}, {'id': 2, 'dataset_id': 5}, {'id': 3, 'dataset_id': 5, 'auto_evaluation': True}]
    assert rules.resolve_dataset_tier('agent_first', suites, 5) == {
        'tier': 'agent_default', 'suite_id': 3, 'dataset_id': 5}


def test_default_dataset_without_suite_generates_one(rules):
    assert rules.resolve_dataset_tier('agent_first', [{'id': 1, 'dataset_id': 7}], 5) == {
        'tier': 'agent_default', 'suite_id': None, 'dataset_id': 5}


def test_flagged_then_only_suite(rules):
    suites = [{'id': 1, 'dataset_id': 7}, {'id': 2, 'dataset_id': 8, 'auto_evaluation': True}]
    assert rules.resolve_dataset_tier('agent_first', suites)['suite_id'] == 2
    only = rules.resolve_dataset_tier('agent_first', [{'id': 4, 'dataset_id': 9}, {'id': 5, 'dataset_id': None}])
    assert only == {'tier': 'agent_only_suite', 'suite_id': 4, 'dataset_id': 9}


def test_two_unflagged_suites_fall_through(rules):
    suites = [{'id': 1, 'dataset_id': 7}, {'id': 2, 'dataset_id': 8}]
    assert rules.resolve_dataset_tier('agent_first', suites, None, 11)['tier'] == 'project'
    assert rules.resolve_dataset_tier('agent_first', suites)['tier'] == 'platform'
    assert rules.resolve_dataset_tier('agent_only', suites, None, 11)['tier'] is None


# --- pre-flight -------------------------------------------------------------

def test_preflight_order(rules):
    review = {'blocking': [{'code': 'missing_credential', 'message': 'm'}, {'code': 'human_input_step', 'message': 'h'}]}
    assert rules.preflight_reason('no_access', 'project', review)[0] == 'OWNER_UNAVAILABLE'
    assert rules.preflight_reason(None, 'member', review)[0] == 'BUDGET_BLOCKED'
    assert rules.preflight_reason(None, None, review) == ('HITL_REQUIRED', 'h')


@pytest.mark.parametrize('code,reason', [
    ('personal_signin_required', 'OAUTH_REQUIRED'),
    ('delegated_credential', 'OAUTH_REQUIRED'),
    ('missing_credential', 'AGENT_BROKEN'),
])
def test_preflight_scan_codes(rules, code, reason):
    assert rules.preflight_reason(None, None, {'blocking': [{'code': code}]})[0] == reason


def test_preflight_incomplete_scan_is_other(rules):
    review = {'blocking': [], 'notices': [{'code': 'scan_incomplete', 'message': 'x'}]}
    assert rules.preflight_reason(None, None, review) == ('OTHER', 'x')
    assert rules.preflight_reason(None, None, {'blocking': [], 'notices': []}) is None


# --- post-run classifier ----------------------------------------------------

def _execs(*statuses):
    return [{'status': status} for status in statuses]


def test_classifier_threshold(rules):
    assert rules.classify_run({'status': 'finished'}, _execs('timeout', 'ok', 'ok'), 0.5) is None
    assert rules.classify_run({'status': 'finished'}, _execs('timeout', 'timeout', 'ok', 'ok'), 0.5)[0] == 'TIMEOUT'


def test_classifier_hitl_and_oauth(rules):
    paused = {'status': 'guardrail_paused', 'trajectory': {'pause': {'pause_type': 'hitl'}}}
    assert rules.classify_run({}, [paused, paused, {'status': 'ok'}], 0.5)[0] == 'HITL_REQUIRED'
    auth = {'status': 'ok', 'trajectory': {'steps': [{'kind': 'tool', 'status': 'action_required'}]}}
    assert rules.classify_run({}, [auth], 0.5)[0] == 'OAUTH_REQUIRED'


def test_classifier_broken(rules):
    assert rules.classify_run({}, _execs('predict_error', 'predict_exception'), 0.5)[0] == 'AGENT_BROKEN'
    load = [{'status': 'error', 'error': 'could not load agent version 4: boom'}]
    assert rules.classify_run({}, load, 0.5) == ('AGENT_BROKEN', 'The agent version could not be loaded')
    assert rules.classify_run({'status': 'errored', 'error': 'x'}, [], 0.5)[0] == 'AGENT_BROKEN'


def test_classifier_budget_stop(rules):
    run = {'status': 'cancelled', 'meta': {'stop_reason': 'budget_exhausted'}}
    assert rules.classify_run(run, _execs('ok'), 0.5)[0] == 'BUDGET_BLOCKED'


# --- verdict ----------------------------------------------------------------

def _snapshot(case_ids, bindings):
    return {'cases': [{'id': case_id} for case_id in case_ids], 'bindings': bindings}


def test_summarize_coverage_and_targets(rules):
    snap = _snapshot([1, 2, 3, 4], [{'dimension_id': 10, 'target': 70, 'target_operator': '>=', 'weight': 1}])
    results = [
        {'dataset_case_id': 1, 'dimension_id': 10, 'status': 'ok', 'native_score': 80, 'normalized_score': 80},
        {'dataset_case_id': 2, 'dimension_id': 10, 'status': 'ok', 'native_score': 50, 'normalized_score': 50},
        {'dataset_case_id': 3, 'dimension_id': 10, 'status': 'error', 'native_score': None, 'normalized_score': None},
    ]
    summary = rules.summarize_results(snap, results)
    assert summary['coverage'] == 50.0
    assert summary['bindings'][0]['mean_native'] == 65.0
    assert summary['bindings'][0]['met'] is False


def test_verdict(rules, settings):
    met = [{'met': True, 'weight': 1}]
    assert rules.verdict(90, 100, met, settings)[0] == 'passed'
    assert rules.verdict(40, 100, met, settings)[0] == 'failed'
    assert rules.verdict(40, 10, met, settings)[0] == 'failed'
    assert rules.verdict(90, 70, met, settings)[0] == 'warning'
    assert rules.verdict(90, 100, [{'met': False, 'weight': 1}], settings)[0] == 'warning'
    assert rules.verdict(90, 100, [{'met': False, 'weight': 0}, {'met': None}], settings)[0] == 'passed'
    assert rules.verdict(None, 0, [], settings)[0] == 'not_evaluated'


def test_like_pattern_escapes_wildcards(rules):
    assert rules.like_pattern('bot') == '%bot%'
    assert rules.like_pattern('a_b%c') == '%a\\_b\\%c%'
    assert rules.like_pattern('a\\b') == '%a\\\\b%'


# --- platform probes --------------------------------------------------------

def test_clean_probes_trims_and_dedupes_in_order(rules):
    assert rules.clean_probes([' Hi ', 'Bye', 'Hi', '  ']) == ['Hi', 'Bye']


@pytest.mark.parametrize('probes', [None, 'Hi', [1], [], ['  ']])
def test_clean_probes_rejects_a_bad_list(rules, probes):
    with pytest.raises(ValueError):
        rules.clean_probes(probes)


def test_clean_probes_caps_the_count_and_length(rules):
    with pytest.raises(ValueError, match='at most'):
        rules.clean_probes([f'probe {n}' for n in range(rules.MAX_PROBES + 1)])
    with pytest.raises(ValueError, match='characters'):
        rules.clean_probes(['x' * (rules.MAX_PROBE_LENGTH + 1)])


def test_probe_keys_keep_the_seeded_key_for_unchanged_text(rules):
    existing = {'capabilities': 'What can you help me with?', 'custom_1': 'Old'}
    keys = rules.probe_keys(['New', 'What can you help me with?', 'Another'], existing)
    assert keys[1] == 'capabilities'
    assert keys[0] != keys[2] and not {keys[0], keys[2]} & set(existing)


def test_health_check_inputs_append_starters_and_cap(rules):
    assert rules.health_check_inputs(['a', 'b'], ['b', ' ', 'c', 'd', 'e'], 3, 10) == ['a', 'b', 'c', 'd']
    assert rules.health_check_inputs(['a', 'b'], None, 3, 1) == ['a']
