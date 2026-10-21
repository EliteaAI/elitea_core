"""EL-7011 US-21 R2.1-R2.4, R3.1, R3.2 - validate_schedule_draft clears bad fields and warns."""
import sys
import types
from datetime import date, timedelta

import pytest
from unit._tool_spend_loader import load

ROSTER = [
    {'application_id': 42, 'name': 'Digest agent', 'agent_type': 'react', 'version_id': 420, 'version_name': 'base'},
    {'application_id': 51, 'name': 'Report flow', 'agent_type': 'pipeline', 'version_id': 510, 'version_name': 'base'},
    {'application_id': 51, 'name': 'Report flow', 'agent_type': 'pipeline', 'version_id': 511, 'version_name': 'v2'},
]
AGENT = {'entity_type': 'agent', 'entity_name': 'Digest agent', 'version_name': 'base'}
PIPELINE = {'entity_type': 'pipeline', 'entity_name': 'Report flow', 'version_name': 'base'}


@pytest.fixture
def setting():
    """The project setting "Allow Run without asking" and the project ids it was read for."""
    return types.SimpleNamespace(allowed=False, reads=[])


def load_draft_utils(monkeypatch, setting=None):
    """schedule_draft_utils on its real models, with the roster above and the given project setting."""
    setting = setting or types.SimpleNamespace(allowed=False, reads=[])
    stub = types.ModuleType('tools')
    stub.this = types.SimpleNamespace(module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config={})))
    monkeypatch.setitem(sys.modules, 'tools', stub)
    settings = types.ModuleType('plugins.elitea_core.utils.schedule_settings')
    settings.is_run_without_asking_allowed = lambda project_id: setting.reads.append(project_id) or setting.allowed
    monkeypatch.setitem(sys.modules, settings.__name__, settings)
    for dotted in ('utils.schedule_timing', 'models.pd.entity_schedule', 'models.pd.generate_schedule_draft',
                   'utils.schedule_draft_utils'):
        monkeypatch.setitem(sys.modules, 'plugins.elitea_core.' + dotted, None)
    load('models/pd/index.py')
    load('utils/schedule_timing.py', fresh=True)
    load('models/pd/entity_schedule.py', fresh=True)
    load('models/pd/generate_schedule_draft.py', fresh=True)
    module = load('utils/schedule_draft_utils.py', fresh=True)
    monkeypatch.setattr(module, 'schedule_roster', lambda project_id: ROSTER)
    return module


@pytest.fixture
def validate(setting, monkeypatch):
    module = load_draft_utils(monkeypatch, setting)
    return lambda raw, locked=None: module.validate_schedule_draft(1, raw, locked)


def _fields(warnings):
    return [warning['field'] for warning in warnings]


def test_el_7011_r2_1_keeps_a_valid_cron_and_time_zone(validate):
    draft, warnings = validate({'cron': '0 9 * * 1-5', 'timezone': 'Europe/Kyiv'})
    assert (draft.cron, draft.timezone, warnings) == ('0 9 * * 1-5', 'Europe/Kyiv', [])


@pytest.mark.parametrize('raw, field', [
    ({'cron': '* * * * *', 'timezone': 'UTC'}, 'cron'),  # under the 5 minute floor
    ({'cron': 'not a cron', 'timezone': 'UTC'}, 'cron'),
    ({'cron': '0 9 * * *', 'timezone': 'Mars/Base'}, 'timezone'),
])
def test_el_7011_r2_1_clears_only_the_bad_schedule_field(validate, raw, field):
    draft, warnings = validate(raw)
    assert getattr(draft, field) is None
    other = 'timezone' if field == 'cron' else 'cron'
    assert getattr(draft, other) == raw[other]
    assert _fields(warnings) == [field]
    assert warnings[0]['reason']


def test_el_7011_r2_2_resolves_entity_and_version_by_name(validate):
    draft, warnings = validate(AGENT)
    assert (draft.entity_type, draft.application_id, draft.version_id, warnings) == ('agent', 42, 420, [])


@pytest.mark.parametrize('raw, field, kept', [
    ({'entity_type': 'agent', 'entity_name': 'Ghost agent', 'version_name': 'base'}, 'application_id', None),
    ({'entity_type': 'agent', 'entity_name': 'Digest agent', 'version_name': 'latest'}, 'version_id', 42),
    # a pipeline name never resolves as an agent
    ({'entity_type': 'agent', 'entity_name': 'Report flow', 'version_name': 'base'}, 'application_id', None),
])
def test_el_7011_r2_2_clears_an_unresolved_entity_or_version(validate, raw, field, kept):
    draft, warnings = validate(raw)
    assert getattr(draft, field) is None
    assert draft.application_id == kept
    assert _fields(warnings) == [field]


def test_el_7011_r2_3_locked_entity_wins_over_the_drafted_one(validate):
    raw = {'entity_type': 'pipeline', 'application_id': 51, 'run_mode': 'entry_point'}
    draft, warnings = validate(raw, {'entity_type': 'agent', 'application_id': 42})
    assert (draft.entity_type, draft.application_id, draft.run_mode) == ('agent', 42, None)
    assert _fields(warnings) == ['run_mode']


def test_el_7011_r2_3_locked_pipeline_keeps_type_id_and_drafted_prompt(validate):
    raw = {'entity_type': 'agent', 'application_id': 7, 'version_name': 'v2', 'run_mode': 'prompt',
           'prompt': 'Build the {{region}} report'}
    draft, warnings = validate(raw, {'entity_type': 'pipeline', 'application_id': 51})
    assert (draft.entity_type, draft.application_id, draft.version_id) == ('pipeline', 51, 511)
    assert (draft.run_mode, draft.prompt, warnings) == ('prompt', 'Build the {{region}} report', [])


@pytest.mark.parametrize('allowed, rule, warned', [
    (False, 'run_without_asking', True),
    (True, 'run_without_asking', False),
    (False, 'pause_and_ask', False),
    (True, 'pause_and_ask', False),
])
def test_el_7011_r2_4_run_without_asking_needs_the_project_setting(validate, setting, allowed, rule, warned):
    setting.allowed = allowed
    draft, warnings = validate({'approval_rule': rule})
    assert draft.approval_rule == ('pause_and_ask' if warned else rule)
    assert _fields(warnings) == (['approval_rule'] if warned else [])
    assert setting.reads == ([1] if rule == 'run_without_asking' else [])


def test_el_7011_r3_1_prompt_mode_keeps_prompt_and_variables(validate):
    raw = {**PIPELINE, 'run_mode': 'prompt', 'prompt': 'Build the {{region}} report',
           'variables': [{'name': 'region', 'value': 'EU'}]}
    draft, warnings = validate(raw)
    assert draft.run_mode == 'prompt'
    assert (draft.prompt, draft.variables, warnings) == (raw['prompt'], raw['variables'], [])


def test_el_7011_r3_1_entry_point_drops_prompt_and_variables(validate):
    raw = {**PIPELINE, 'run_mode': 'entry_point', 'prompt': 'Build the report',
           'variables': [{'name': 'region', 'value': 'EU'}]}
    draft, warnings = validate(raw)
    assert (draft.run_mode, draft.prompt, draft.variables) == ('entry_point', None, [])
    assert _fields(warnings) == ['prompt']
    assert 'Entry point' in warnings[0]['reason']


def test_el_7011_r3_2_unknown_pipeline_run_mode_is_cleared(validate):
    draft, warnings = validate({**PIPELINE, 'run_mode': 'chat'})
    assert draft.run_mode is None
    assert _fields(warnings) == ['run_mode']


def test_el_7011_r3_2_agent_keeps_prompt_and_gets_no_run_mode(validate):
    draft, warnings = validate({**AGENT, 'prompt': 'Summarise', 'run_mode': 'prompt'})
    assert (draft.run_mode, draft.prompt) == (None, 'Summarise')
    assert _fields(warnings) == ['run_mode']


def test_el_7011_r2_1_a_field_of_the_wrong_type_is_cleared(validate):
    draft, warnings = validate({'name': 'Digest', 'expires_at': 'someday', 'variables': 'no'})
    assert (draft.name, draft.expires_at, draft.variables) == ('Digest', None, [])
    assert sorted(_fields(warnings)) == ['expires_at', 'variables']


def test_el_7011_r3_1_variables_as_the_service_prompt_answers_them_become_name_value_items(validate):
    """The generate_schedule_draft prompt (US-20) asks for variables as {name: value}."""
    raw = {**PIPELINE, 'run_mode': 'prompt', 'prompt': 'Report for {{region}}', 'variables': {'region': 'EU'}}
    draft, warnings = validate(raw)
    assert (draft.variables, warnings) == ([{'name': 'region', 'value': 'EU'}], [])


@pytest.mark.parametrize('rule, kept', [('Pause and ask', 'pause_and_ask'), (' PAUSE_AND_ASK ', 'pause_and_ask')])
def test_el_7011_r2_4_an_approval_rule_in_other_case_or_spaces_is_normalised(validate, rule, kept):
    draft, warnings = validate({'approval_rule': rule})
    assert (draft.approval_rule, warnings) == (kept, [])


def test_el_7011_r2_4_an_unknown_approval_rule_becomes_pause_and_ask(validate):
    draft, warnings = validate({'approval_rule': 'always'})
    assert draft.approval_rule == 'pause_and_ask'
    assert _fields(warnings) == ['approval_rule']


def test_el_7011_r2_4_a_normalised_run_without_asking_still_needs_the_setting(validate):
    draft, warnings = validate({'approval_rule': 'Run without asking'})
    assert draft.approval_rule == 'pause_and_ask'
    assert _fields(warnings) == ['approval_rule']


@pytest.mark.parametrize('raw', [
    {'cron': '0 9 * * *', 'timezone': 'UTC', 'expires_at': '2020-01-01'},  # in the past
    {'cron': '0 9 1 1 *', 'timezone': 'UTC', 'expires_at': (date.today() + timedelta(days=1)).isoformat()},
])
def test_el_7011_r2_1_a_stop_date_that_fails_the_save_rules_is_cleared(validate, raw):
    if raw['cron'] == '0 9 1 1 *' and date.today() + timedelta(days=1) >= date(date.today().year + 1, 1, 1):
        pytest.skip('the next run of 1 January falls inside the window')
    draft, warnings = validate(raw)
    assert (draft.cron, draft.expires_at) == (raw['cron'], None)
    assert _fields(warnings) == ['expires_at']
    assert warnings[0]['reason']


def test_el_7011_r2_1_a_future_stop_date_is_kept(validate):
    stop = date.today() + timedelta(days=30)
    draft, warnings = validate({'cron': '0 9 * * *', 'timezone': 'UTC', 'expires_at': stop.isoformat()})
    assert (draft.expires_at, warnings) == (stop, [])


def test_el_7011_p3_a_given_roster_is_used_instead_of_a_new_read(monkeypatch):
    module = load_draft_utils(monkeypatch)
    monkeypatch.setattr(module, 'schedule_roster', lambda project_id: pytest.fail('roster read twice'))
    draft, _ = module.validate_schedule_draft(1, AGENT, None, ROSTER)
    assert draft.application_id == 42


def test_el_7011_r2_1_a_stop_date_time_keeps_its_date(validate):
    stop = date.today() + timedelta(days=30)
    draft, warnings = validate({'cron': '0 9 * * *', 'timezone': 'UTC', 'expires_at': f'{stop.isoformat()}T23:59:00Z'})
    assert (draft.expires_at, warnings) == (stop, [])


def test_el_7011_r2_2_a_version_without_an_entity_name_is_warned(validate):
    draft, warnings = validate({'entity_type': 'agent', 'version_name': 'base'})
    assert (draft.application_id, draft.version_id) == (None, None)
    assert _fields(warnings) == ['version_id']
