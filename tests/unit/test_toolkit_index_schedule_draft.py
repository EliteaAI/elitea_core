"""Build with AI for a Toolkit Index schedule: the request models, the index roster, the prompt and the checks.

The draft models, the draft checks, the timing helpers and the schedule table are real, on SQLite. The folder
rights, the vector store count of the toolkits and the "Run without asking" setting are autospecs of the real
functions. One agent draft proves that the old path is unchanged.

Run via:
    python tests/run_tests.py unit/test_toolkit_index_schedule_draft.py -v
"""
import sys
import types
from unittest import mock

import pytest
from fixtures import schedule_sqlite as sqlite_db
from pydantic import ValidationError
from unit.test_7011_schedule_yaml_import import PACKAGE, load_plugin_module, sandbox

PROJECT = 1
OWNER_ID = 7
DB_INT_MAX = 2**31 - 1
INDEX_FLOOR_TEXT = 'Runs must be at least 1440 minutes apart'
INDEX_SCHEDULED = 'This index already has a schedule'
TOOLKIT_TYPES = ['toolkit', 'mcp']


def index_row(toolkit_id, toolkit_name, index_name, scheduled=False):
    return {'toolkit_id': toolkit_id, 'toolkit_name': toolkit_name, 'index_name': index_name, 'scheduled': scheduled}


INDEX_ROSTER = [
    index_row(7, 'Docs toolkit', 'docs'),
    index_row(7, 'Docs toolkit', 'wiki', scheduled=True),
    index_row(8, 'Code toolkit', 'src'),
]
INDEX_DRAFT = {
    'entity_type': 'toolkit_index', 'entity_name': 'Docs toolkit', 'index_name': 'docs', 'name': 'Docs refresh',
    'description': 'Nightly', 'cron': '0 3 * * *', 'timezone': 'Europe/Kyiv',
}
LOCKED_SRC = {'entity_type': 'toolkit_index', 'toolkit_id': 8, 'index_name': 'src'}


# Real signatures for the autospecs below
def entity_access_error(project_id, entity_type, entity_id, write=False, user_id=None):
    raise AssertionError('replaced by the autospec')


def folder_exclusion_clause(project_id, entity_types, id_column, user_id=None):
    raise AssertionError('replaced by the autospec')


def toolkit_ids_with_indexes(project_id, toolkit_query):
    raise AssertionError('replaced by the autospec')


def is_run_without_asking_allowed(project_id):
    raise AssertionError('replaced by the autospec')


class DraftEnv:
    """The loaded modules and the records of the stubbed neighbours."""

    def add_toolkit(self, toolkit_id, name, indexes=(), legacy_scheduled=()):
        meta = {'indexes_meta': {index: ({'schedules': {'x': {}}} if index in legacy_scheduled else {})
                                 for index in indexes}}
        with self.session_factory() as session:
            session.add(self.EliteATool(id=toolkit_id, name=name, meta=meta))
            session.commit()

    def schedule_index(self, toolkit_id, index_name):
        sqlite_db.seed_schedule(
            self.session_factory, self.EntitySchedule, 42, entity_type='toolkit_index', application_id=None,
            version_id=None, toolkit_id=toolkit_id, index_name=index_name, name='Docs refresh', prompt=None,
            timezone='Europe/Kyiv', cron='0 3 * * *')

    def validate(self, raw, locked=None, index_roster=INDEX_ROSTER, roster=()):
        return self.module.validate_schedule_draft(
            PROJECT, raw, locked, list(roster), owner_id=OWNER_ID, index_roster=index_roster)


@pytest.fixture
def env(monkeypatch):
    env = DraftEnv()
    env.hidden, env.with_indexes, env.blocked, env.config = set(), set(), set(), {}
    with sandbox(monkeypatch, env) as (Base, engine):
        env.EliteATool = sys.modules[f'{PACKAGE}.models.all'].EliteATool
        tools = sqlite_db.LenientModule('tools')
        tools.db = types.SimpleNamespace(with_project_schema_session=lambda project_id: env.session_factory())
        tools.this = types.SimpleNamespace(
            module=types.SimpleNamespace(descriptor=types.SimpleNamespace(config=env.config)))
        monkeypatch.setitem(sys.modules, 'tools', tools)

        env.access = mock.create_autospec(
            entity_access_error,
            side_effect=lambda project_id, entity_type, entity_id, write=False, user_id=None:
            ({'ok': False, 'error': 'Not found'}, 404) if entity_id in env.blocked else None)
        env.folder_clause = mock.create_autospec(
            folder_exclusion_clause,
            side_effect=lambda project_id, entity_types, id_column, user_id=None:
            id_column.notin_(env.hidden) if env.hidden else None)
        env.index_counts = mock.create_autospec(
            toolkit_ids_with_indexes,
            side_effect=lambda project_id, toolkit_query: [
                row.id for row in toolkit_query.with_entities(env.EliteATool.id).all() if row.id in env.with_indexes])
        env.run_without_asking = mock.create_autospec(is_run_without_asking_allowed, return_value=False)
        for name, attrs in (
                ('folder_access', {'entity_access_error': env.access, 'folder_exclusion_clause': env.folder_clause,
                                   'TOOLKIT_ENTITY_TYPES': TOOLKIT_TYPES, 'APPLICATION_ENTITY_TYPES': ['agent']}),
                ('application_tools', {'_toolkit_ids_with_indexes': env.index_counts}),
                ('schedule_settings', {'is_run_without_asking_allowed': env.run_without_asking})):
            module = types.ModuleType(f'{PACKAGE}.utils.{name}')
            module.__dict__.update(attrs)
            monkeypatch.setitem(sys.modules, module.__name__, module)

        env.EntitySchedule = sqlite_db.create_schedule_table(load_plugin_module, Base, engine)
        load_plugin_module('models/pd/index.py', 'models.pd.index')
        load_plugin_module('utils/schedule_timing.py', 'utils.schedule_timing')
        load_plugin_module('models/pd/entity_schedule.py', 'models.pd.entity_schedule')
        env.models = load_plugin_module('models/pd/generate_schedule_draft.py', 'models.pd.generate_schedule_draft')
        env.module = load_plugin_module('utils/schedule_draft_utils.py', 'utils.schedule_draft_utils')
        yield env


def fields(warnings):
    return [warning['field'] for warning in warnings]


# --- request and response models

def test_locked_index_takes_a_toolkit_id_and_an_index_name(env):
    request = env.models.GenerateScheduleDraftRequest(prompt='x', locked_entity=LOCKED_SRC)
    assert request.locked_entity.model_dump() == LOCKED_SRC


def test_locked_agent_dump_has_no_index_keys(env):
    request = env.models.GenerateScheduleDraftRequest(
        prompt='x', locked_entity={'entity_type': 'agent', 'application_id': 4})
    assert request.locked_entity.model_dump() == {'entity_type': 'agent', 'application_id': 4}


@pytest.mark.parametrize('locked', [
    {'entity_type': 'toolkit_index', 'application_id': 4},
    {'entity_type': 'toolkit_index', 'toolkit_id': 8},
    {'entity_type': 'toolkit_index', 'index_name': 'src'},
    {'entity_type': 'toolkit_index', 'toolkit_id': 0, 'index_name': 'src'},
    {'entity_type': 'toolkit_index', 'toolkit_id': DB_INT_MAX + 1, 'index_name': 'src'},
    {'entity_type': 'toolkit_index', 'toolkit_id': 8, 'index_name': ''},
    {'entity_type': 'toolkit_index', 'toolkit_id': 8, 'index_name': 'x' * 257},
    {'entity_type': 'agent', 'toolkit_id': 8, 'index_name': 'src'},
])
def test_locked_index_refuses_a_missing_or_out_of_range_field(env, locked):
    with pytest.raises(ValidationError) as caught:
        env.models.GenerateScheduleDraftRequest(prompt='x', locked_entity=locked)
    assert caught.value.errors()[0]['loc'][0] == 'locked_entity'


def test_current_form_takes_the_index_target_and_an_empty_toolkit_is_unset(env):
    form = env.models.CurrentForm(entity_type='toolkit_index', toolkit_id='', index_name='docs')
    assert form.model_dump(exclude_unset=True) == {'entity_type': 'toolkit_index', 'toolkit_id': None,
                                                   'index_name': 'docs'}
    with pytest.raises(ValidationError):
        env.models.CurrentForm(toolkit_id='7')


def test_draft_response_carries_the_index_target_and_it_is_null_for_an_agent(env):
    draft = env.models.GenerateScheduleDraftResponse(
        draft={'entity_type': 'toolkit_index', 'toolkit_id': 7, 'index_name': 'docs'}).model_dump(mode='json')['draft']
    assert (draft['toolkit_id'], draft['index_name'], draft['application_id']) == (7, 'docs', None)
    agent = env.models.GenerateScheduleDraftResponse(draft={'application_id': 1}).model_dump(mode='json')['draft']
    assert (agent['application_id'], agent['toolkit_id'], agent['index_name']) == (1, None, None)


# --- the index roster

def test_roster_lists_every_index_of_a_toolkit_with_indexes_in_name_order_and_marks_the_scheduled(env):
    env.add_toolkit(7, 'Docs toolkit', ['docs', 'wiki'])
    env.add_toolkit(8, 'Code toolkit', ['src', 'old'], legacy_scheduled=['old'])
    env.add_toolkit(9, 'Empty toolkit')
    env.with_indexes = {7, 8}
    env.schedule_index(7, 'wiki')
    assert env.module.schedule_index_roster(PROJECT, OWNER_ID) == [
        index_row(8, 'Code toolkit', 'old', scheduled=True), index_row(8, 'Code toolkit', 'src'),
        index_row(7, 'Docs toolkit', 'docs'), index_row(7, 'Docs toolkit', 'wiki', scheduled=True),
    ]


def test_roster_never_lists_a_toolkit_in_a_folder_the_user_cannot_open(env):
    env.add_toolkit(7, 'Docs toolkit', ['docs'])
    env.add_toolkit(8, 'Hidden toolkit', ['secret'])
    env.with_indexes, env.hidden = {7, 8}, {8}
    roster = env.module.schedule_index_roster(PROJECT, OWNER_ID)
    assert [row['toolkit_id'] for row in roster] == [7]
    env.folder_clause.assert_called_once_with(PROJECT, TOOLKIT_TYPES, env.EliteATool.id, OWNER_ID)
    env.index_counts.assert_called_once()


def test_roster_is_empty_when_no_toolkit_has_an_index(env):
    env.add_toolkit(7, 'Docs toolkit', ['docs'])
    assert env.module.schedule_index_roster(PROJECT, OWNER_ID) == []


def test_roster_is_read_only_when_the_slot_or_a_locked_index_asks_for_it(env, monkeypatch):
    roster = mock.create_autospec(env.module.schedule_index_roster, return_value=INDEX_ROSTER)
    monkeypatch.setattr(env.module, 'schedule_index_roster', roster)
    locked_agent = {'entity_type': 'agent', 'application_id': 1}
    assert env.module.index_roster_for(PROJECT, OWNER_ID, 'Draft. {agents}', locked_agent) is None
    assert env.module.index_roster_for(PROJECT, OWNER_ID, 'Draft. {agents}', None) is None
    roster.assert_not_called()
    assert env.module.index_roster_for(PROJECT, OWNER_ID, 'Indexes: {toolkit_indexes}', None) == INDEX_ROSTER
    assert env.module.index_roster_for(PROJECT, OWNER_ID, 'Draft.', LOCKED_SRC) == INDEX_ROSTER
    assert roster.call_args_list == [mock.call(PROJECT, OWNER_ID)] * 2


# --- the prompt

AGENT_ROSTER = [{'application_id': 42, 'name': 'Digest agent', 'agent_type': 'react', 'version_id': 420,
                 'version_name': 'base'}]


def test_prompt_lists_the_indexes_without_a_schedule_beside_the_agents(env):
    prompt = env.module.build_schedule_draft_prompt(
        'Agents:\n{agents}\nIndexes:\n{toolkit_indexes}', {}, AGENT_ROSTER, None, None, INDEX_ROSTER)
    assert prompt == ('Agents:\n- Digest agent (versions: base)\n'
                      'Indexes:\n- Docs toolkit (indexes: docs)\n- Code toolkit (indexes: src)')


def test_prompt_says_none_when_there_is_no_index_to_schedule(env):
    scheduled = [index_row(7, 'Docs toolkit', 'wiki', scheduled=True)]
    assert env.module.build_schedule_draft_prompt('{toolkit_indexes}', {}, [], None, None, scheduled) == '(none)'
    assert env.module.build_schedule_draft_prompt('{toolkit_indexes}', {}, [], None) == '(none)'


def test_prompt_without_the_slot_stays_as_the_admin_wrote_it(env):
    template = 'Draft a schedule. Agents: {agents}. Keep {unknown}.'
    prompt = env.module.build_schedule_draft_prompt(template, {}, AGENT_ROSTER, None, None, INDEX_ROSTER)
    assert prompt == 'Draft a schedule. Agents: - Digest agent (versions: base). Keep {unknown}.'


def test_prompt_holds_at_most_three_hundred_pairs(env):
    roster = ([index_row(1, 'First', f'a{number}') for number in range(200)]
              + [index_row(2, 'Second', f'b{number}') for number in range(150)])
    prompt = env.module.build_schedule_draft_prompt('{toolkit_indexes}', {}, [], None, None, roster)
    first, second = prompt.splitlines()
    assert len(first.split(', ')) == 200 and len(second.split(', ')) == 100
    assert 'b99' in second and 'b100' not in second


@pytest.mark.parametrize('locked, index_roster, expected', [
    (LOCKED_SRC, INDEX_ROSTER, 'toolkit_index Code toolkit/src'),
    (LOCKED_SRC, [], 'toolkit_index 8/src'),
    ({'entity_type': 'agent', 'application_id': 42}, INDEX_ROSTER, 'agent Digest agent'),
])
def test_prompt_names_the_locked_entity(env, locked, index_roster, expected):
    prompt = env.module.build_schedule_draft_prompt('{locked_entity}', {}, AGENT_ROSTER, None, locked, index_roster)
    assert prompt == expected


# --- the checks

def test_index_names_map_to_ids_and_the_model_ids_are_ignored(env):
    draft, warnings = env.validate({**INDEX_DRAFT, 'entity_name': 'docs TOOLKIT', 'toolkit_id': 8, 'application_id': 1,
                                    'version_id': 10})
    assert (draft.entity_type, draft.toolkit_id, draft.index_name) == ('toolkit_index', 7, 'docs')
    assert (draft.application_id, draft.version_id) == (None, None)
    assert (draft.name, draft.description, draft.cron, draft.timezone) == (
        'Docs refresh', 'Nightly', '0 3 * * *', 'Europe/Kyiv')
    assert warnings == []
    env.access.assert_called_once_with(PROJECT, TOOLKIT_TYPES, 7, user_id=OWNER_ID)


@pytest.mark.parametrize('overrides, field, reason', [
    ({'entity_name': 'Ghost toolkit'}, 'toolkit_id', "No index 'docs' in a toolkit named 'Ghost toolkit' in this project"),
    ({'index_name': 'ghost'}, 'toolkit_id', "No index 'ghost' in a toolkit named 'Docs toolkit' in this project"),
    ({'index_name': 'DOCS'}, 'toolkit_id', "No index 'DOCS' in a toolkit named 'Docs toolkit' in this project"),
    ({'index_name': 'wiki'}, 'index_name', INDEX_SCHEDULED),
    ({'entity_name': None}, 'toolkit_id', 'A toolkit index needs a toolkit name and an index name'),
    ({'index_name': None}, 'toolkit_id', 'A toolkit index needs a toolkit name and an index name'),
])
def test_a_missing_or_scheduled_index_clears_the_target_and_keeps_the_other_fields(env, overrides, field, reason):
    draft, warnings = env.validate({**INDEX_DRAFT, **overrides})
    assert (draft.toolkit_id, draft.index_name) == (None, None)
    assert draft.entity_type == 'toolkit_index'
    assert (draft.name, draft.cron) == ('Docs refresh', '0 3 * * *')
    assert warnings == [{'field': field, 'reason': reason}]
    env.access.assert_not_called()


def test_a_toolkit_name_found_twice_clears_the_target(env):
    twice = INDEX_ROSTER + [index_row(11, 'docs toolkit', 'docs')]
    draft, warnings = env.validate(INDEX_DRAFT, index_roster=twice)
    assert (draft.toolkit_id, draft.index_name) == (None, None)
    assert warnings == [{'field': 'toolkit_id',
                         'reason': "More than one toolkit is named 'Docs toolkit' and has this index"}]


def test_a_toolkit_the_user_cannot_open_clears_the_target(env):
    env.blocked = {7}
    draft, warnings = env.validate(INDEX_DRAFT)
    assert (draft.toolkit_id, draft.index_name) == (None, None)
    assert warnings == [{'field': 'toolkit_id', 'reason': 'No toolkit with this id in this project'}]


def test_a_locked_index_wins_over_the_drafted_one(env):
    draft, warnings = env.validate(INDEX_DRAFT, locked=LOCKED_SRC)
    assert (draft.entity_type, draft.toolkit_id, draft.index_name) == ('toolkit_index', 8, 'src')
    assert warnings == []
    env.access.assert_called_once_with(PROJECT, TOOLKIT_TYPES, 8, user_id=OWNER_ID)


def test_a_locked_index_wins_over_a_drafted_agent(env):
    draft, _warnings = env.validate({'entity_type': 'agent', 'entity_name': 'Digest agent'}, locked=LOCKED_SRC,
                                    roster=AGENT_ROSTER)
    assert (draft.entity_type, draft.toolkit_id, draft.index_name, draft.application_id) == (
        'toolkit_index', 8, 'src', None)


def test_a_locked_index_the_user_cannot_open_is_dropped(env):
    env.blocked = {8}
    draft, warnings = env.validate({}, locked=LOCKED_SRC)
    assert (draft.entity_type, draft.toolkit_id, draft.index_name) == ('toolkit_index', None, None)
    assert fields(warnings) == ['toolkit_id']


def test_a_cron_under_the_index_floor_is_cleared_and_the_time_zone_stays(env):
    draft, warnings = env.validate({**INDEX_DRAFT, 'cron': '0 * * * *'})
    assert (draft.cron, draft.timezone) == (None, 'Europe/Kyiv')
    assert warnings == [{'field': 'cron', 'reason': INDEX_FLOOR_TEXT}]
    assert (draft.toolkit_id, draft.index_name) == (7, 'docs')


def test_a_cron_follows_the_floor_the_admin_set_for_indexes(env):
    env.config['scheduler'] = {'schedules': {'toolkit_index_min_interval_minutes': 60}}
    draft, warnings = env.validate({**INDEX_DRAFT, 'cron': '0 * * * *'})
    assert (draft.cron, warnings) == ('0 * * * *', [])


def test_the_run_fields_are_cleared_with_a_warning_each_and_nothing_is_scanned(env):
    draft, warnings = env.validate({
        **INDEX_DRAFT, 'run_mode': 'prompt', 'prompt': 'Summarize', 'variables': {'topic': 'sales'},
        'approval_rule': 'run_without_asking'})
    assert (draft.run_mode, draft.prompt, draft.variables, draft.approval_rule) == (None, None, [], None)
    assert sorted(fields(warnings)) == ['approval_rule', 'prompt', 'run_mode', 'variables']
    env.run_without_asking.assert_not_called()
    assert (draft.toolkit_id, draft.index_name) == (7, 'docs')


def test_an_empty_run_field_gives_no_warning(env):
    _draft, warnings = env.validate({**INDEX_DRAFT, 'variables': [], 'prompt': ''})
    assert warnings == []


def test_an_unknown_type_names_the_three_kinds(env):
    draft, warnings = env.validate({'entity_type': 'toolkit', 'entity_name': 'Docs toolkit'})
    assert draft.entity_type is None
    assert warnings == [{'field': 'entity_type', 'reason': 'The type must be agent, pipeline or toolkit_index'}]


def test_the_index_roster_is_read_for_an_index_draft_that_came_without_one(env, monkeypatch):
    roster = mock.create_autospec(env.module.schedule_index_roster, return_value=INDEX_ROSTER)
    monkeypatch.setattr(env.module, 'schedule_index_roster', roster)
    draft, warnings = env.validate(INDEX_DRAFT, index_roster=None)
    assert (draft.toolkit_id, draft.index_name, warnings) == (7, 'docs', [])
    roster.assert_called_once_with(PROJECT, OWNER_ID)


def test_an_agent_draft_keeps_its_old_path_and_never_reads_the_index_roster(env, monkeypatch):
    roster = mock.create_autospec(env.module.schedule_index_roster, return_value=INDEX_ROSTER)
    monkeypatch.setattr(env.module, 'schedule_index_roster', roster)
    with env.session_factory() as session:
        session.add_all([env.Application(id=1, name='Digest agent')])
        session.flush()
        session.add(env.ApplicationVersion(id=10, application_id=1, name='base', agent_type='openai'))
        session.commit()
    draft, warnings = env.module.validate_schedule_draft(PROJECT, {
        'entity_type': 'agent', 'entity_name': 'digest AGENT', 'version_name': 'base', 'name': 'Digest',
        'cron': '*/10 * * * *', 'timezone': 'Europe/Kyiv', 'prompt': 'Summarize',
    }, None, owner_id=OWNER_ID)
    assert (draft.entity_type, draft.application_id, draft.version_id) == ('agent', 1, 10)
    assert (draft.toolkit_id, draft.index_name) == (None, None)
    assert (draft.cron, draft.prompt, warnings) == ('*/10 * * * *', 'Summarize', [])  # the index floor is not applied
    env.access.assert_called_once_with(PROJECT, 'agent', 1, user_id=OWNER_ID)
    roster.assert_not_called()
