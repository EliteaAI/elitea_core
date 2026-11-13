"""Build with AI offers skills up to the agent skill limit, and an edit draft always offers what is attached (#7388).

Run via:
    python tests/run_tests.py unit/utils/test_7388_draft_skill_limit.py -v
"""
import enum
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PKG = 'draftpkg_7388_test'
PROJECT = 2
PUBLIC_PROJECT = 1


class _Column:
    """Stands in for an ORM column: comparisons and SQL helpers just return another column."""

    def __eq__(self, other):
        return _Column()

    def __ne__(self, other):
        return _Column()

    def __and__(self, other):
        return _Column()

    __hash__ = object.__hash__

    def isnot(self, other):
        return _Column()

    def any(self, *a):
        return _Column()

    def label(self, name):
        return self


def _model(name, *columns):
    return type(name, (), {column: _Column() for column in columns})


class _Query:
    def __init__(self, rows, order_log=None):
        self.rows = rows
        self.order_log = order_log if order_log is not None else []

    def filter(self, *a):
        return self

    def join(self, *a):
        return self

    def order_by(self, *a):
        self.order_log.append(a)
        return self

    def limit(self, n):
        return _Query(self.rows[:n], self.order_log)

    def all(self):
        return self.rows


class _Session:
    def __init__(self, skill_rows):
        self.skill_rows = skill_rows
        self.skill_order = []

    def __call__(self, project_id):
        return self

    def query(self, first, *rest):
        if first is sys.modules[f'{PKG}.models.skill'].Skill.id:
            return _Query(self.skill_rows, self.skill_order)
        return _Query([])


def _load(session, public_project_id=PROJECT):
    pkg = types.ModuleType(PKG)
    pkg.__path__ = []
    models_pkg = types.ModuleType(f'{PKG}.models')
    models_pkg.__path__ = []
    utils_pkg = types.ModuleType(f'{PKG}.utils')
    utils_pkg.__path__ = [str(PLUGIN_ROOT / 'utils')]

    all_models = types.ModuleType(f'{PKG}.models.all')
    all_models.Application = _model('Application', 'id', 'name', 'description', 'shared_owner_id', 'shared_id')
    all_models.ApplicationVersion = _model(
        'ApplicationVersion', 'id', 'application_id', 'name', 'agent_type', 'status')
    elitea_tools = types.ModuleType(f'{PKG}.models.elitea_tools')
    elitea_tools.EliteATool = _model('EliteATool', 'id', 'type', 'name', 'description')
    skill = types.ModuleType(f'{PKG}.models.skill')
    skill.Skill = _model('Skill', 'id', 'name', 'description', 'shared_owner_id', 'shared_id', 'versions')
    skill.SkillVersion = _model('SkillVersion', 'status')
    enums_pkg = types.ModuleType(f'{PKG}.models.enums')
    enums_pkg.__path__ = []
    enums_all = types.ModuleType(f'{PKG}.models.enums.all')
    enums_all.PublishStatus = enum.Enum('PublishStatus', {'published': 'published'})
    utils_utils = types.ModuleType(f'{PKG}.utils.utils')
    utils_utils.get_public_project_id = lambda: public_project_id

    @contextmanager
    def _session(project_id):
        yield session(project_id)

    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(
        warning=lambda *a, **k: None, exception=lambda *a, **k: None,
        debug=lambda *a, **k: None, info=lambda *a, **k: None,
    )
    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(with_project_schema_session=_session)

    saved = {name: sys.modules.get(name) for name in ('pylon', 'pylon.core', 'pylon.core.tools', 'tools')}
    sys.modules['pylon'] = types.ModuleType('pylon')
    sys.modules['pylon.core'] = types.ModuleType('pylon.core')
    sys.modules['pylon.core.tools'] = pylon_tools
    sys.modules['tools'] = tools
    for name, mod in {
        PKG: pkg,
        f'{PKG}.models': models_pkg,
        f'{PKG}.models.all': all_models,
        f'{PKG}.models.elitea_tools': elitea_tools,
        f'{PKG}.models.skill': skill,
        f'{PKG}.models.enums': enums_pkg,
        f'{PKG}.models.enums.all': enums_all,
        f'{PKG}.utils': utils_pkg,
        f'{PKG}.utils.utils': utils_utils,
    }.items():
        sys.modules[name] = mod

    full = f'{PKG}.utils.generate_application_utils'
    spec = importlib.util.spec_from_file_location(full, PLUGIN_ROOT / 'utils' / 'generate_application_utils.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[full] = module
    try:
        spec.loader.exec_module(module)
    finally:
        for name, mod in saved.items():
            if mod is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = mod
    return module


SKILL_ROWS = [types.SimpleNamespace(id=i, name=f'skill-{i}', description='') for i in range(1, 201)]


@pytest.fixture
def load():
    leaked_sqlalchemy_stubs = {
        name: mod for name, mod in sys.modules.items()
        if (name == 'sqlalchemy' or name.startswith('sqlalchemy.')) and getattr(mod, '__file__', None) is None
    }
    for name in leaked_sqlalchemy_stubs:
        del sys.modules[name]
    yield _load
    for name in list(sys.modules):
        if name.startswith(PKG):
            del sys.modules[name]
    sys.modules.update(leaked_sqlalchemy_stubs)


@pytest.fixture
def utils(load):
    return load(_Session([]))


class TestSkillPool:
    @pytest.mark.parametrize('limit', [6, 37, 50, 120, 200])
    def test_pool_reaches_the_skill_limit(self, load, limit):
        module = load(_Session(SKILL_ROWS))
        *_, skills = module.fetch_project_resources(PROJECT, 'an agent with many skills', limit)
        assert [s['id'] for s in skills] == list(range(1, limit + 1))

    def test_catalog_skills_fill_the_pool_up_to_the_limit(self, load):
        own = SKILL_ROWS[:3]
        catalog = [types.SimpleNamespace(id=1000 + i, name=f'catalog-{i}', description='') for i in range(200)]
        sessions = {PROJECT: _Session(own), PUBLIC_PROJECT: _Session(catalog)}
        module = load(lambda project_id: sessions[project_id], public_project_id=PUBLIC_PROJECT)
        *_, skills = module.fetch_project_resources(PROJECT, 'an agent with many skills', 80)
        assert [s['id'] for s in skills] == [1, 2, 3] + list(range(1000, 1077))
        assert {s.get('project_id') for s in skills[3:]} == {PUBLIC_PROJECT}

    def test_skills_named_in_the_request_lead_the_pool(self, load):
        generic = [types.SimpleNamespace(id=i, name=f'generic-{i}', description='format all case edge for')
                   for i in range(1, 41)]
        named = [types.SimpleNamespace(id=100, name='all-caps', description=''),
                 types.SimpleNamespace(id=101, name='haiku-format', description='')]
        session = _Session(generic + named)
        module = load(session)
        *_, skills = module.fetch_project_resources(
            PROJECT, 'Attach these skills: all-caps, haiku-format. Format all edge case for', 6)
        assert {s['id'] for s in skills[:2]} == {100, 101}
        assert len(skills) == 6

    def test_sql_orders_by_the_name_match_before_token_relevance(self, load):
        session = _Session(SKILL_ROWS)
        module = load(session)
        module.fetch_project_resources(PROJECT, 'Attach all-caps', 6)
        first_order = str(session.skill_order[0][0])
        assert 'strpos' in first_order.lower()

    def test_named_catalog_skill_is_not_crowded_out_by_own_skills(self, load):
        own = [types.SimpleNamespace(id=i, name=f'own-{i}', description='') for i in range(1, 11)]
        catalog = [types.SimpleNamespace(id=500, name='weather-reporter', description='')]
        sessions = {PROJECT: _Session(own), PUBLIC_PROJECT: _Session(catalog)}
        module = load(lambda project_id: sessions[project_id], public_project_id=PUBLIC_PROJECT)
        *_, skills = module.fetch_project_resources(PROJECT, 'use weather-reporter', 5)
        assert skills[0]['id'] == 500 and skills[0]['project_id'] == PUBLIC_PROJECT
        assert len(skills) == 5

    def test_other_kinds_stay_capped_at_five(self, load):
        module = load(_Session(SKILL_ROWS))
        assert (module._MAX_TOOLKITS, module._MAX_MCP, module._MAX_AGENTS, module._MAX_PIPELINES) == (5, 5, 5, 5)


ATTACHED_SKILLS = [{'id': 40, 'name': 'pinned-a', 'description': None}, {'id': 41, 'name': 'pinned-b', 'description': None}]
ATTACHED_AGENT = {'application_id': 9, 'name': 'sub', 'description': None, 'type': 'agent'}
ATTACHED_PIPELINE = {'application_id': 12, 'name': 'flow', 'description': None, 'type': 'pipeline'}
CURRENT_CONFIG = {
    'attached_skills': ATTACHED_SKILLS,
    'attached_agents': [ATTACHED_AGENT],
    'attached_pipelines': [ATTACHED_PIPELINE],
}


class TestOfferAttached:
    def test_attached_items_lead_every_list(self, utils):
        ranked_skills = [{'id': i, 'name': f's{i}'} for i in range(1, 6)]
        ranked_agents = [{'application_id': 3, 'name': 'a3', 'type': 'agent'}]
        agents, pipelines, skills = utils.offer_attached_resources(
            CURRENT_CONFIG, ranked_agents, [], ranked_skills, 50)
        assert [a['application_id'] for a in agents] == [9, 3]
        assert [p['application_id'] for p in pipelines] == [12]
        assert [s['id'] for s in skills] == [40, 41, 1, 2, 3, 4, 5]

    def test_attached_item_also_ranked_is_offered_once(self, utils):
        _, _, skills = utils.offer_attached_resources(
            CURRENT_CONFIG, [], [], [{'id': 41, 'name': 'pinned-b'}, {'id': 1, 'name': 's1'}], 50)
        assert [s['id'] for s in skills] == [40, 41, 1]

    def test_catalog_item_sharing_an_attached_id_is_kept(self, utils):
        catalog = {'id': 40, 'name': 'catalog', 'project_id': PUBLIC_PROJECT}
        _, _, skills = utils.offer_attached_resources(CURRENT_CONFIG, [], [], [catalog], 50)
        assert skills == ATTACHED_SKILLS + [catalog]

    def test_skills_are_capped_at_the_limit(self, utils):
        ranked = [{'id': i, 'name': f's{i}'} for i in range(1, 10)]
        _, _, skills = utils.offer_attached_resources(CURRENT_CONFIG, [], [], ranked, 4)
        assert [s['id'] for s in skills] == [40, 41, 1, 2]

    def test_attached_skills_above_the_limit_are_all_kept(self, utils):
        _, _, skills = utils.offer_attached_resources(CURRENT_CONFIG, [], [], [{'id': 1, 'name': 's1'}], 1)
        assert [s['id'] for s in skills] == [40, 41]

    def test_nothing_attached_leaves_the_pool_as_ranked(self, utils):
        ranked = [{'id': 1, 'name': 's1'}]
        assert utils.offer_attached_resources({}, [], [], ranked, 50) == ([], [], ranked)


@pytest.fixture
def draft_utils():
    from fixtures.helpers import load_utils_module
    utils_stub = types.ModuleType('utils')
    utils_stub.get_public_project_id = lambda: PUBLIC_PROJECT
    return load_utils_module(
        PLUGIN_ROOT / 'utils', 'draft_llm_utils',
        extra_stubs={'plugins.elitea_core.utils.utils': utils_stub},
    )


class TestEditDraftKeepsAttached:
    """The offered filter is what used to drop attached items; feed it the lists the endpoint builds."""

    def test_attached_items_echoed_by_the_model_survive_the_offered_filter(self, utils, draft_utils):
        ranked_skills = [{'id': i, 'name': f's{i}'} for i in range(1, 6)]
        agents, pipelines, skills = utils.offer_attached_resources(CURRENT_CONFIG, [], [], ranked_skills, 50)
        parsed = {
            'suggested_skills': [{'id': 40}, {'id': 41}],
            'suggested_agents': [{'application_id': 9, 'type': 'agent'}],
            'suggested_pipelines': [{'application_id': 12, 'type': 'pipeline'}],
        }
        draft_utils.apply_offered_origins(parsed, PROJECT, agents, pipelines, skills)
        assert [s['id'] for s in parsed['suggested_skills']] == [40, 41]
        assert [a['application_id'] for a in parsed['suggested_agents']] == [9]
        assert [p['application_id'] for p in parsed['suggested_pipelines']] == [12]

    def test_kept_skills_take_their_description_from_the_offered_item(self, utils, draft_utils):
        offered = [{'id': 1, 'name': 's1', 'description': 'full text ' * 200},
                   {'id': 2, 'name': 's2', 'description': None, 'project_id': PUBLIC_PROJECT}]
        parsed = {'suggested_skills': [{'id': 1, 'project_id': PROJECT},
                                       {'id': 2, 'project_id': PUBLIC_PROJECT, 'description': 'invented'}]}
        draft_utils.apply_offered_origins(parsed, PROJECT, [], [], offered)
        assert [s['description'] for s in parsed['suggested_skills']] == ['full text ' * 200, None]

    def test_an_id_never_offered_is_still_dropped(self, utils, draft_utils):
        agents, pipelines, skills = utils.offer_attached_resources(CURRENT_CONFIG, [], [], [], 50)
        parsed = {'suggested_skills': [{'id': 999}]}
        draft_utils.apply_offered_origins(parsed, PROJECT, agents, pipelines, skills)
        assert parsed['suggested_skills'] == []


class _EditSession:
    def __init__(self, versions, applications, tools):
        self.versions = versions
        self.applications = applications
        self.tools = tools

    def query(self, model):
        session = self

        class _Version:
            application = types.SimpleNamespace(name='host', description='')
            instructions = welcome_message = ''
            conversation_starters = []
            llm_settings = {}

            def to_dict(self):
                return {'tools': session.tools, 'skills': [], 'variables': []}

        return types.SimpleNamespace(filter=lambda *a: types.SimpleNamespace(first=lambda: _Version()))

    def get(self, model, key):
        source = self.versions if model.__name__ == 'ApplicationVersion' else self.applications
        return source.get(key)


class TestAttachedApplicationKind:
    """The kind of an attached app comes from the version it links to; tool settings rarely carry it."""

    def _config(self, load, tools):
        versions = {6: types.SimpleNamespace(agent_type='pipeline'), 306: types.SimpleNamespace(agent_type='openai')}
        applications = {6: types.SimpleNamespace(name='Demo Pipeline', description=None),
                        194: types.SimpleNamespace(name='Echo Agent', description=None)}
        module = load(lambda project_id: _EditSession(versions, applications, tools))
        return module.fetch_application_for_edit(PROJECT, 201, 315)

    def test_pipeline_without_agent_type_in_settings_is_a_pipeline(self, load):
        config = self._config(load, [
            {'id': 138, 'type': 'application', 'name': 'base',
             'settings': {'application_id': 6, 'application_version_id': 6}},
            {'id': 137, 'type': 'application', 'name': 'base',
             'settings': {'application_id': 194, 'application_version_id': 306}},
        ])
        assert config['attached_pipelines'] == [
            {'application_id': 6, 'name': 'Demo Pipeline', 'description': None, 'type': 'pipeline'}]
        assert config['attached_agents'] == [
            {'application_id': 194, 'name': 'Echo Agent', 'description': None, 'type': 'agent'}]

    def test_missing_linked_version_falls_back_to_settings(self, load):
        config = self._config(load, [
            {'id': 140, 'type': 'application', 'name': 'base',
             'settings': {'application_id': 6, 'application_version_id': 999, 'agent_type': 'pipeline'}},
        ])
        assert [p['application_id'] for p in config['attached_pipelines']] == [6]
