import contextlib
import types
from datetime import datetime

import pytest

from fixtures.helpers import load_utils_module

PACKAGE = 'plugins.elitea_core'
TEMPLATE_PROJECT_ID = 2
PUBLIC_PROJECT_ID = 1
OTHER_PROJECT_ID = 9

ICON = {'name': 'robot.png', 'url': 'https://host/robot.png'}
SKILL_ICON = {'name': 'brain.png', 'url': 'https://host/brain.png'}


class FakeColumn:
    def in_(self, ids):
        return ('in', frozenset(ids))


class FakeVersion:
    def __init__(self, version_id, name='base', icon_meta=None, created_at=datetime(2025, 1, 1)):
        self.id = version_id
        self.name = name
        self.meta = {'icon_meta': icon_meta} if icon_meta is not None else {}
        self.created_at = created_at


class FakeEntity:
    def __init__(self, entity_id, versions, default_version_id=None):
        self.id = entity_id
        self.versions = versions
        self.meta = {'default_version_id': default_version_id} if default_version_id else {}

    def get_default_version(self):
        default_id = self.meta.get('default_version_id')
        return (
            next((v for v in self.versions if v.id == default_id), None)
            or next((v for v in self.versions if v.name == 'base'), None)
        )


def make_model(name):
    return type(name, (), {'id': FakeColumn(), 'versions': 'versions', '__name__': name})


class FakeQuery:
    def __init__(self, rows):
        self._rows = rows
        self._ids = None

    def options(self, *_):
        return self

    def filter(self, clause):
        self._ids = clause[1]
        return self

    def all(self):
        return [row for row in self._rows if row.id in self._ids]


@pytest.fixture
def harness(isolated_sys_modules, utils_path):
    Application = make_model('Application')
    Skill = make_model('Skill')
    rows = {}
    queries = []
    failing_projects = set()

    @contextlib.contextmanager
    def with_project_schema_session(project_id):
        if project_id in failing_projects:
            raise RuntimeError('db down')

        class Session:
            def query(self, model):
                queries.append((project_id, model.__name__))
                return FakeQuery(rows.get((project_id, model), []))

        yield Session()

    pylon = types.ModuleType('pylon')
    pylon_core = types.ModuleType('pylon.core')
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(exception=lambda *a, **kw: None)
    sqlalchemy_orm = types.ModuleType('sqlalchemy.orm')
    sqlalchemy_orm.selectinload = lambda attr: attr
    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(with_project_schema_session=with_project_schema_session)
    utils = types.ModuleType(f'{PACKAGE}.utils.utils')
    utils.get_public_project_id = lambda: PUBLIC_PROJECT_ID
    models_all = types.ModuleType(f'{PACKAGE}.models.all')
    models_all.Application = Application
    models_skill = types.ModuleType(f'{PACKAGE}.models.skill')
    models_skill.Skill = Skill

    module = load_utils_module(utils_path, 'chat_template_icons', extra_stubs={
        'pylon': pylon,
        'pylon.core': pylon_core,
        'pylon.core.tools': pylon_tools,
        'sqlalchemy.orm': sqlalchemy_orm,
        'tools': tools,
        f'{PACKAGE}.utils.utils': utils,
        f'{PACKAGE}.models.all': models_all,
        f'{PACKAGE}.models.skill': models_skill,
    })

    def add(project_id, model, entity):
        rows.setdefault((project_id, model), []).append(entity)

    def attach(participants):
        templates = [{'id': 1, 'participants': participants}]
        return module.attach_participant_icons(templates, TEMPLATE_PROJECT_ID)[0]['participants']

    return types.SimpleNamespace(
        attach=attach, add=add, queries=queries, failing_projects=failing_projects,
        Application=Application, Skill=Skill,
    )


def agent(entity_id, project_id=TEMPLATE_PROJECT_ID, entity_name='application'):
    return {'id': entity_id, 'entity_name': entity_name, 'project_id': project_id, 'name': f'#{entity_id}'}


class TestAttachParticipantIcons:
    def test_agents_pipelines_and_skills_get_their_current_icon(self, harness):
        harness.add(TEMPLATE_PROJECT_ID, harness.Application, FakeEntity(3, [FakeVersion(30, icon_meta=ICON)]))
        harness.add(TEMPLATE_PROJECT_ID, harness.Application, FakeEntity(4, [FakeVersion(40, icon_meta=ICON)]))
        harness.add(PUBLIC_PROJECT_ID, harness.Skill, FakeEntity(8, [FakeVersion(80, icon_meta=SKILL_ICON)]))

        result = harness.attach([
            agent(3),
            agent(4, entity_name='pipeline'),
            {'id': 8, 'entity_name': 'skill', 'project_id': PUBLIC_PROJECT_ID},
        ])

        assert [p.get('icon_meta') for p in result] == [ICON, ICON, SKILL_ICON]

    def test_icon_comes_from_the_default_version(self, harness):
        versions = [FakeVersion(30, icon_meta=ICON), FakeVersion(31, name='v2', icon_meta=SKILL_ICON)]
        harness.add(TEMPLATE_PROJECT_ID, harness.Application, FakeEntity(3, versions, default_version_id=31))

        assert harness.attach([agent(3)])[0]['icon_meta'] == SKILL_ICON

    def test_without_a_base_or_default_version_the_oldest_version_is_used(self, harness):
        versions = [
            FakeVersion(31, name='v2', icon_meta=SKILL_ICON, created_at=datetime(2025, 2, 1)),
            FakeVersion(30, name='v1', icon_meta=ICON, created_at=datetime(2025, 1, 1)),
        ]
        harness.add(TEMPLATE_PROJECT_ID, harness.Application, FakeEntity(3, versions))

        assert harness.attach([agent(3)])[0]['icon_meta'] == ICON

    def test_a_reset_icon_with_a_blank_url_is_not_attached(self, harness):
        reset = {'name': '', 'url': ''}
        harness.add(TEMPLATE_PROJECT_ID, harness.Application, FakeEntity(3, [FakeVersion(30, icon_meta=reset)]))

        assert 'icon_meta' not in harness.attach([agent(3)])[0]

    def test_toolkits_and_users_are_not_looked_up(self, harness):
        result = harness.attach([
            {'id': 5, 'entity_name': 'toolkit', 'project_id': TEMPLATE_PROJECT_ID},
            {'id': 7, 'entity_name': 'user'},
        ])

        assert harness.queries == []
        assert all('icon_meta' not in p for p in result)

    def test_participants_from_other_projects_are_not_read(self, harness):
        harness.add(OTHER_PROJECT_ID, harness.Application, FakeEntity(3, [FakeVersion(30, icon_meta=ICON)]))

        result = harness.attach([agent(3, project_id=OTHER_PROJECT_ID)])

        assert harness.queries == []
        assert 'icon_meta' not in result[0]

    def test_a_participant_without_project_id_is_read_from_the_template_project(self, harness):
        harness.add(TEMPLATE_PROJECT_ID, harness.Application, FakeEntity(3, [FakeVersion(30, icon_meta=ICON)]))

        assert harness.attach([agent(3, project_id=None)])[0]['icon_meta'] == ICON

    def test_one_query_per_project_and_entity_model(self, harness):
        for entity_id in (3, 4, 5):
            harness.add(TEMPLATE_PROJECT_ID, harness.Application,
                        FakeEntity(entity_id, [FakeVersion(entity_id * 10, icon_meta=ICON)]))

        harness.attach([agent(3), agent(4, entity_name='pipeline'), agent(5)])

        assert harness.queries == [(TEMPLATE_PROJECT_ID, 'Application')]

    def test_a_deleted_entity_keeps_the_participant_without_an_icon(self, harness):
        assert 'icon_meta' not in harness.attach([agent(3)])[0]

    def test_a_failing_project_does_not_break_icons_from_other_projects(self, harness):
        harness.failing_projects.add(PUBLIC_PROJECT_ID)
        harness.add(TEMPLATE_PROJECT_ID, harness.Application, FakeEntity(3, [FakeVersion(30, icon_meta=ICON)]))

        result = harness.attach([agent(3), {'id': 8, 'entity_name': 'skill', 'project_id': PUBLIC_PROJECT_ID}])

        assert result[0]['icon_meta'] == ICON
        assert 'icon_meta' not in result[1]
