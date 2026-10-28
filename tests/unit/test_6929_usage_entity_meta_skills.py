"""#6929: usage_entity_meta names skills for the Analytics Skills tab.

A skill is looked up in its own schema, which is this project or the public one; one its schema
no longer has is deleted and simply absent. Conversation uuids resolve to this project's ids so a
recent run can link to its Run History analytics.
"""

import importlib.util
import pathlib
import sys
import types
import uuid
from contextlib import contextmanager
from unittest import mock

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'plugins.elitea_core'
PROJECT_ID = 2
PUBLIC_PROJECT_ID = 1
CONVERSATION = '11111111-2222-3333-4444-555555555555'


class AnyModule(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return mock.MagicMock(name=f'{self.__name__}.{name}')


class Query:
    def __init__(self, rows):
        self.rows = rows

    def __getattr__(self, name):
        return lambda *args, **kwargs: self

    def all(self):
        return self.rows


class Schemas:
    """Rows per (project schema, model) and the schemas each model was read from."""

    def __init__(self, models):
        self.models = models
        self.rows = {}
        self.reads = []

    def session(self, project_id):
        schemas = self

        class Session:
            def query(self, first, *rest):
                model = next(name for name, value in schemas.models.items() if first in value)
                schemas.reads.append((project_id, model))
                return Query(schemas.rows.get((project_id, model), []))

        @contextmanager
        def scope():
            yield Session()

        return scope()


@pytest.fixture
def rpc(monkeypatch):
    skill = types.SimpleNamespace(id=mock.MagicMock(), name=mock.MagicMock(), meta=mock.MagicMock())
    version = types.SimpleNamespace(
        id=mock.MagicMock(), skill_id=mock.MagicMock(), name=mock.MagicMock(), meta=mock.MagicMock(),
    )
    conversation = types.SimpleNamespace(
        id=mock.MagicMock(), uuid=mock.MagicMock(),
        name=mock.MagicMock(), is_private=mock.MagicMock(),
        author_id=mock.MagicMock(), source=mock.MagicMock(),
        meta=mock.MagicMock(), created_at=mock.MagicMock(),
    )
    application = types.SimpleNamespace(id=mock.MagicMock(), name=mock.MagicMock())
    participant = mock.MagicMock()
    participant_mapping = mock.MagicMock()
    schemas = Schemas({
        'skill': {skill.id}, 'version': {version.id}, 'conversation': {conversation.id},
        'application': {application.id},
        'participant_mapping': {participant_mapping.conversation_id},
    })
    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(get_session=schemas.session)
    monkeypatch.setitem(sys.modules, 'tools', tools)
    pylon_tools = AnyModule('pylon.core.tools')
    pylon_tools.web = types.SimpleNamespace(rpc=lambda *a, **k: (lambda func: func))
    monkeypatch.setitem(sys.modules, 'pylon.core.tools', pylon_tools)
    for name in ('pylon', 'pylon.core', 'sqlalchemy', f'{PKG}.models.enums.all'):
        monkeypatch.setitem(sys.modules, name, AnyModule(name))
    for name in ('plugins', PKG, f'{PKG}.rpc', f'{PKG}.models', f'{PKG}.models.enums', f'{PKG}.utils'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    models = {
        'all': {'Application': application, 'ApplicationVersion': mock.MagicMock()},
        'conversation': {'Conversation': conversation},
        'participants': {'Participant': participant, 'ParticipantMapping': participant_mapping},
        'skill': {'Skill': skill, 'SkillVersion': version},
        'utils': {'get_public_project_id': lambda: PUBLIC_PROJECT_ID},
    }
    for name, attrs in models.items():
        module_name = f'{PKG}.utils.utils' if name == 'utils' else f'{PKG}.models.{name}'
        module = types.ModuleType(module_name)
        module.__dict__.update(attrs)
        monkeypatch.setitem(sys.modules, module_name, module)
    spec = importlib.util.spec_from_file_location(
        f'{PKG}.rpc.usage_entity_meta', PLUGIN_ROOT / 'rpc' / 'usage_entity_meta.py',
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    module.schemas = schemas
    return module


def call(rpc, **kwargs):
    return rpc.RPC().usage_entity_meta(PROJECT_ID, **kwargs)


def own_and_public_skills(rpc):
    rpc.schemas.rows.update({
        (PROJECT_ID, 'skill'): [(174, 'summarizer', {'default_version_id': 258})],
        (PROJECT_ID, 'version'): [
            (252, 174, 'base', {'icon_meta': {'url': 'base.png'}}),
            (258, 174, 'model-x', {'icon_meta': {'url': 'x.png'}}),
        ],
        (PUBLIC_PROJECT_ID, 'skill'): [(174, 'catalog summarizer', {})],
        (PUBLIC_PROJECT_ID, 'version'): [(145, 174, 'base', {'icon_meta': {'url': 'cat.png'}})],
    })


class TestScopedSkills:
    def test_own_and_public_skills_resolve_in_their_own_schema(self, rpc):
        own_and_public_skills(rpc)

        result = call(rpc, skill_refs=[[PROJECT_ID, 174], [PUBLIC_PROJECT_ID, 174]])

        assert sorted(result['scoped_skills'], key=lambda s: s['project_id']) == [
            {
                'id': 174, 'project_id': PUBLIC_PROJECT_ID, 'name': 'catalog summarizer',
                'icon_meta': {'url': 'cat.png'}, 'versions': [{'id': 145, 'name': 'base'}], 'is_public': True,
            },
            {
                'id': 174, 'project_id': PROJECT_ID, 'name': 'summarizer', 'icon_meta': {'url': 'x.png'},
                'versions': [{'id': 252, 'name': 'base'}, {'id': 258, 'name': 'model-x'}], 'is_public': False,
            },
        ]

    def test_a_deleted_skill_is_absent(self, rpc):
        rpc.schemas.rows[(PROJECT_ID, 'version')] = [(252, 999, 'base', {})]

        result = call(rpc, skill_refs=[[PROJECT_ID, 999]])

        assert result['scoped_skills'] == []

    def test_another_projects_schema_is_never_read(self, rpc):
        call(rpc, skill_refs=[[7, 174]])

        assert all(project_id in (PROJECT_ID, PUBLIC_PROJECT_ID) for project_id, _ in rpc.schemas.reads)
        assert not rpc.schemas.reads

    def test_asking_only_for_skills_reads_no_application(self, rpc):
        own_and_public_skills(rpc)

        call(rpc, skill_refs=[[PROJECT_ID, 174]])

        assert {model for _, model in rpc.schemas.reads} == {'skill', 'version'}

    def test_callers_that_ask_for_no_skills_get_the_old_shape(self, rpc):
        assert call(rpc, application_ids=[]) == {'applications': [], 'versions': [], 'scoped_applications': []}

    def test_an_empty_skill_request_still_answers(self, rpc):
        assert call(rpc, skill_refs=[])['scoped_skills'] == []


class TestConversations:
    def test_uuids_resolve_to_this_projects_ids(self, rpc):
        rpc.schemas.rows[(PROJECT_ID, 'conversation')] = [
            (1701, uuid.UUID(CONVERSATION), 'My Chat', False, None, None, None, None),
        ]

        result = call(rpc, conversation_uuids=[CONVERSATION, 'not-a-uuid'])

        assert result['conversations'] == [{
            'uuid': CONVERSATION, 'id': 1701, 'name': 'My Chat', 'is_private': False,
            'author_id': None, 'source': None, 'is_hidden': False, 'created_at': None,
            'participant_types': [],
        }]
        assert all(project_id == PROJECT_ID for project_id, _ in rpc.schemas.reads)

    def test_only_malformed_uuids_read_nothing(self, rpc):
        assert call(rpc, conversation_uuids=['x'])['conversations'] == []
        assert not rpc.schemas.reads
