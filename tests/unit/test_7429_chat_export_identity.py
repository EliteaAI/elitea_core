"""Chat export (#7429): each AI participant carries its identity and the version the chat actually runs.

Covers:
  - agents, skills and toolkits get a ref {project_id, entity_id}; users don't
  - an agent's exported version is the conversation's pinned version, not the default
  - a skill's exported version is its pinned version; a missing pin falls back to the default

Run via:
    python tests/run_tests.py unit/test_7429_chat_export_identity.py -v
"""
import enum
import importlib.util
import pathlib
import sys
import types
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

PKG = 'elitea_core_7429_export'


def register_package(name):
    pkg = types.ModuleType(name)
    pkg.__path__ = []
    sys.modules[name] = pkg
    return pkg


def register_module(name, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    sys.modules[name] = mod
    return mod


class ParticipantTypes(str, enum.Enum):
    user = 'user'
    application = 'application'
    dummy = 'dummy'
    toolkit = 'toolkit'
    skill = 'skill'


class Column:
    def __eq__(self, other):
        return other

    __hash__ = object.__hash__


class Skill:
    id = Column()
    versions = 'versions'


SKILLS = {}


class SkillQuery:
    def options(self, *args):
        return self

    def filter(self, skill_id):
        self.skill_id = skill_id
        return self

    def first(self):
        return SKILLS.get(self.skill_id)


@contextmanager
def open_schema_session(project_id):
    yield SimpleNamespace(query=lambda model: SkillQuery())


APPLICATION = {
    'name': 'Agent-X',
    'version_details': {'name': 'base'},
    'versions': [{'id': 90, 'name': 'base'}, {'id': 91, 'name': 'v2'}],
}


@pytest.fixture(scope='module')
def export_module():
    register_package(PKG)
    register_package(f'{PKG}.models')
    register_package(f'{PKG}.models.message_items')
    register_package(f'{PKG}.models.enums')
    register_package(f'{PKG}.utils')
    register_module(f'{PKG}.models.enums.all', ParticipantTypes=ParticipantTypes)
    register_module(f'{PKG}.models.folder', ConversationFolder=object)
    register_module(f'{PKG}.models.message_group', ConversationMessageGroup=object)
    register_module(f'{PKG}.models.message_items.base', MessageItem=object)
    register_module(f'{PKG}.models.participants', ParticipantMapping=object)
    register_module(f'{PKG}.models.skill', Skill=Skill)
    register_module(f'{PKG}.utils.authors', get_authors_data=lambda ids: [])
    register_module(f'{PKG}.utils.utils', get_public_project_id_or_none=lambda: 1)

    this = SimpleNamespace(module=SimpleNamespace(
        get_application_by_id=lambda **kwargs: APPLICATION,
        get_toolkit_by_id=lambda **kwargs: {'name': 'Tk', 'type': 'github'},
    ))
    shared_tools = sys.modules.get('tools')
    sys.modules['tools'] = SimpleNamespace(
        MinioClient=None, db=SimpleNamespace(with_project_schema_session=open_schema_session), this=this,
    )
    try:
        spec = importlib.util.spec_from_file_location(
            f'{PKG}.utils.conversation_export', PLUGIN_ROOT / 'utils' / 'conversation_export.py',
        )
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
    finally:
        if shared_tools is None:
            sys.modules.pop('tools', None)
        else:
            sys.modules['tools'] = shared_tools
    module.selectinload = lambda *args, **kwargs: None
    return module


def make_participant(pid, entity_name, entity_id, project_id=2):
    return SimpleNamespace(id=pid, entity_name=entity_name, entity_meta={'id': entity_id, 'project_id': project_id})


def make_skill_version(vid, name):
    return SimpleNamespace(id=vid, name=name)


def test_agent_exports_ref_and_its_pinned_version(export_module):
    [agent] = export_module._resolve_participants([make_participant(2, 'application', 5)], {2: 91})

    assert agent['ref'] == {'project_id': 2, 'entity_id': 5}
    assert agent['version'] == 'v2'


def test_agent_without_a_pin_exports_its_default_version(export_module):
    [agent] = export_module._resolve_participants([make_participant(2, 'application', 5)], {})

    assert agent['version'] == 'base'


def test_skill_exports_its_pinned_version_and_falls_back_to_default(export_module):
    base, pinned = make_skill_version(30, 'base'), make_skill_version(31, 'v2')
    SKILLS[3] = SimpleNamespace(name='Sk', versions=[base, pinned], get_default_version=lambda: base)

    [with_pin] = export_module._resolve_participants([make_participant(4, 'skill', 3)], {4: 31})
    [without_pin] = export_module._resolve_participants([make_participant(4, 'skill', 3)], {})

    assert (with_pin['meta']['name'], with_pin['version']) == ('Sk', 'v2')
    assert without_pin['version'] == 'base'


def test_ref_reaches_the_export_file_for_ai_participants_only(export_module):
    participants = export_module._resolve_participants(
        [make_participant(1, 'user', 3), make_participant(2, 'application', 5), make_participant(3, 'toolkit', 8)], {},
    )
    payload = export_module.build_export_payload(
        conversation={'id': 1, 'uuid': 'u', 'name': 'c', 'is_private': True, 'author_id': 3},
        participants=participants, groups=[], include_attachments=False,
        exported_by={'id': 3}, project_id=2, exported_at=datetime(2026, 11, 12, tzinfo=timezone.utc),
    )

    refs = {p['entity_name']: p.get('ref') for p in payload['conversation']['participants']}
    assert refs == {
        'user': None,
        'application': {'project_id': 2, 'entity_id': 5},
        'toolkit': {'project_id': 2, 'entity_id': 8},
    }
