"""Chat import (#7429): participants are linked by identity, filtered by access, and never silently swapped.

Covers:
  - the exact original entity (same project or public, by ref) wins over a lower-id same-name one
  - a ref into another project never matches by id; a same-name entity is reported as name_only
  - folder-restricted entities are filtered out of every lookup
  - public agents pin a published version; the pinned version and its variables survive
  - commit links exactly what the preview staged, minus skipped rows, with no fallback candidate

Run via:
    python tests/run_tests.py unit/test_7429_chat_import_participants.py -v
"""
import importlib.util
import json
import pathlib
import sys
import types
from datetime import datetime
from types import SimpleNamespace

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

PKG = 'elitea_core_7429_import'
TARGET_PROJECT = 7
PUBLIC_PROJECT = 1
SOURCE_PROJECT = 3


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


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class Column:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return ('eq', self.name, other)

    def is_(self, other):
        return ('is', self.name, other)

    __hash__ = object.__hash__


class Model:
    id = Column('id')
    name = Column('name')
    versions = 'versions'


class Application(Model):
    pass


class Skill(Model):
    pass


class EliteATool(Model):
    pass


class SkillRunError(Exception):
    pass


def build_skill_run_target(skill, version_id, published_only):
    """Mirror of the real rule: a pinned id must exist (and be published for public); else newest/default."""
    if version_id is not None:
        version = next((v for v in skill.versions if v.id == version_id), None)
        if version is None or (published_only and get_version_status(version) != 'published'):
            raise SkillRunError('not runnable')
        return SimpleNamespace(version_id=version.id)
    candidates = [v for v in skill.versions if not published_only or get_version_status(v) == 'published']
    if not candidates:
        raise SkillRunError('no runnable version')
    return SimpleNamespace(version_id=candidates[-1].id)


def get_version_status(version):
    return str(version.status)


class ParticipantCreate:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


@pytest.fixture(scope='module')
def ci():
    register_package(PKG)
    register_package(f'{PKG}.models')
    register_package(f'{PKG}.models.enums')
    register_package(f'{PKG}.models.message_items')
    register_package(f'{PKG}.models.pd')
    register_package(f'{PKG}.utils')
    enums = load_module(f'{PKG}.models.enums.all', PLUGIN_ROOT / 'models' / 'enums' / 'all.py')
    load_module(f'{PKG}.models.pd.conversation_import', PLUGIN_ROOT / 'models' / 'pd' / 'conversation_import.py')
    register_module(f'{PKG}.models.all', Application=Application)
    register_module(f'{PKG}.models.elitea_tools', EliteATool=EliteATool)
    register_module(f'{PKG}.models.skill', Skill=Skill)
    register_module(f'{PKG}.models.conversation', Conversation=object)
    register_module(f'{PKG}.models.message_group', ConversationMessageGroup=object)
    register_module(f'{PKG}.models.message_items.attachment', AttachmentMessageItem=object)
    register_module(f'{PKG}.models.message_items.canvas', CanvasMessageItem=object, CanvasVersionItem=object)
    register_module(f'{PKG}.models.message_items.text', TextMessageItem=object)
    register_module(f'{PKG}.models.pd.conversation', ConversationCreate=object)
    register_module(f'{PKG}.models.pd.participant', ParticipantCreate=ParticipantCreate)
    register_module(f'{PKG}.models.pd.tool', ToolDetails=SimpleNamespace(model_validate=None))
    register_module(f'{PKG}.utils.authors', get_authors_data=lambda ids: [])
    register_module(f'{PKG}.utils.attachments', FileToAIProcessorCollector=object,
            NotSupportableProcessorExtension=Exception, parse_filepath=lambda p: p)
    register_module(f'{PKG}.utils.chat_crypto', CHAT_ENVELOPE=None, ENVELOPE_SUFFIX='.enc',
            EnvelopeIntegrityError=Exception, EnvelopeKeyMismatch=Exception,
            chat_master_key=lambda: None, iter_file_chunks=lambda f: [])
    register_module(f'{PKG}.utils.chat_constants', CONVERSATION_NAME_MAX_LENGTH=50)
    register_module(f'{PKG}.utils.conversation_utils', create_conversation=None)
    register_module(f'{PKG}.utils.file_utils', sanitize_filename=lambda n: n)
    register_module(f'{PKG}.utils.folder_access', APPLICATION_ENTITY_TYPES=['agent', 'pipeline'],
            TOOLKIT_ENTITY_TYPES=['toolkit', 'mcp'],
            build_folder_ids_exclusion_clause=lambda types_, column, restricted:
            ('hidden', tuple(types_), tuple(restricted)) if restricted else None,
            get_restricted_folder_ids=lambda project_id, types_, user_id: [])
    register_module(f'{PKG}.utils.internal_tools', get_default_attachment_bucket=lambda p: 'attachments')
    register_module(f'{PKG}.utils.participant_utils', add_participant_to_conversation=None)
    register_module(f'{PKG}.utils.skill_run_utils', SkillRunError=SkillRunError,
            build_skill_run_target=build_skill_run_target)
    register_module(f'{PKG}.utils.utils', get_public_project_id_or_none=lambda: PUBLIC_PROJECT)
    register_module(f'{PKG}.utils.zip_safety', is_compression_bomb=None, is_encrypted_zip_member=None,
            is_unsafe_zip_member=None)

    shared_tools = sys.modules.get('tools')
    sys.modules['tools'] = SimpleNamespace(MinioClient=None, VaultClient=None, auth=None, db=None, rpc_tools=None)
    try:
        module = load_module(f'{PKG}.utils.conversation_import', PLUGIN_ROOT / 'utils' / 'conversation_import.py')
    finally:
        if shared_tools is None:
            sys.modules.pop('tools', None)
        else:
            sys.modules['tools'] = shared_tools
    module.selectinload = lambda *args, **kwargs: None
    module.ENUMS = enums
    return module


class Query:
    """Returns rows matching the recorded id/name filters, the way the ORM query would."""

    def __init__(self, rows, log):
        self.rows = rows
        self.log = log
        self.filters = []

    def options(self, *args):
        return self

    def order_by(self, *args):
        return self

    def filter(self, clause):
        self.log.append(clause)
        clone = Query(self.rows, self.log)
        clone.filters = self.filters + [clause]
        return clone

    def all(self):
        rows = sorted(self.rows, key=lambda r: r.id)
        for clause in self.filters:
            if clause[0] == 'eq':
                rows = [r for r in rows if getattr(r, clause[1]) == clause[2]]
            elif clause[0] == 'is':
                rows = [r for r in rows if getattr(r, clause[1]) is clause[2]]
            elif clause[0] == 'hidden':
                rows = [r for r in rows if r.id not in HIDDEN_IDS]
        return rows


HIDDEN_IDS = set()


class Session:
    def __init__(self, rows):
        self.rows = rows
        self.log = []

    def query(self, model):
        return Query([r for r in self.rows if r.model is model], self.log)


def make_version(vid, name, status='draft', created=1, variables=(), icon=None):
    return SimpleNamespace(
        id=vid, name=name, status=status, created_at=datetime(2026, 1, created), agent_type='openai',
        variables=[SimpleNamespace(id=vid * 10 + i, name=k, value=v) for i, (k, v) in enumerate(variables)],
        meta={'icon_meta': icon} if icon else {},
    )


def make_app(aid, name, versions, default=None):
    return SimpleNamespace(
        model=Application, id=aid, name=name, versions=versions,
        get_default_version=lambda: default if default is not None else next(
            (v for v in versions if v.name == 'base'), None),
    )


def make_skill(sid, name, versions):
    return SimpleNamespace(model=Skill, id=sid, name=name, versions=versions)


def make_participant(ci, pid, name, entity_name='application', ref=None, version=None, public=False):
    import_models = sys.modules[f'{PKG}.models.pd.conversation_import']
    return import_models.ImportParticipant(
        id=pid, entity_name=entity_name, name=name, version=version, public=public, ref=ref,
    )


def make_sources(ci, target_rows, public_rows=(), restricted=()):
    return [
        ci.LookupSource(TARGET_PROJECT, Session(list(target_rows)), False, list(restricted)),
        ci.LookupSource(PUBLIC_PROJECT, Session(list(public_rows)), True, list(restricted)),
    ]


@pytest.fixture(autouse=True)
def reset_hidden_ids():
    HIDDEN_IDS.clear()
    yield
    HIDDEN_IDS.clear()


def test_same_project_ref_links_the_exact_agent_not_the_lowest_id_namesake(ci):
    namesake = make_app(4, 'Agent-X', [make_version(40, 'base')])
    original = make_app(9, 'Agent-X', [make_version(90, 'base'), make_version(91, 'v2', variables=[('lang', 'de')])])
    participant = make_participant(ci, 1, 'Agent-X', ref={'project_id': TARGET_PROJECT, 'entity_id': 9}, version='v2')

    row = ci._resolve_participant(make_sources(ci, [namesake, original]), participant)

    assert row['status'] == ci.ImportParticipantStatus.same
    assert row['target']['entity_id'] == 9
    assert row['target']['entity_settings']['version_id'] == 91
    assert row['target']['entity_settings']['variables'] == [{'id': 910, 'name': 'lang', 'value': 'de'}]


def test_ref_into_another_project_never_matches_by_numeric_id(ci):
    unrelated_same_id = make_app(9, 'Totally different', [make_version(90, 'base')])
    namesake = make_app(12, 'Agent-X', [make_version(120, 'base')])
    participant = make_participant(ci, 1, 'Agent-X', ref={'project_id': SOURCE_PROJECT, 'entity_id': 9})

    row = ci._resolve_participant(make_sources(ci, [unrelated_same_id, namesake]), participant)

    assert row['status'] == ci.ImportParticipantStatus.name_only
    assert row['target']['entity_id'] == 12


def test_legacy_file_without_ref_is_name_only(ci):
    namesake = make_app(4, 'Agent-X', [make_version(40, 'base')])

    row = ci._resolve_participant(make_sources(ci, [namesake]), make_participant(ci, 1, 'Agent-X'))

    assert row['status'] == ci.ImportParticipantStatus.name_only
    assert row['target']['entity_settings']['version_id'] == 40


def test_restricted_entity_is_neither_linked_nor_named(ci):
    hidden = make_app(4, 'Secret', [make_version(40, 'base')])
    HIDDEN_IDS.add(4)
    participant = make_participant(ci, 1, 'Secret', ref={'project_id': TARGET_PROJECT, 'entity_id': 4})

    row = ci._resolve_participant(make_sources(ci, [hidden], restricted=[77]), participant)

    assert row['status'] == ci.ImportParticipantStatus.not_found
    assert row['target'] is None
    assert ci.build_participant_preview(row).get('target_name') is None


def test_restricted_folder_clause_is_applied_to_lookups(ci):
    source = ci.LookupSource(TARGET_PROJECT, Session([]), False, [77])

    ci._find_entities_by_name(source, make_participant(ci, 1, 'Agent-X'))

    assert ('hidden', ('agent', 'pipeline'), (77,)) in source.session.log


def test_public_agent_pins_its_published_version_not_the_draft_base(ci):
    published = make_version(51, '1.0', status='published', created=2)
    public_agent = make_app(5, 'Pub-A', [make_version(50, 'base'), published])
    participant = make_participant(ci, 1, 'Pub-A', ref={'project_id': PUBLIC_PROJECT, 'entity_id': 5},
                                   version='base', public=True)

    row = ci._resolve_participant(make_sources(ci, [], [public_agent]), participant)

    assert row['status'] == ci.ImportParticipantStatus.same
    assert row['target']['project_id'] == PUBLIC_PROJECT
    assert row['target']['entity_settings']['version_id'] == 51


def test_unpublished_public_agent_is_not_found(ci):
    unpublished = make_app(6, 'Pub-B', [make_version(60, 'base')])
    participant = make_participant(ci, 1, 'Pub-B', ref={'project_id': PUBLIC_PROJECT, 'entity_id': 6}, public=True)

    row = ci._resolve_participant(make_sources(ci, [], [unpublished]), participant)

    assert row['status'] == ci.ImportParticipantStatus.not_found


def test_skill_keeps_its_pinned_version_when_runnable(ci):
    skill = make_skill(3, 'Sk', [make_version(30, 'base'), make_version(31, 'v2')])
    participant = make_participant(ci, 1, 'Sk', entity_name='skill',
                                   ref={'project_id': TARGET_PROJECT, 'entity_id': 3}, version='base')

    row = ci._resolve_participant(make_sources(ci, [skill]), participant)

    assert row['target']['entity_settings'] == {'version_id': 30}


def test_rows_sharing_one_target_are_marked(ci):
    rows = ci._mark_shared_targets([
        {'id': 1, 'entity_name': 'application', 'target': {'project_id': 7, 'entity_id': 4}},
        {'id': 2, 'entity_name': 'application', 'target': {'project_id': 7, 'entity_id': 4}},
    ])

    assert 'same_as' not in rows[0]
    assert rows[1]['same_as'] == 1


def test_status_survives_the_staging_json_round_trip(ci):
    row = {'status': ci.ImportParticipantStatus.name_only}

    loaded = json.loads(json.dumps(row))

    assert loaded['status'] == 'name_only'
    assert loaded['status'] == ci.ImportParticipantStatus.name_only


def make_staged_row(row_id, entity_id, project_id=TARGET_PROJECT, entity_name='application'):
    return {
        'id': row_id, 'entity_name': entity_name, 'name': f'A{entity_id}', 'status': 'name_only',
        'target': {'project_id': project_id, 'entity_id': entity_id, 'name': f'A{entity_id}',
                   'entity_settings': {'version_id': entity_id * 10}},
    }


class Conversation:
    id = 100

    def __init__(self):
        self.participants = []


class CommitSession:
    def __init__(self, conversation):
        self.conversation = conversation

    def flush(self):
        pass

    def refresh(self, conversation):
        pass


def run_commit(ci, monkeypatch, staged, skipped=(), failing=()):
    conversation = Conversation()
    calls = []

    def add(project_id, session, participant, conversation, initiator_id):
        calls.append((participant.entity_meta['id'], participant.entity_settings))
        if participant.entity_meta['id'] in failing:
            raise RuntimeError('gone')
        conversation.participants.append(SimpleNamespace(
            id=1000 + participant.entity_meta['id'], entity_name=participant.entity_name,
            entity_meta=participant.entity_meta,
        ))

    monkeypatch.setattr(ci, 'add_participant_to_conversation', add)
    mapping = ci._link_ai_participants(
        CommitSession(conversation), TARGET_PROJECT, 5, conversation, staged, set(skipped),
    )
    return mapping, calls


def test_commit_links_staged_targets_with_their_settings(ci, monkeypatch):
    mapping, calls = run_commit(ci, monkeypatch, [make_staged_row(1, 4), make_staged_row(2, 8)])

    assert mapping == {1: 1004, 2: 1008}
    assert calls == [(4, {'version_id': 40}), (8, {'version_id': 80})]


def test_commit_skips_unticked_rows(ci, monkeypatch):
    mapping, calls = run_commit(ci, monkeypatch, [make_staged_row(1, 4), make_staged_row(2, 8)], skipped=[2])

    assert mapping == {1: 1004}
    assert [c[0] for c in calls] == [4]


def test_commit_never_falls_back_to_another_candidate(ci, monkeypatch):
    mapping, calls = run_commit(ci, monkeypatch, [make_staged_row(1, 4)], failing=[4])

    assert mapping == {}
    assert [c[0] for c in calls] == [4]


def test_commit_shares_one_participant_for_rows_with_one_target(ci, monkeypatch):
    mapping, calls = run_commit(ci, monkeypatch, [make_staged_row(1, 4), make_staged_row(2, 4)])

    assert mapping == {1: 1004, 2: 1004}
    assert len(calls) == 1


def test_commit_ignores_not_found_rows(ci, monkeypatch):
    mapping, calls = run_commit(ci, monkeypatch, [{'id': 1, 'entity_name': 'application', 'target': None}])

    assert mapping == {}
    assert calls == []
