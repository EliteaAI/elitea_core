import contextlib
import datetime
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PACKAGE = 'plugins.elitea_core'

CHAT_PROJECT_ID = 7
PUBLIC_PROJECT_ID = 1
FOREIGN_PROJECT_ID = 9
DEFAULT_MODEL = {'model_name': 'default-model', 'model_project_id': CHAT_PROJECT_ID}
AVAILABLE_MODELS = {('saved-model', 3), ('override-model', 3), ('conversation-model', 3)}


def _load(relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def _stub(name, **attrs):
    module = types.ModuleType(f'{PACKAGE}.{name}')
    module.__dict__.update(attrs)
    sys.modules[module.__name__] = module
    return module


def resolve_fake_llm_settings(project_id, llm_settings, **kwargs):
    settings = dict(llm_settings or {})
    name, project = settings.get('model_name'), settings.get('model_project_id')
    if name and project is None:
        project = next((p for (n, p) in AVAILABLE_MODELS if n == name), None)
    if name and (name, project) in AVAILABLE_MODELS:
        return {**settings, 'model_project_id': project}
    return {**settings, **DEFAULT_MODEL}


class FakeVersion:
    def __init__(self, id, name='base', status='draft', instructions='You review code.', created_at=1,
                 meta=None, run_settings=None):
        self.id = id
        self.name = name
        self.status = status
        self.instructions = instructions
        self.created_at = datetime.datetime(2026, 1, created_at)
        self.meta = meta or {}
        self.run_settings = run_settings


class FakeSkill:
    def __init__(self, id, name, versions):
        self.id = id
        self.name = name
        self.versions = versions

    def get_default_version(self):
        return self.versions[0]


class FakeQuery:
    def __init__(self, rows):
        self.rows = rows

    def options(self, *args):
        return self

    def filter(self, wanted_id, *conditions):
        if isinstance(wanted_id, frozenset):
            return FakeQuery([row for row in self.rows if row.id in wanted_id])
        return FakeQuery([row for row in self.rows if row.id == wanted_id])

    def all(self):
        return list(self.rows)

    def where(self, *args):
        return self

    def order_by(self, *args):
        return self

    def offset(self, count):
        return FakeQuery(self.rows[count:])

    def first(self):
        return self.rows[0] if self.rows else None


class IdColumn:
    def __eq__(self, other):
        return other

    def in_(self, values):
        return frozenset(values)


class Column:
    def __eq__(self, other):
        return True


class FakePredictPayload:
    def __init__(self, llm_settings=None):
        self.project_id = CHAT_PROJECT_ID
        self.llm_settings = types.SimpleNamespace(model_dump=lambda **kwargs: llm_settings) if llm_settings else None


@pytest.fixture
def env(isolated_sys_modules):
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.pd',
                 f'{PACKAGE}.models.enums'):
        _package(name)

    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(exception=lambda *a, **k: None, warning=lambda *a, **k: None)
    _package('pylon').core = _package('pylon.core')
    sys.modules['pylon.core'].tools = pylon_tools
    sys.modules['pylon.core.tools'] = pylon_tools

    schemas = {}

    @contextlib.contextmanager
    def with_project_schema_session(project_id):
        yield types.SimpleNamespace(query=lambda model: FakeQuery(schemas.get(project_id, [])))

    class FakeRpc:
        def timeout(self, seconds):
            return self

        def configurations_get_available_models(self, project_id, section, include_shared):
            return {(project, name): {} for (name, project) in AVAILABLE_MODELS}

        def configurations_get_auto_routing_settings(self, project_id):
            return {'enabled': True}

    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(with_project_schema_session=with_project_schema_session)
    tools.rpc_tools = types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=FakeRpc()))
    tools.auth = types.SimpleNamespace(sio_users={}, current_user=lambda auth_data: {}, is_sio_user_in_project=None)
    sys.modules['tools'] = tools

    sqlalchemy = _package('sqlalchemy')
    sqlalchemy.desc = lambda column: column
    sqlalchemy_orm = types.ModuleType('sqlalchemy.orm')
    sqlalchemy_orm.selectinload = lambda *a: None
    sqlalchemy.orm = sqlalchemy_orm
    sys.modules['sqlalchemy.orm'] = sqlalchemy_orm

    _stub('utils.application_utils', validate_and_resolve_llm_settings=resolve_fake_llm_settings)
    _stub('utils.predict_utils', get_project_context=lambda project_id: {'enabled': False, 'content': ''})
    _stub('utils.utils', get_public_project_id=lambda: PUBLIC_PROJECT_ID)
    _stub('utils.skill_utils', consume_message_skills=lambda content, candidates: (content, list(candidates)))
    _stub('models.skill', Skill=types.SimpleNamespace(id=IdColumn(), versions=None),
          SkillVersion=types.SimpleNamespace(id=IdColumn(), skill_id=Column()))
    _stub('models.message_group', ConversationMessageGroup=types.SimpleNamespace(
        author_participant_id=Column(), conversation_id=Column(), created_at=Column(),
    ))
    _stub('models.participants', ParticipantMapping=types.SimpleNamespace(
        entity_settings=None, participant_id=Column(), conversation_id=Column(),
    ), Participant=types.SimpleNamespace(id=Column(), entity_name=Column()))

    _load('models/enums/all.py', f'{PACKAGE}.models.enums.all')
    for name in ('skill_predict', 'llm', 'skill_run_settings', 'participant'):
        _load(f'models/pd/{name}.py', f'{PACKAGE}.models.pd.{name}')
    for name in ('exceptions', 'mcp_versioning', 'sio_utils', 'project_context_utils', 'usage_attribution',
                 'skill_run_settings', 'skill_run_utils', 'skill_mentions', 'skill_llm_override'):
        _load(f'utils/{name}.py', f'{PACKAGE}.utils.{name}')
    module = _load('utils/skill_participant_utils.py', f'{PACKAGE}.utils.skill_participant_utils')

    schemas[CHAT_PROJECT_ID] = [FakeSkill(10, 'Reviewer', [
        FakeVersion(100, 'base', instructions='Own base.', meta={'icon_meta': {'url': 'own.png'}},
                    run_settings={'llm_settings': {'model_name': 'saved-model', 'model_project_id': 3}}),
        FakeVersion(101, 'v2', instructions='Own v2.'),
    ])]
    schemas[PUBLIC_PROJECT_ID] = [FakeSkill(10, 'Catalog reviewer', [
        FakeVersion(200, 'base', status='published', instructions='Catalog published.'),
        FakeVersion(201, 'draft', status='draft', instructions='Catalog draft.'),
    ])]
    schemas[FOREIGN_PROJECT_ID] = [FakeSkill(10, 'Private elsewhere', [FakeVersion(300)])]
    return types.SimpleNamespace(mod=module, schemas=schemas)


def build_msg_group(project_id, previous_messages=()):
    participant = types.SimpleNamespace(id=55, entity_meta={'id': 10, 'project_id': project_id})
    rows = [types.SimpleNamespace(id=0, meta={})] + list(previous_messages)
    session = types.SimpleNamespace(query=lambda model: FakeQuery(rows))
    return session, types.SimpleNamespace(sent_to=participant, sent_to_id=55, conversation_id=3)


class TestParticipantSource:
    def test_own_skill_runs_any_version(self, env):
        source = env.mod.resolve_skill_participant({'id': 10, 'project_id': CHAT_PROJECT_ID}, CHAT_PROJECT_ID)
        assert source.project_id == CHAT_PROJECT_ID and not source.published_only

    def test_catalog_skill_is_limited_to_published_versions(self, env):
        source = env.mod.resolve_skill_participant({'id': 10, 'project_id': PUBLIC_PROJECT_ID}, CHAT_PROJECT_ID)
        assert source.published_only
        with pytest.raises(env.mod.SkillParticipantError, match='not published'):
            env.mod.load_skill_participant_target(
                {'id': 10, 'project_id': PUBLIC_PROJECT_ID}, CHAT_PROJECT_ID, 201,
            )

    def test_skill_from_another_private_project_is_rejected(self, env):
        with pytest.raises(env.mod.SkillParticipantError, match='this project or the public catalog'):
            env.mod.load_skill_participant_target(
                {'id': 10, 'project_id': FOREIGN_PROJECT_ID}, CHAT_PROJECT_ID, None,
            )

    def test_missing_version_is_a_participant_error(self, env):
        with pytest.raises(env.mod.SkillParticipantError, match="'999' not found"):
            env.mod.load_skill_participant_target({'id': 10, 'project_id': CHAT_PROJECT_ID}, CHAT_PROJECT_ID, 999)

    def test_details_name_the_skill_for_the_participant_row(self, env):
        details = env.mod.load_skill_participant_details({'id': 10, 'project_id': CHAT_PROJECT_ID})
        assert details == {'name': 'Reviewer', 'icon_meta': {'url': 'own.png'}}


class TestPayload:
    def test_pinned_version_becomes_the_tool_less_system_prompt(self, env):
        session, msg_group = build_msg_group(CHAT_PROJECT_ID)
        payload = env.mod.build_skill_participant_payload(session, msg_group, FakePredictPayload(), {'version_id': 101})
        details = payload['version_details']
        assert details['instructions'] == 'Own v2.'
        assert details['tools'] == [] and payload['tools'] == [] and payload['internal_tools'] == []
        assert 'application_id' not in payload and 'version_id' not in payload

    def test_chat_owns_stream_identity_and_history(self, env):
        session, msg_group = build_msg_group(CHAT_PROJECT_ID)
        payload = env.mod.build_skill_participant_payload(session, msg_group, FakePredictPayload(), {})
        assert not {'stream_id', 'message_id', 'user_input', 'chat_history'} & payload.keys()

    def test_saved_run_settings_pick_the_model_without_an_override(self, env):
        session, msg_group = build_msg_group(CHAT_PROJECT_ID)
        payload = env.mod.build_skill_participant_payload(session, msg_group, FakePredictPayload(), {'version_id': 100})
        assert payload['llm_settings']['model_name'] == 'saved-model'

    def test_message_override_beats_conversation_override(self, env):
        session, msg_group = build_msg_group(CHAT_PROJECT_ID)
        payload = env.mod.build_skill_participant_payload(
            session, msg_group, FakePredictPayload({'model_name': 'override-model', 'model_project_id': 3}),
            {'version_id': 100, 'llm_settings': {'model_name': 'conversation-model', 'model_project_id': 3}},
        )
        assert payload['llm_settings']['model_name'] == 'override-model'

    def test_conversation_override_applies_without_a_message_override(self, env):
        session, msg_group = build_msg_group(CHAT_PROJECT_ID)
        payload = env.mod.build_skill_participant_payload(
            session, msg_group, FakePredictPayload(),
            {'version_id': 100, 'llm_settings': {'model_name': 'conversation-model', 'model_project_id': 3}},
        )
        assert payload['llm_settings']['model_name'] == 'conversation-model'

    def test_catalog_skill_keeps_its_own_project_as_the_attribution_root(self, env):
        session, msg_group = build_msg_group(PUBLIC_PROJECT_ID)
        payload = env.mod.build_skill_participant_payload(session, msg_group, FakePredictPayload(), {})
        assert payload['project_id'] == PUBLIC_PROJECT_ID
        assert payload['version_details']['instructions'] == 'Catalog published.'

    def test_dispatch_carries_attribution_and_the_chat_project_socket_check(self, env):
        session, msg_group = build_msg_group(PUBLIC_PROJECT_ID)
        payload = env.mod.build_skill_participant_payload(session, msg_group, FakePredictPayload(), {})
        rpc_kwargs, start_event = env.mod.pop_skill_dispatch(payload, {'participant_id': 55, 'question_id': 'q'})
        assert env.mod.SKILL_DISPATCH_KEY not in payload
        assert rpc_kwargs['sid_project_id'] == CHAT_PROJECT_ID
        assert rpc_kwargs['usage_entity']['root'] == {'type': 'skill', 'id': 10, 'version_id': 200}
        assert rpc_kwargs['applied_skills'] == [{'skill_id': 10, 'name': 'Catalog reviewer', 'icon_meta': None}]
        assert start_event['participant_id'] == 55 and start_event['question_id'] == 'q'
        assert start_event['skill_run']['skill_version_id'] == 200

    def test_follow_up_turn_resumes_the_previous_thread(self, env):
        session, msg_group = build_msg_group(
            CHAT_PROJECT_ID, [types.SimpleNamespace(id=1, meta={'thread_id': 'thread-1'})],
        )
        payload = env.mod.build_skill_participant_payload(session, msg_group, FakePredictPayload(), {})
        assert payload['thread_id'] == 'thread-1'

    def test_unrunnable_version_surfaces_as_a_participant_error(self, env):
        env.schemas[CHAT_PROJECT_ID][0].versions[1].instructions = '   '
        session, msg_group = build_msg_group(CHAT_PROJECT_ID)
        with pytest.raises(env.mod.SkillParticipantError, match='no instructions'):
            env.mod.build_skill_participant_payload(session, msg_group, FakePredictPayload(), {'version_id': 101})


class TestDispatch:
    def test_non_skill_payload_dispatches_unchanged(self, env):
        payload = {'user_input': 'hi'}
        start = {'participant_id': 1}
        assert env.mod.pop_skill_dispatch(payload, start) == ({}, start)
        assert payload == {'user_input': 'hi'}


class TestAttachmentModel:
    def _session(self, entity_settings):
        mapping = types.SimpleNamespace(entity_settings=entity_settings)
        return types.SimpleNamespace(query=lambda model: FakeQuery([types.SimpleNamespace(id=0, **vars(mapping))]))

    def test_document_extraction_uses_the_skill_model(self, env):
        participant = types.SimpleNamespace(id=55, entity_meta={'id': 10, 'project_id': CHAT_PROJECT_ID})
        settings = env.mod.resolve_skill_attachment_llm_settings(
            self._session({'version_id': 100}), 3, participant, CHAT_PROJECT_ID,
        )
        assert settings['model_name'] == 'saved-model'

    def test_unavailable_skill_leaves_extraction_without_a_model(self, env):
        participant = types.SimpleNamespace(id=55, entity_meta={'id': 10, 'project_id': FOREIGN_PROJECT_ID})
        assert env.mod.resolve_skill_attachment_llm_settings(self._session({}), 3, participant, CHAT_PROJECT_ID) is None


class TestCreateValidation:
    def _participant(self, entity_name, entity_meta, entity_settings=None):
        return types.SimpleNamespace(entity_name=entity_name, entity_meta=entity_meta,
                                     entity_settings=entity_settings or {})

    def test_any_invalid_skill_rejects_the_whole_creation(self, env):
        participants = [
            self._participant('application', {'id': 5, 'project_id': CHAT_PROJECT_ID}),
            self._participant('skill', {'id': 10, 'project_id': PUBLIC_PROJECT_ID}, {'version_id': 201}),
        ]
        with pytest.raises(env.mod.SkillParticipantError, match='not published'):
            env.mod.validate_skill_participants(participants, CHAT_PROJECT_ID)

    def test_valid_skills_and_other_participants_pass(self, env):
        participants = [
            self._participant('user', {'id': 3}),
            self._participant('skill', {'id': 10, 'project_id': CHAT_PROJECT_ID}, {'version_id': 101}),
            self._participant('skill', {'id': 10, 'project_id': PUBLIC_PROJECT_ID}),
        ]
        env.mod.validate_skill_participants(participants, CHAT_PROJECT_ID)
