import contextlib
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PACKAGE = 'plugins.elitea_core'


def _load_6922_harness():
    spec = importlib.util.spec_from_file_location(
        'skill_participant_harness_6922', pathlib.Path(__file__).with_name('test_6922_skill_participant.py'),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_harness = _load_6922_harness()
CHAT_PROJECT_ID = _harness.CHAT_PROJECT_ID
FOREIGN_PROJECT_ID = _harness.FOREIGN_PROJECT_ID
PUBLIC_PROJECT_ID = _harness.PUBLIC_PROJECT_ID
FakePredictPayload = _harness.FakePredictPayload
FakeQuery = _harness.FakeQuery
FakeVersion = _harness.FakeVersion
build_msg_group = _harness.build_msg_group
env = _harness.env


def _skill_participant(participant_id, skill_id, project_id, version_id=None):
    return {
        'id': participant_id,
        'entity_meta': {'id': skill_id, 'project_id': project_id},
        'entity_settings': {'version_id': version_id} if version_id else {},
    }


class TestConversationDetailsAvailability:
    def test_own_pinned_version_is_available_with_current_name(self, env):
        details = env.mod.describe_skill_participants([_skill_participant(1, 10, CHAT_PROJECT_ID, 101)], CHAT_PROJECT_ID)
        assert details[1] == {'name': 'Reviewer', 'icon_meta': {}, 'version_name': 'v2', 'is_available': True}

    def test_catalog_draft_pin_is_unavailable_and_unnamed(self, env):
        details = env.mod.describe_skill_participants(
            [_skill_participant(1, 10, PUBLIC_PROJECT_ID, 201)], CHAT_PROJECT_ID,
        )
        assert details[1]['is_available'] is False
        assert details[1]['version_name'] is None

    def test_deleted_pinned_version_is_unavailable(self, env):
        details = env.mod.describe_skill_participants([_skill_participant(1, 10, CHAT_PROJECT_ID, 999)], CHAT_PROJECT_ID)
        assert details[1]['is_available'] is False

    def test_foreign_project_skill_is_never_read(self, env):
        details = env.mod.describe_skill_participants(
            [_skill_participant(1, 10, FOREIGN_PROJECT_ID, 300)], CHAT_PROJECT_ID,
        )
        assert details[1] == {'is_available': False, 'version_name': None}

    def test_deleted_skill_keeps_the_stored_chip(self, env):
        details = env.mod.describe_skill_participants([_skill_participant(1, 77, CHAT_PROJECT_ID)], CHAT_PROJECT_ID)
        assert details[1] == {'is_available': False, 'version_name': None}


class TestUnavailablePinnedVersion:
    def _build(self, env, project_id, version_id):
        session, msg_group = build_msg_group(project_id)
        return env.mod.build_skill_participant_payload(
            session, msg_group, FakePredictPayload(), {'version_id': version_id},
        )

    def test_deleted_version_names_the_pin_and_the_way_out(self, env):
        with pytest.raises(env.mod.SkillParticipantError) as error:
            self._build(env, CHAT_PROJECT_ID, 999)
        assert str(error.value) == (
            'Skill version #999 is no longer available. Choose another version or remove the participant.'
        )

    def test_unpublished_catalog_version_does_not_fall_back(self, env):
        with pytest.raises(env.mod.SkillParticipantError, match='#201 is no longer available'):
            self._build(env, PUBLIC_PROJECT_ID, 201)

    def test_foreign_project_keeps_its_own_error(self, env):
        with pytest.raises(env.mod.SkillParticipantError, match=env.mod.FOREIGN_SKILL_ERROR):
            self._build(env, FOREIGN_PROJECT_ID, 300)

    def test_system_prompt_travels_in_version_details_not_the_droppable_instructions(self, env):
        payload = self._build(env, CHAT_PROJECT_ID, 101)
        assert 'instructions' not in payload
        assert payload['version_details']['instructions'].startswith('Own v2.')


class CommitSession:
    commits = 0

    def commit(self):
        self.commits += 1


class TestSkillRunRecord:
    def test_reply_row_keeps_the_skill_run_beside_existing_meta(self, env):
        session, response_msg = CommitSession(), types.SimpleNamespace(meta={'thread_id': 't'})
        env.mod.record_skill_run(session, response_msg, {'question_id': 'q', 'skill_run': {'skill_id': 10}})
        assert response_msg.meta == {'thread_id': 't', 'skill_run': {'skill_id': 10}}
        assert session.commits == 1

    def test_non_skill_turn_leaves_the_reply_untouched(self, env):
        session, response_msg = CommitSession(), types.SimpleNamespace(meta={'thread_id': 't'})
        env.mod.record_skill_run(session, response_msg, {'question_id': 'q'})
        assert response_msg.meta == {'thread_id': 't'}
        assert session.commits == 0

    def test_skill_payload_dispatch_carries_the_skill_run_for_the_reply(self, env):
        session, msg_group = build_msg_group(CHAT_PROJECT_ID)
        payload = env.mod.build_skill_participant_payload(session, msg_group, FakePredictPayload(), {'version_id': 101})
        _, start_event_content = env.mod.pop_skill_dispatch(payload, {'question_id': 'q'})
        assert start_event_content['skill_run']['skill_version_id'] == 101


class MappingSession:
    def __init__(self, entity_settings):
        self.rows = [types.SimpleNamespace(id=0, entity_settings=entity_settings)]

    def query(self, *models):
        return FakeQuery(self.rows)


class TestPinnedVersionUpdate:
    def _participant(self, project_id):
        return types.SimpleNamespace(id=55, entity_meta={'id': 10, 'project_id': project_id})

    def test_update_without_version_keeps_the_current_pin(self, env):
        version_id = env.mod.pinned_skill_version_id(
            MappingSession({'version_id': 101}), self._participant(CHAT_PROJECT_ID), 3, CHAT_PROJECT_ID, None,
        )
        assert version_id == 101

    def test_switch_to_another_version_of_the_same_skill(self, env):
        version_id = env.mod.pinned_skill_version_id(
            MappingSession({'version_id': 101}), self._participant(CHAT_PROJECT_ID), 3, CHAT_PROJECT_ID, 100,
        )
        assert version_id == 100

    def test_version_of_another_skill_is_rejected(self, env):
        with pytest.raises(env.mod.SkillParticipantError, match='not found'):
            env.mod.pinned_skill_version_id(
                MappingSession({}), self._participant(CHAT_PROJECT_ID), 3, CHAT_PROJECT_ID, 300,
            )

    def test_catalog_draft_version_is_rejected(self, env):
        with pytest.raises(env.mod.SkillParticipantError, match='not published'):
            env.mod.pinned_skill_version_id(
                MappingSession({}), self._participant(PUBLIC_PROJECT_ID), 3, CHAT_PROJECT_ID, 201,
            )

    def test_foreign_project_participant_is_rejected(self, env):
        with pytest.raises(env.mod.SkillParticipantError, match=env.mod.FOREIGN_SKILL_ERROR):
            env.mod.pinned_skill_version_id(
                MappingSession({}), self._participant(FOREIGN_PROJECT_ID), 3, CHAT_PROJECT_ID, 300,
            )


class ParticipantRowsSession:
    def __init__(self, rows):
        self.rows = rows

    def query(self, *models):
        return types.SimpleNamespace(
            join=lambda *a: types.SimpleNamespace(filter=lambda *b: types.SimpleNamespace(all=lambda: self.rows)),
        )


def _row(participant_id, project_id, version_id):
    participant = types.SimpleNamespace(id=participant_id, entity_meta={'id': 10, 'project_id': project_id})
    return participant, {'version_id': version_id}


class TestMentionCandidates:
    def test_candidates_are_the_other_skill_participants_pinned_versions(self, env):
        session = ParticipantRowsSession([
            _row(55, CHAT_PROJECT_ID, 100),
            _row(56, CHAT_PROJECT_ID, 101),
            _row(57, PUBLIC_PROJECT_ID, 200),
        ])
        msg_group = types.SimpleNamespace(conversation_id=3, sent_to_id=55)
        candidates = env.mod.participant_skill_mention_candidates(session, msg_group, CHAT_PROJECT_ID)
        assert [(c['skill_version_id'], c['instructions']) for c in candidates] == [
            (101, 'Own v2.'), (200, 'Catalog published.'),
        ]

    def test_unavailable_and_foreign_participants_are_skipped(self, env):
        session = ParticipantRowsSession([
            _row(56, PUBLIC_PROJECT_ID, 201),
            _row(57, FOREIGN_PROJECT_ID, 300),
            _row(58, CHAT_PROJECT_ID, 999),
        ])
        msg_group = types.SimpleNamespace(conversation_id=3, sent_to_id=55)
        assert env.mod.participant_skill_mention_candidates(session, msg_group, CHAT_PROJECT_ID) == []

    @pytest.mark.parametrize('user_input, expected', [
        ('use ~Reviewer', True),
        ('no mention', False),
        ([{'type': 'image_url'}, {'type': 'text', 'text': 'ask ~Reviewer'}], True),
        ([{'type': 'text', 'text': 'plain'}], False),
        (None, False),
    ])
    def test_mentions_are_only_looked_up_when_a_tilde_is_typed(self, env, user_input, expected):
        assert env.mod.has_skill_mention(user_input) is expected


def _load(relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _auto_stub(name, **attrs):
    module = types.ModuleType(name)
    module.__path__ = []
    module.__getattr__ = lambda attr: type(attr, (), {'__init__': lambda self, *a, **k: None})
    module.__dict__.update(attrs)
    sys.modules[name] = module
    return module


class AnyColumn:
    __hash__ = object.__hash__

    def __eq__(self, other):
        return True

    def __ne__(self, other):
        return True

    def in_(self, values):
        return True

    def desc(self):
        return self


class ModelMeta(type):
    def __getattr__(cls, name):
        return AnyColumn()


def _model(name):
    return ModelMeta(name, (), {})


class FiredEvents(list):
    def fire_event(self, name, payload):
        self.append((name, payload))


class PassThroughModel:
    @classmethod
    def model_validate(cls, obj, **kwargs):
        return obj


@pytest.fixture
def skill_modules(isolated_sys_modules):
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.pd',
                 f'{PACKAGE}.models.enums', 'pylon', 'pylon.core'):
        _auto_stub(name)
    _auto_stub('pylon.core.tools', log=types.SimpleNamespace(
        info=lambda *a, **k: None, warning=lambda *a, **k: None, debug=lambda *a, **k: None,
        error=lambda *a, **k: None, exception=lambda *a, **k: None,
    ))
    _auto_stub('sqlalchemy', func=None, or_=None, asc=None, desc=None)
    _auto_stub('sqlalchemy.orm', selectinload=lambda *a: None)
    _auto_stub('sqlalchemy.exc', IntegrityError=type('IntegrityError', (Exception,), {}))

    events = FiredEvents()
    sessions = {}

    @contextlib.contextmanager
    def get_session(project_id):
        yield sessions[project_id]

    _auto_stub('tools', db=types.SimpleNamespace(get_session=get_session), serialize=lambda obj, **k: obj,
               context=types.SimpleNamespace(event_manager=events),
               rpc_tools=types.SimpleNamespace(
                   EventManagerMixin=lambda: types.SimpleNamespace(event_manager=events)),
               auth=types.SimpleNamespace(current_user=lambda: {'id': 1}), this=None)
    _auto_stub(f'{PACKAGE}.utils.authors', get_authors_data=lambda ids: [])
    _auto_stub(f'{PACKAGE}.utils.utils', get_public_project_id=lambda: PUBLIC_PROJECT_ID)
    _auto_stub(f'{PACKAGE}.models.pd.skill', SkillDetailModel=PassThroughModel)
    _auto_stub(f'{PACKAGE}.models.pd.skill_version', SkillVersionDetailModel=PassThroughModel)
    _auto_stub(f'{PACKAGE}.models.pd.publish', VERSION_NAME_PATTERN=r'.+')
    _load('models/enums/all.py', f'{PACKAGE}.models.enums.all')
    _load('models/enums/events.py', f'{PACKAGE}.models.enums.events')
    for name in ('skill_category_utils', 'publish_utils', 'constants', 'skill_run_settings', 'like_utils',
                 'folder_access'):
        _auto_stub(f'{PACKAGE}.utils.{name}')
    _auto_stub(f'{PACKAGE}.models.skill', Skill=_model('Skill'), SkillVersion=_model('SkillVersion'),
               EntitySkillMapping=_model('EntitySkillMapping'))
    for name in ('all', 'pd.collection_base', 'pd.skill_publish', 'pd.skill_run_settings'):
        _auto_stub(f'{PACKAGE}.models.{name}')

    skill_utils = _load('utils/skill_utils.py', f'{PACKAGE}.utils.skill_utils')
    skill_publish_utils = _load('utils/skill_publish_utils.py', f'{PACKAGE}.utils.skill_publish_utils')
    return types.SimpleNamespace(
        skill_utils=skill_utils, skill_publish_utils=skill_publish_utils, events=events, sessions=sessions,
    )


CANDIDATE = {'skill_id': 4, 'skill_version_id': 40, 'name': 'Tester', 'version_name': 'base',
             'icon_meta': {}, 'instructions': 'Write tests.'}


class TestMessageSkillConsumption:
    def test_text_mention_resolves_and_keeps_the_name(self, skill_modules):
        cleaned, skills = skill_modules.skill_utils.consume_message_skills('ask ~Tester now', [CANDIDATE])
        assert cleaned == 'ask Tester now'
        assert [s['skill_version_id'] for s in skills] == [40]

    def test_unknown_name_stays_plain_text(self, skill_modules):
        cleaned, skills = skill_modules.skill_utils.consume_message_skills('ask ~Nobody', [CANDIDATE])
        assert (cleaned, skills) == ('ask ~Nobody', [])

    def test_block_input_is_rebuilt_without_touching_the_original(self, skill_modules):
        blocks = [{'type': 'text', 'text': '~Tester'}, {'type': 'image_url', 'image_url': 'x'}]
        rebuilt, skills = skill_modules.skill_utils.consume_message_skills(blocks, [CANDIDATE])
        assert blocks[0]['text'] == '~Tester'
        assert rebuilt[0]['text'] == 'Tester'
        assert [s['name'] for s in skills] == ['Tester']

    def test_no_candidates_means_nothing_is_invoked(self, skill_modules):
        assert skill_modules.skill_utils.consume_message_skills('ask ~Tester', []) == ('ask ~Tester', [])


class FakeSkillRow:
    def __init__(self, name, icon):
        self.id = 10
        self.name = name
        self.description = 'd'
        self.meta = {}
        self.versions = [FakeVersion(100, meta={'icon_meta': icon})]

    def get_default_version(self):
        return self.versions[0]


class RowQuery:
    def __init__(self, row):
        self.row = row

    def filter(self, *a):
        return self

    def options(self, *a):
        return self

    def first(self):
        return self.row


class SkillSession:
    version_model = None

    def __init__(self, skill):
        self.skill = skill

    def query(self, model):
        return RowQuery(self.skill.versions[0] if model is SkillSession.version_model else self.skill)

    def flush(self):
        pass


def _update(skill_modules, skill, **fields):
    update = types.SimpleNamespace(name=None, description=None, meta=None, version=None)
    update.__dict__.update(fields)
    skill_utils = skill_modules.skill_utils
    skill_utils._build_authors_map = lambda skill: {}
    skill_utils.update_skill(CHAT_PROJECT_ID, 10, update, session=SkillSession(skill))


def _skill_events(skill_modules, name):
    return [payload for event, payload in skill_modules.events if event == name]


class TestSkillUpdatedEvent:
    def test_rename_fires_the_new_chip_for_the_owner_project(self, skill_modules):
        _update(skill_modules, FakeSkillRow('Old', {'url': 'a.png'}), name='New')
        assert _skill_events(skill_modules, 'skill_updated') == [
            {'id': 10, 'owner_id': CHAT_PROJECT_ID, 'data': {'name': 'New', 'icon_meta': {'url': 'a.png'}}},
        ]

    def test_description_only_edit_fires_nothing(self, skill_modules):
        _update(skill_modules, FakeSkillRow('Same', {}), description='other')
        assert _skill_events(skill_modules, 'skill_updated') == []

    def test_default_version_icon_change_fires(self, skill_modules):
        skill = FakeSkillRow('Same', {'url': 'a.png'})
        skill_utils = skill_modules.skill_utils
        SkillSession.version_model = skill_utils.SkillVersion
        skill_utils._ensure_version_updatable = lambda version: None
        skill_utils._ensure_version_renamable = lambda *a: None
        skill_utils.update_skill_version(
            CHAT_PROJECT_ID, 10, 100,
            types.SimpleNamespace(name=None, instructions=None, meta={'icon_meta': {'url': 'b.png'}}, tags=None,
                                  model_fields_set=set()),
            session=SkillSession(skill),
        )
        assert _skill_events(skill_modules, 'skill_updated')[0]['data']['icon_meta'] == {'url': 'b.png'}


class TestSkillDeletedEvent:
    def test_payload_names_the_owner_project(self, skill_modules):
        skill = FakeSkillRow('Gone', {})

        class DeleteSession:
            def query(self, model):
                return types.SimpleNamespace(
                    filter=lambda *a: types.SimpleNamespace(first=lambda: None if model is not skill_modules.skill_utils.Skill else skill,
                                                            delete=lambda **k: 0),
                )

            def delete(self, row):
                pass

            def flush(self):
                pass

        skill_modules.skill_utils.delete_skill(CHAT_PROJECT_ID, 10, session=DeleteSession())
        assert _skill_events(skill_modules, 'skill_deleted') == [
            {'id': 10, 'name': 'Gone', 'project_id': CHAT_PROJECT_ID, 'owner_id': CHAT_PROJECT_ID},
        ]


class PublicSession:
    def __init__(self, version, skill, remaining):
        self.version, self.skill, self.remaining = version, skill, remaining
        self.deleted = []

    def query(self, model):
        session = self

        class Query:
            def get(self, row_id):
                return session.version if row_id == session.version.id else session.skill

            def filter(self, *a):
                return self

            def order_by(self, *a):
                return self

            def all(self):
                return session.remaining

            def first(self):
                return session.remaining[0] if session.remaining else None

        return Query()

    def delete(self, row):
        self.deleted.append(row)

    def flush(self):
        pass

    def commit(self):
        pass


class TestSkillUnpublishedEvent:
    def _unpublish(self, skill_modules, remaining, shared_owner_id=CHAT_PROJECT_ID):
        version = FakeVersion(200, status='published')
        version.skill_id = 10
        skill = types.SimpleNamespace(id=10, shared_owner_id=shared_owner_id, meta={'default_version_id': 200})
        skill_modules.sessions[PUBLIC_PROJECT_ID] = PublicSession(version, skill, remaining)
        skill_modules.skill_publish_utils.delete_public_skill_version(PUBLIC_PROJECT_ID, 200)
        return _skill_events(skill_modules, 'skill_unpublished')

    def test_last_version_reports_the_shell_deleted_for_the_public_project(self, skill_modules):
        assert self._unpublish(skill_modules, remaining=[]) == [
            {'id': 10, 'version_id': 200, 'shell_deleted': True, 'catalog_emptied': True,
             'owner_id': PUBLIC_PROJECT_ID},
        ]

    def test_remaining_versions_keep_the_shell(self, skill_modules):
        events = self._unpublish(skill_modules, remaining=[FakeVersion(199, status='published')])
        assert (events[0]['shell_deleted'], events[0]['catalog_emptied']) == (False, False)

    def test_skill_authored_in_the_catalog_reports_its_last_published_version_gone(self, skill_modules):
        events = self._unpublish(skill_modules, remaining=[], shared_owner_id=None)
        assert (events[0]['shell_deleted'], events[0]['catalog_emptied']) == (False, True)


ALL_PROJECTS = [1, 2, 3]


@pytest.fixture
def handlers(isolated_sys_modules):
    for name in ('plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.enums',
                 f'{PACKAGE}.events', 'pylon', 'pylon.core'):
        _auto_stub(name)

    class Web:
        def __getattr__(self, name):
            return lambda *a, **k: (lambda func: func)

    _auto_stub('pylon.core.tools', web=Web(), log=types.SimpleNamespace(
        info=lambda *a, **k: None, exception=lambda *a, **k: None))
    calls = []
    _auto_stub(f'{PACKAGE}.utils.utils', get_public_project_id=lambda: PUBLIC_PROJECT_ID)
    _auto_stub(f'{PACKAGE}.utils.participant_utils',
               update_participant_meta=lambda *a, **k: calls.append(('meta', a, k)))
    _auto_stub(f'{PACKAGE}.utils.chat_template_utils',
               delete_entity_from_templates=lambda *a: calls.append(('template_delete', a)),
               update_entity_name_in_templates=lambda *a: calls.append(('template_rename', a)))
    _load('models/enums/all.py', f'{PACKAGE}.models.enums.all')
    _load('models/enums/events.py', f'{PACKAGE}.models.enums.events')
    participant_events = _load('events/participant.py', f'{PACKAGE}.events.participant').Event()
    template_events = _load('events/chat_template.py', f'{PACKAGE}.events.chat_template').Event()
    participant_events.delete_entity_in_all_conversations = lambda *a: calls.append(('participant_delete', a))
    context = types.SimpleNamespace(rpc_manager=types.SimpleNamespace(call=types.SimpleNamespace(
        project_list=lambda filter_: [{'id': project_id} for project_id in ALL_PROJECTS],
    )))
    return types.SimpleNamespace(participant=participant_events, template=template_events, calls=calls,
                                 context=context)


HANDLERS = {
    'skill_deleted': ('delete_skill_participant_handler', 'on_skill_deleted'),
    'skill_unpublished': ('unpublish_skill_participant_handler', 'on_skill_unpublished'),
    'skill_updated': ('update_skill_participant_handler', 'on_skill_updated'),
}


def _fire(handlers, name, payload):
    participant_handler, template_handler = HANDLERS[name]
    getattr(handlers.participant, participant_handler)(handlers.context, name, payload)
    getattr(handlers.template, template_handler)(handlers.context, name, payload)


class TestLifecycleHandlers:
    def test_deleted_own_skill_becomes_a_dummy_in_the_owner_project_only(self, handlers):
        _fire(handlers, 'skill_deleted', {'id': 5, 'name': 'x', 'project_id': 2, 'owner_id': 2})
        assert ('participant_delete', (2, 'skill', {'project_id': 2, 'id': 5})) in handlers.calls
        assert ('template_delete', (2, ['skill'], 5, 2)) in handlers.calls
        assert len(handlers.calls) == 2

    def test_removed_catalog_shell_is_cleaned_in_every_project(self, handlers):
        _fire(handlers, 'skill_unpublished', {'id': 5, 'version_id': 50, 'shell_deleted': True,
                                              'catalog_emptied': True, 'owner_id': PUBLIC_PROJECT_ID})
        deleted = [call[1][0] for call in handlers.calls if call[0] == 'participant_delete']
        assert deleted == ALL_PROJECTS
        assert all(call[1][2] == {'project_id': PUBLIC_PROJECT_ID, 'id': 5}
                   for call in handlers.calls if call[0] == 'participant_delete')

    def test_unpublishing_one_of_several_versions_removes_nobody(self, handlers):
        _fire(handlers, 'skill_unpublished', {'id': 5, 'version_id': 50, 'shell_deleted': False,
                                              'catalog_emptied': False, 'owner_id': PUBLIC_PROJECT_ID})
        assert handlers.calls == []

    def test_catalog_authored_skill_without_published_versions_is_cleaned_everywhere(self, handlers):
        _fire(handlers, 'skill_unpublished', {'id': 5, 'version_id': 50, 'shell_deleted': False,
                                              'catalog_emptied': True, 'owner_id': PUBLIC_PROJECT_ID})
        assert [call[1][0] for call in handlers.calls if call[0] == 'participant_delete'] == ALL_PROJECTS
        assert [call[1][0] for call in handlers.calls if call[0] == 'template_delete'] == ALL_PROJECTS

    def test_catalog_rename_updates_chips_and_templates_everywhere(self, handlers):
        _fire(handlers, 'skill_updated', {'id': 5, 'owner_id': PUBLIC_PROJECT_ID,
                                          'data': {'name': 'New', 'icon_meta': {}}})
        meta_projects = [call[1][0] for call in handlers.calls if call[0] == 'meta']
        renamed_projects = [call[1][0] for call in handlers.calls if call[0] == 'template_rename']
        assert meta_projects == ALL_PROJECTS
        assert renamed_projects == ALL_PROJECTS
        assert handlers.calls[0][2]['entity_meta'] == {'project_id': PUBLIC_PROJECT_ID, 'id': 5}


def _load_predict_payload_harness():
    spec = importlib.util.spec_from_file_location(
        'predict_payload_harness_6923', pathlib.Path(__file__).with_name('test_auto_routing_predict_payload.py'),
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_predict_harness = _load_predict_payload_harness()
builder = _predict_harness.builder


class TestSkillParticipantMentionsInPredict:
    def test_skill_participant_resolves_mentions_against_the_other_skill_participants(self, builder):
        build, llm, rpc = builder
        consumed = []
        build.__globals__['consume_message_skills'] = (
            lambda content, candidates: consumed.append(candidates) or ('ask Tester', [CANDIDATE])
        )
        request = _predict_harness.parsed(llm, agent=True, auto=False)
        request.user_input = 'ask ~Tester'
        request.version_details['mention_skills'] = [CANDIDATE]
        result = build(request, 2, skip_expansion=True)
        assert consumed == [[CANDIDATE]]
        assert result['user_input'] == 'ask Tester'
        assert result['invoked_skills'] == [CANDIDATE]
        assert 'attached_skills' not in result

    def test_attached_agent_skills_come_before_mention_candidates(self, builder):
        build, llm, rpc = builder
        consumed = []
        build.__globals__['consume_message_skills'] = lambda content, candidates: consumed.append(candidates) or (content, [])
        build.__globals__['log'] = types.SimpleNamespace(debug=lambda *a, **k: None)
        request = _predict_harness.parsed(llm, agent=True, auto=False)
        attached = [{**CANDIDATE, 'name': 'Attached'}]
        request.version_details.update(skills=attached, mention_skills=[CANDIDATE])
        build(request, 2, skip_expansion=True)
        assert consumed == [[*attached, CANDIDATE]]
