"""#7126 - every project skill reaches a chat turn as a body-less catalog entry; `~name` loads at most the cap."""
import ast
import contextlib
import importlib.util
import pathlib
import sys
import types
from queue import Empty

import pytest

PACKAGE = 'plugins.elitea_core'
PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _load_sibling(file_name, module_name):
    spec = importlib.util.spec_from_file_location(module_name, pathlib.Path(__file__).with_name(file_name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_harness = _load_sibling('test_6922_skill_participant.py', 'skill_participant_harness_7126')
_backend = _load_sibling('test_6923_skill_participant_backend.py', 'skill_backend_harness_7126')
_predict = _load_sibling('test_auto_routing_predict_payload.py', 'predict_payload_harness_7126')
CHAT_PROJECT_ID = _harness.CHAT_PROJECT_ID
PUBLIC_PROJECT_ID = _harness.PUBLIC_PROJECT_ID
ParticipantRowsSession = _backend.ParticipantRowsSession
participant_row = _backend._row
env = _harness.env
builder = _predict.builder
parsed = _predict.parsed


@pytest.fixture
def project_skills(env):
    return sys.modules[f'{PACKAGE}.utils.project_skills']


def _user(user_id):
    return types.SimpleNamespace(entity_name='user', entity_meta={'id': user_id})


def _conversation(is_private=True, user_ids=(5,)):
    return types.SimpleNamespace(
        is_private=is_private,
        participants=[_user(user_id) for user_id in user_ids] + [types.SimpleNamespace(entity_name='dummy', entity_meta={})],
    )


def _row(skill_id, name, version_id, version_name='base', has_body=True, skill_meta=None, description='d', icon=None):
    return (skill_id, name, description, skill_meta or {}, version_id, version_name, {'icon_meta': icon} if icon else {}, has_body)


def _entry(skill_id, name, version_id=None, project_id=CHAT_PROJECT_ID, **extra):
    return {
        'skill_id': skill_id, 'skill_version_id': version_id or 100 + skill_id, 'skill_project_id': project_id,
        'name': name, 'description': f'Use for {name}.', 'version_name': 'base', 'icon_meta': None, 'origin': 'project',
        **extra,
    }


class TestSetting:
    def _enabled(self, project_skills, monkeypatch, record):
        rpc = types.SimpleNamespace(configurations_get_first_filtered_project=lambda project_id, filter_fields: record)
        monkeypatch.setattr(project_skills.rpc_tools, 'RpcMixin', lambda: types.SimpleNamespace(
            rpc=types.SimpleNamespace(timeout=lambda seconds: rpc),
        ))
        return project_skills.is_project_skills_in_chat_enabled(CHAT_PROJECT_ID)

    @pytest.mark.parametrize('record, expected', [
        (None, False),
        ({'data': {'enabled': True}}, True),
        ({'data': {'enabled': False}}, False),
        ({'data': {'enabled': 'yes'}}, False),
        ({'data': None}, False),
    ])
    def test_only_an_explicit_true_turns_the_feature_on(self, project_skills, monkeypatch, record, expected):
        assert self._enabled(project_skills, monkeypatch, record) is expected

    def test_the_record_is_read_by_type(self, project_skills, monkeypatch):
        seen = {}

        def reader(project_id, filter_fields):
            seen.update(project_id=project_id, filter_fields=filter_fields)
            return {'data': {'enabled': True}}

        monkeypatch.setattr(project_skills.rpc_tools, 'RpcMixin', lambda: types.SimpleNamespace(
            rpc=types.SimpleNamespace(timeout=lambda seconds: types.SimpleNamespace(configurations_get_first_filtered_project=reader)),
        ))
        project_skills.is_project_skills_in_chat_enabled(CHAT_PROJECT_ID)
        assert seen == {'project_id': CHAT_PROJECT_ID, 'filter_fields': {'type': 'project_skills'}}

    def test_a_failing_read_means_off(self, project_skills, monkeypatch):
        def failing(project_id, filter_fields):
            raise RuntimeError('configurations down')

        monkeypatch.setattr(project_skills.rpc_tools, 'RpcMixin', lambda: types.SimpleNamespace(
            rpc=types.SimpleNamespace(timeout=lambda seconds: types.SimpleNamespace(configurations_get_first_filtered_project=failing)),
        ))
        assert project_skills.is_project_skills_in_chat_enabled(CHAT_PROJECT_ID) is False


class _Expression:
    def __gt__(self, other):
        return self

    def label(self, name):
        return self


class _Query:
    def __init__(self, platform, columns):
        self.platform, self.columns = platform, columns

    def join(self, *args):
        return self

    def filter(self, condition):
        if len(self.columns) == 2:
            self.platform.body_requests.append(sorted(condition))
        else:
            self.platform.filters.append(condition)
        self.condition = condition
        return self

    def order_by(self, *columns):
        return self

    def all(self):
        if len(self.columns) == 2:
            return [(version_id, f'Body {version_id}.') for version_id in sorted(self.condition)]
        return list(self.platform.rows)


@pytest.fixture
def platform(project_skills, monkeypatch):
    state = types.SimpleNamespace(
        restricted_calls=[], member_calls=0, supported=True, timeouts=set(), errors=set(),
        restricted_folders={5: [50], 6: [60], 7: []}, members=[5, 6, 7, 7],
        rows=[], filters=[], clause_inputs=[], body_requests=[], filter_unavailable=False,
    )

    def restricted(project_id, entity_type, user_id):
        state.restricted_calls.append(user_id)
        if user_id in state.timeouts:
            raise Empty()
        if user_id in state.errors:
            raise RuntimeError('social down')
        return state.restricted_folders.get(user_id, [])

    def supported(project_id):
        if 'supported' in state.timeouts:
            raise Empty()
        return state.supported

    def members(project_id):
        state.member_calls += 1
        return state.members

    def clause(entity_types, id_column, restricted_ids):
        state.clause_inputs.append(list(restricted_ids))
        if not restricted_ids or state.filter_unavailable:
            return None
        return ('exclude', tuple(restricted_ids))

    @contextlib.contextmanager
    def session(project_id):
        yield types.SimpleNamespace(query=lambda *columns: _Query(state, columns))

    rpc = types.SimpleNamespace(
        social_folder_permissions_supported=supported,
        admin_get_users_ids_in_project=members,
        social_get_restricted_folder_ids=restricted,
    )
    monkeypatch.setattr(project_skills.rpc_tools, 'RpcMixin', lambda: types.SimpleNamespace(
        rpc=types.SimpleNamespace(timeout=lambda seconds: rpc),
    ))
    monkeypatch.setattr(project_skills, 'build_folder_ids_exclusion_clause', clause)
    monkeypatch.setattr(project_skills, 'db', types.SimpleNamespace(with_project_schema_session=session))
    monkeypatch.setattr(project_skills, 'func', types.SimpleNamespace(length=lambda value: _Expression(), btrim=lambda value: value))
    return state


def _private_catalog(project_skills, platform, rows):
    platform.rows = rows
    return project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation(True, (5,)))


class TestAccessRule:
    def test_a_private_chat_with_only_the_sender_filters_by_the_sender(self, project_skills, platform):
        project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation(True, (5,)))
        assert platform.restricted_calls == [5]
        assert platform.clause_inputs == [[50]]

    def test_a_private_chat_with_another_reader_filters_for_everyone(self, project_skills, platform):
        project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation(True, (5, 6)))
        assert sorted(platform.restricted_calls) == [5, 6, 7]
        assert platform.clause_inputs == [[50, 60]]

    def test_a_public_chat_filters_for_everyone_even_with_one_member(self, project_skills, platform):
        project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation(False, (5,)))
        assert sorted(platform.restricted_calls) == [5, 6, 7]

    def test_a_sender_stored_as_a_string_id_still_counts_as_the_only_reader(self, project_skills, platform):
        project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation(True, ('5',)))
        assert platform.restricted_calls == [5]

    def test_without_folder_permissions_nobody_is_looked_up(self, project_skills, platform):
        platform.supported = False
        project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation(False, (5,)))
        assert platform.restricted_calls == []
        assert platform.clause_inputs == [[]]

    def test_members_are_enumerated_once_per_catalog_build(self, project_skills, platform):
        project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation(False, (5,)))
        assert platform.member_calls == 1
        assert len(platform.restricted_calls) == 3

    @pytest.mark.parametrize('timeout, user_ids', [
        (6, (5, 6)),
        (5, (5,)),
        ('supported', (5, 6)),
    ], ids=['one member of a shared chat', 'the sender of a private chat', 'the permissions probe'])
    def test_a_social_timeout_offers_nothing_instead_of_dropping_restrictions(self, project_skills, platform, timeout, user_ids):
        platform.timeouts.add(timeout)
        platform.rows = [_row(1, 'restricted-skill', 10)]

        assert project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation(True, user_ids)) == []
        assert platform.clause_inputs == []

    def test_without_the_social_plugin_there_are_no_folders_to_look_up(self, project_skills, platform, monkeypatch):
        monkeypatch.setattr(project_skills.context, 'module_manager', types.SimpleNamespace(modules={}))

        project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation(False, (5, 6)))

        assert platform.restricted_calls == [] and platform.member_calls == 0
        assert platform.clause_inputs == [[]]

    def test_the_support_project_never_offers_project_skills(self, project_skills, platform, monkeypatch):
        monkeypatch.setattr(project_skills, 'get_support_config', lambda: {'project_id': CHAT_PROJECT_ID})
        platform.rows = [_row(1, 'one', 10)]
        assert project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation()) == []
        assert platform.clause_inputs == []

    def test_a_failed_lookup_offers_nothing_instead_of_everything(self, project_skills, platform):
        platform.errors.add(5)
        platform.rows = [_row(1, 'restricted-skill', 10)]
        assert project_skills.build_project_skill_catalog(CHAT_PROJECT_ID, 5, _conversation()) == []
        assert platform.clause_inputs == []


class TestFolderFilter:
    def test_restricted_folders_without_a_folder_filter_offer_nothing(self, project_skills, platform):
        platform.filter_unavailable = True

        assert _private_catalog(project_skills, platform, [_row(1, 'restricted-skill', 10)]) == []
        assert platform.filters == []

    def test_restricted_folders_with_a_folder_filter_apply_it(self, project_skills, platform):
        _private_catalog(project_skills, platform, [_row(1, 'one', 10)])

        assert platform.filters == [('exclude', (50,))]


class TestCatalogEntries:
    def test_the_default_version_comes_from_meta_then_base(self, project_skills, platform):
        rows = [
            _row(1, 'one', 10, 'base', skill_meta={'default_version_id': 11}),
            _row(1, 'one', 11, 'v2', skill_meta={'default_version_id': 11}),
            _row(2, 'two', 20, 'base'), _row(2, 'two', 21, 'v2'),
            _row(3, 'three', 30, 'v2'),
        ]
        entries = _private_catalog(project_skills, platform, rows)
        assert [(e['name'], e['skill_version_id'], e['version_name']) for e in entries] == [('one', 11, 'v2'), ('two', 20, 'base')]

    def test_a_stale_default_pointer_falls_back_to_base(self, project_skills, platform):
        rows = [_row(1, 'one', 10, 'base', skill_meta={'default_version_id': 999})]
        assert _private_catalog(project_skills, platform, rows)[0]['skill_version_id'] == 10

    def test_a_default_without_a_body_is_left_out(self, project_skills, platform):
        rows = [_row(1, 'one', 10, has_body=False), _row(1, 'one', 11, 'v2')]
        assert _private_catalog(project_skills, platform, rows) == []

    def test_duplicate_names_keep_the_lowest_id(self, project_skills, platform):
        rows = [_row(9, 'Dup', 90), _row(4, 'dup', 40), _row(5, ' dup ', 50)]
        assert [e['skill_id'] for e in _private_catalog(project_skills, platform, rows)] == [4]

    def test_entries_carry_identity_and_description_but_no_body(self, project_skills, platform):
        rows = [_row(1, 'one', 10, description='Use for one.', icon={'url': 'x.png'})]
        assert _private_catalog(project_skills, platform, rows) == [{
            'skill_id': 1, 'skill_version_id': 10, 'skill_project_id': CHAT_PROJECT_ID, 'name': 'one',
            'description': 'Use for one.', 'version_name': 'base', 'icon_meta': {'url': 'x.png'}, 'origin': 'project',
        }]


class TestMentions:
    TOKEN_CATALOG = [_entry(i, name) for i, name in enumerate(
        ['release-notes', 'a', 'b', '5min', 'writer', 'bar', 'strike', 'bad'], start=1,
    )]

    @pytest.mark.parametrize('text, expected', [
        ('use ~release-notes now', ['release-notes']),
        ('~A ~b ~a', ['a', 'b']),
        ('foo~bar ~~strike~~ ~-bad ~5min', ['5min']),
        ('x ~ y', []),
        ([{'type': 'image_url'}, {'type': 'text', 'text': 'ask ~Writer'}], ['writer']),
        (None, []),
    ])
    def test_tokens_follow_the_skill_name_grammar(self, project_skills, platform, text, expected):
        mentioned = project_skills.resolve_mentioned_project_skills(text, set(), self.TOKEN_CATALOG, CHAT_PROJECT_ID)
        assert [m['name'] for m in mentioned] == expected

    def test_a_mentioned_project_skill_gets_its_body(self, project_skills, platform):
        catalog = [_entry(1, 'release-notes'), _entry(2, 'sql-review')]
        mentioned = project_skills.resolve_mentioned_project_skills('do ~SQL-Review', set(), catalog, CHAT_PROJECT_ID)
        assert mentioned == [{**catalog[1], 'instructions': 'Body 102.'}]
        assert platform.body_requests == [[102]]

    def test_known_names_are_never_looked_up_in_the_project(self, project_skills, platform):
        catalog = [_entry(1, 'reviewer')]
        assert project_skills.resolve_mentioned_project_skills('~Reviewer', {'Reviewer'}, catalog, CHAT_PROJECT_ID) == []
        assert platform.body_requests == []

    def test_at_most_the_per_turn_cap_is_loaded(self, project_skills, platform):
        catalog = [_entry(i, f's{i}') for i in range(1, 8)]
        text = ' '.join(f'~s{i}' for i in range(1, 8))
        mentioned = project_skills.resolve_mentioned_project_skills(text, set(), catalog, CHAT_PROJECT_ID)
        assert [m['name'] for m in mentioned] == ['s1', 's2', 's3', 's4', 's5']
        assert platform.body_requests == [[101, 102, 103, 104, 105]]

    def test_a_close_unknown_name_is_answered_with_a_suggestion(self, project_skills, platform):
        catalog = [_entry(1, 'release-notes')]
        with pytest.raises(project_skills.UnknownSkillMentionError) as error:
            project_skills.resolve_mentioned_project_skills('~relese-notes please', {'reviewer'}, catalog, CHAT_PROJECT_ID,
                                                            strict=True)
        assert str(error.value) == 'No skill named "~relese-notes". Did you mean ~release-notes?'
        assert platform.body_requests == []

    def test_a_suggestion_may_point_at_a_chat_skill(self, project_skills, platform):
        with pytest.raises(project_skills.UnknownSkillMentionError, match='Did you mean ~reviewer'):
            project_skills.resolve_mentioned_project_skills('~reviewr', {'Reviewer'}, [], CHAT_PROJECT_ID, strict=True)

    def test_a_regenerated_message_keeps_a_close_unknown_name_literal(self, project_skills, platform):
        catalog = [_entry(1, 'release-notes'), _entry(2, 'sql-review')]

        mentioned = project_skills.resolve_mentioned_project_skills(
            '~relese-notes and ~sql-review', {'reviewer'}, catalog, CHAT_PROJECT_ID,
        )

        assert [m['name'] for m in mentioned] == ['sql-review']

    def test_a_token_unlike_any_skill_stays_literal(self, project_skills, platform):
        catalog = [_entry(1, 'release-notes')]
        assert project_skills.resolve_mentioned_project_skills('about ~5 minutes', set(), catalog, CHAT_PROJECT_ID,
                                                               strict=True) == []


def _recipient(entity_name):
    return types.SimpleNamespace(id=55, entity_name=entity_name)


def _msg_group(entity_name, conversation=None):
    return types.SimpleNamespace(
        conversation_id=3, sent_to_id=55, sent_to=_recipient(entity_name),
        author_participant=types.SimpleNamespace(entity_meta={'id': 5}),
        conversation=conversation or _conversation(),
    )


class TestChatTurn:
    @pytest.fixture
    def catalog(self, env, project_skills, platform, monkeypatch):
        state = {'enabled': True, 'catalog': [_entry(1, 'release-notes'), _entry(2, 'sql-review')], 'asked': []}
        monkeypatch.setattr(env.mod, 'is_project_skills_in_chat_enabled', lambda project_id: state['enabled'])

        def catalog_for(project_id, sender_id, conversation):
            state['asked'].append((project_id, sender_id, conversation))
            return list(state['catalog'])

        monkeypatch.setattr(env.mod, 'build_project_skill_catalog', catalog_for)
        return state

    def _apply(self, env, entity_name, user_input='hello', version_details=None, rows=None, msg_group=None, strict=True):
        session = ParticipantRowsSession(rows if rows is not None else [participant_row(56, CHAT_PROJECT_ID, 101)])
        result = {'user_input': user_input}
        if version_details is not None:
            result['version_details'] = version_details
        env.mod.apply_participant_skill_mentions(
            session, msg_group or _msg_group(entity_name), result, CHAT_PROJECT_ID, strict_mentions=strict,
        )
        return result

    def test_a_model_chat_gets_the_catalog_beside_the_participants(self, env, catalog):
        result = self._apply(env, 'dummy')
        assert [s['name'] for s in result[env.mod.PROJECT_SKILLS_KEY]] == ['release-notes', 'sql-review']
        assert all('instructions' not in s for s in result[env.mod.PROJECT_SKILLS_KEY])
        assert [s['skill_version_id'] for s in result[env.mod.CHAT_SKILLS_KEY]] == [101]
        assert env.mod.INVOKED_SKILLS_KEY not in result

    def test_the_catalog_is_resolved_for_the_sender_and_the_conversation(self, env, catalog):
        conversation = _conversation(False, (5, 6))
        self._apply(env, 'dummy', msg_group=_msg_group('dummy', conversation))
        assert catalog['asked'] == [(CHAT_PROJECT_ID, 5, conversation)]

    def test_a_chat_with_no_participants_still_gets_the_catalog(self, env, catalog):
        result = self._apply(env, 'dummy', rows=[])
        assert env.mod.CHAT_SKILLS_KEY not in result
        assert len(result[env.mod.PROJECT_SKILLS_KEY]) == 2

    def test_a_tilde_project_skill_is_invoked_with_its_body_in_a_model_chat(self, env, catalog):
        result = self._apply(env, 'dummy', user_input='write ~release-notes')
        invoked = {s['name']: s for s in result[env.mod.INVOKED_SKILLS_KEY]}
        assert invoked['release-notes']['instructions'] == 'Body 101.'
        assert invoked['release-notes']['skill_project_id'] == CHAT_PROJECT_ID
        assert [s.get('instructions') for s in result[env.mod.PROJECT_SKILLS_KEY]] == ['Body 101.', None]

    def test_an_agent_gets_the_catalog_and_the_mentioned_body_for_predict(self, env, catalog):
        result = self._apply(env, 'application', user_input='~sql-review this', version_details={'agent_type': 'openai', 'skills': []})
        assert [s.get('instructions') for s in result[env.mod.PROJECT_SKILLS_KEY]] == [None, 'Body 102.']
        assert env.mod.INVOKED_SKILLS_KEY not in result

    def test_an_agents_own_skill_name_is_not_resolved_from_the_project(self, env, catalog):
        catalog['catalog'].append(_entry(3, 'tester'))
        version_details = {'agent_type': 'openai', 'skills': [{'name': 'Tester'}]}
        result = self._apply(env, 'application', user_input='~tester', version_details=version_details)
        assert all(s.get('instructions') is None for s in result[env.mod.PROJECT_SKILLS_KEY])

    def test_a_participant_name_shadows_the_project_copy(self, env, catalog):
        catalog['catalog'].append(_entry(3, 'reviewer'))
        result = self._apply(env, 'dummy', user_input='~Reviewer')
        assert all(s.get('instructions') is None for s in result[env.mod.PROJECT_SKILLS_KEY])

    def test_an_unknown_tilde_name_raises_for_the_chat_to_answer(self, env, catalog, project_skills):
        with pytest.raises(project_skills.UnknownSkillMentionError, match='Did you mean ~release-notes'):
            self._apply(env, 'dummy', user_input='~release-note')

    def test_a_mentioned_body_replaces_only_the_entry_with_the_same_project_and_id(self, env, catalog):
        catalog['catalog'] = [_entry(1, 'a'), _entry(2, 'b'), _entry(1, 'a-public', project_id=PUBLIC_PROJECT_ID)]

        result = self._apply(env, 'dummy', user_input='~a')

        assert [s.get('instructions') for s in result[env.mod.PROJECT_SKILLS_KEY]] == ['Body 101.', None, None]

    def test_regenerate_and_continue_keep_an_unknown_tilde_name_literal(self, env, catalog):
        result = self._apply(env, 'dummy', user_input='~release-note please', strict=False)

        assert result['user_input'] == '~release-note please'
        assert all(skill.get('instructions') is None for skill in result[env.mod.PROJECT_SKILLS_KEY])

    @pytest.mark.parametrize('entity_name, version_details', [
        ('skill', {'agent_type': 'openai'}),
        ('application', {'agent_type': 'pipeline'}),
    ])
    def test_skill_recipients_and_pipelines_get_no_project_skills(self, env, catalog, entity_name, version_details):
        result = self._apply(env, entity_name, version_details=version_details)
        assert env.mod.PROJECT_SKILLS_KEY not in result
        assert catalog['asked'] == []

    def test_the_setting_off_means_no_catalog_lookup(self, env, catalog):
        catalog['enabled'] = False
        result = self._apply(env, 'dummy')
        assert env.mod.PROJECT_SKILLS_KEY not in result
        assert catalog['asked'] == []

    def test_the_catalog_leaves_the_payload_as_an_rpc_kwarg(self, env):
        payload = {'user_input': 'hi', env.mod.PROJECT_SKILLS_KEY: ['p']}
        assert env.mod.pop_chat_skills(payload)['project_skills'] == ['p']
        assert payload == {'user_input': 'hi'}


def _without_bodies(skills):
    return [{k: v for k, v in s.items() if k != 'instructions'} for s in skills or []]


class TestPredictPayload:
    @pytest.fixture
    def build(self, builder):
        build, llm, rpc = builder
        build.__globals__['strip_skill_bodies'] = _without_bodies
        return build, llm

    def test_a_model_chat_payload_lists_project_skills_without_bodies(self, build):
        build, llm = build
        project = [_entry(1, 'release-notes', instructions='Body.'), _entry(2, 'sql-review')]
        payload = build(parsed(llm), 2, skip_expansion=True, project_skills=project)
        assert payload['project_skills'] == _without_bodies(project)
        assert 'attached_skills' not in payload

    def test_an_agent_payload_lists_project_skills_and_resolves_their_mentions(self, build):
        build, llm = build
        consumed = {}

        def consume(text, candidates):
            consumed['candidates'] = candidates
            return text, [candidates[-1]]

        build.__globals__['consume_message_skills'] = consume
        project = [_entry(1, 'release-notes', instructions='Body.')]
        payload = build(parsed(llm, agent=True), 2, skip_expansion=True, project_skills=project, chat_skills=[_entry(9, 'chat')])
        assert [c['name'] for c in consumed['candidates']] == ['chat', 'release-notes']
        assert payload['invoked_skills'] == project
        assert payload['applied_skills'][0]['skill_id'] == 1
        assert payload['project_skills'] == _without_bodies(project)

    def test_a_pipeline_payload_carries_no_project_skills(self, build):
        build, llm = build
        pipeline = parsed(llm, agent=True, auto=False)
        pipeline.version_details['agent_type'] = 'pipeline'
        payload = build(pipeline, 2, skip_expansion=True, project_skills=[_entry(1, 'release-notes')])
        assert 'project_skills' not in payload

    def test_without_project_skills_nothing_changes(self, build):
        build, llm = build
        assert 'project_skills' not in build(parsed(llm, agent=True), 2, skip_expansion=True)


def _function(path, name):
    tree = ast.parse((PLUGIN_ROOT / path).read_text())
    return next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name)


class TestPlumbing:
    @pytest.mark.parametrize('rpc_name', ['predict_sio', 'predict_sio_llm'])
    def test_the_predict_rpcs_accept_and_forward_project_skills(self, rpc_name):
        function = _function('rpc/application.py', rpc_name)
        assert 'project_skills' in [arg.arg for arg in function.args.args]
        calls = [
            node for node in ast.walk(function)
            if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'generate_predict_payload'
        ]
        assert calls and all(
            any(kw.arg == 'project_skills' and kw.value.id == 'project_skills' for kw in call.keywords) for call in calls
        )

    def test_only_a_fresh_send_resolves_mentions_strictly(self):
        def strict_calls(path):
            tree = ast.parse((PLUGIN_ROOT / path).read_text())
            return [
                any(kw.arg == 'strict_skill_mentions' and getattr(kw.value, 'value', None) is True for kw in node.keywords)
                for node in ast.walk(tree)
                if isinstance(node, ast.Call) and getattr(node.func, 'id', None) == 'generate_payload'
            ]

        assert sorted(strict_calls('rpc/chat_all.py')) == [False, True]
        assert strict_calls('api/v2/regenerate.py') == [False]
        function = _function('rpc/chat_all.py', 'generate_payload')
        assert 'strict_mentions=strict_skill_mentions' in ast.unparse(function)

    def test_generate_payload_turns_an_unknown_mention_into_a_payload_error(self):
        function = _function('rpc/chat_all.py', 'generate_payload')
        handlers = [
            node for node in ast.walk(function)
            if isinstance(node, ast.ExceptHandler) and getattr(node.type, 'id', None) == 'UnknownSkillMentionError'
        ]
        assert len(handlers) == 1
        raised = handlers[0].body[0]
        assert isinstance(raised, ast.Raise) and raised.exc.func.id == 'PayloadGenerationError'
