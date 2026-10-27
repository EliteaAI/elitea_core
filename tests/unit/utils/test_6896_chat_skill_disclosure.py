"""#6896 - chat skill participants are offered to agent and model replies for on-demand loading."""
import ast
import importlib.util
import pathlib
import types

import pytest

from fixtures.helpers import load_module_with_stubs, load_utils_module

PACKAGE = 'plugins.elitea_core'
PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _load_sibling(file_name, module_name):
    spec = importlib.util.spec_from_file_location(module_name, pathlib.Path(__file__).with_name(file_name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_backend = _load_sibling('test_6923_skill_participant_backend.py', 'skill_backend_harness_6896')
_predict = _load_sibling('test_auto_routing_predict_payload.py', 'predict_payload_harness_6896')
CHAT_PROJECT_ID = _backend.CHAT_PROJECT_ID
PUBLIC_PROJECT_ID = _backend.PUBLIC_PROJECT_ID
ParticipantRowsSession = _backend.ParticipantRowsSession
participant_row = _backend._row
env = _backend.env
builder = _predict.builder


@pytest.fixture
def mentions(isolated_sys_modules, models_path, utils_path):
    enums = load_module_with_stubs(models_path / 'enums/all.py', 'test_6896_enums')
    return load_utils_module(utils_path, 'skill_mentions', extra_stubs={f'{PACKAGE}.models.enums.all': enums})


def _skill(name, skill_id=1, **extra):
    return {'skill_id': skill_id, 'name': name, 'description': f'Use for {name}.', 'instructions': 'Body.', **extra}


class TestMergeDisclosableSkills:
    def test_chat_skills_follow_the_agents_own_skills(self, mentions):
        own = [_skill('reviewer', instruction_referenced=True)]
        merged = mentions.merge_disclosable_skills(own, [_skill('ui-review', 4)])
        assert [s['name'] for s in merged] == ['reviewer', 'ui-review']
        assert merged[0]['instruction_referenced'] is True
        assert merged[1]['instruction_referenced'] is False

    def test_the_agents_own_skill_wins_a_name_clash(self, mentions):
        own = [_skill('Reviewer', 9)]
        merged = mentions.merge_disclosable_skills(own, [_skill(' reviewer ', 4)])
        assert [s['skill_id'] for s in merged] == [9]

    def test_two_chat_skills_with_one_name_are_listed_once(self, mentions):
        merged = mentions.merge_disclosable_skills([], [_skill('writer', 4), _skill('Writer', 5)])
        assert [s['skill_id'] for s in merged] == [4]

    def test_a_nameless_chat_skill_is_not_listed(self, mentions):
        assert mentions.merge_disclosable_skills([], [_skill('', 4)]) == []


def _recipient(entity_name):
    return types.SimpleNamespace(id=55, entity_name=entity_name)


class CountingRowsSession(ParticipantRowsSession):
    def __init__(self, rows):
        super().__init__(rows)
        self.ordered_by = []

    def query(self, *models):
        return types.SimpleNamespace(join=lambda *a: types.SimpleNamespace(filter=lambda *b: types.SimpleNamespace(
            order_by=lambda *columns: self.ordered_by.extend(columns) or types.SimpleNamespace(all=lambda: self.rows),
        )))


def _apply(env, entity_name, user_input='analyze this screenshot', version_details=None):
    session = ParticipantRowsSession([participant_row(56, CHAT_PROJECT_ID, 101), participant_row(57, PUBLIC_PROJECT_ID, 200)])
    msg_group = types.SimpleNamespace(conversation_id=3, sent_to_id=55, sent_to=_recipient(entity_name))
    result = {'user_input': user_input}
    if version_details is not None:
        result['version_details'] = version_details
    env.mod.apply_participant_skill_mentions(session, msg_group, result, CHAT_PROJECT_ID)
    return result


def _pins(chat_skills):
    return [(s['skill_project_id'], s['skill_id'], s['skill_version_id']) for s in chat_skills]


def _agent_skills(count):
    return [_skill(f'agent-skill-{i}', 900 + i) for i in range(1, count + 1)]


class TestAgentMessageSkillLimit:
    @pytest.fixture
    def checked(self, env, monkeypatch):
        calls = []
        monkeypatch.setattr(env.mod, 'validate_message_skill_limit',
                            lambda content, candidates: calls.append((content, candidates)))
        return calls

    def test_an_agent_message_is_checked_against_its_own_and_the_chat_skills(self, env, checked):
        own = _agent_skills(2)
        result = _apply(env, 'application', user_input='~agent-skill-1 do it',
                        version_details={'agent_type': 'openai', 'skills': own})
        [(content, candidates)] = checked
        assert content == '~agent-skill-1 do it'
        assert candidates[:2] == own
        assert candidates[2:] == result[env.mod.CHAT_SKILLS_KEY]
        assert env.mod.INVOKED_SKILLS_KEY not in result

    def test_an_agent_without_chat_skills_is_still_checked(self, env, checked):
        session = ParticipantRowsSession([])
        msg_group = types.SimpleNamespace(conversation_id=3, sent_to_id=55, sent_to=_recipient('application'))
        own = _agent_skills(6)
        result = {'user_input': '~agent-skill-1', 'version_details': {'agent_type': 'openai', 'skills': own}}
        env.mod.apply_participant_skill_mentions(session, msg_group, result, CHAT_PROJECT_ID)
        assert checked == [('~agent-skill-1', own)]

    def test_the_limit_error_reaches_the_caller(self, env, monkeypatch):
        class SkillInvocationLimitError(Exception):
            pass

        def over_the_limit(content, candidates):
            raise SkillInvocationLimitError('This message references 6 skills')

        monkeypatch.setattr(env.mod, 'validate_message_skill_limit', over_the_limit)
        with pytest.raises(SkillInvocationLimitError):
            _apply(env, 'application', user_input='~agent-skill-1',
                   version_details={'agent_type': 'openai', 'skills': _agent_skills(6)})

    @pytest.mark.parametrize('entity_name,user_input,version_details', [
        ('application', 'no mention here', {'agent_type': 'openai', 'skills': _agent_skills(6)}),
        ('application', '~agent-skill-1', {'agent_type': 'pipeline', 'skills': _agent_skills(6)}),
        ('dummy', '~agent-skill-1', None),
    ])
    def test_messages_the_predict_never_resolves_are_not_checked(
        self, env, checked, entity_name, user_input, version_details,
    ):
        _apply(env, entity_name, user_input=user_input, version_details=version_details)
        assert checked == []


class TestTurnSkillLimitBeforeTheTurnIsStored:
    @pytest.fixture
    def checked(self, env, monkeypatch):
        calls = []
        monkeypatch.setattr(env.mod, 'validate_message_skill_limit',
                            lambda content, candidates: calls.append((content, candidates)))
        return calls

    def _validate(self, env, entity_name, user_input='~agent-skill-1', version_details=None):
        session = ParticipantRowsSession([participant_row(56, CHAT_PROJECT_ID, 101), participant_row(57, PUBLIC_PROJECT_ID, 200)])
        env.mod.validate_turn_skill_limit(
            session, 3, _recipient(entity_name), user_input, CHAT_PROJECT_ID, lambda: version_details,
        )

    def test_an_agent_turn_is_checked_against_the_same_candidates_as_the_predict(self, env, checked):
        own = _agent_skills(2)
        self._validate(env, 'application', version_details={'agent_type': 'openai', 'skills': own})
        [(content, candidates)] = checked
        assert content == '~agent-skill-1'
        assert candidates[:2] == own
        assert _pins(candidates[2:]) == [(CHAT_PROJECT_ID, 10, 101), (PUBLIC_PROJECT_ID, 10, 200)]

    @pytest.mark.parametrize('entity_name', ['dummy', 'skill'])
    def test_a_model_or_skill_turn_is_checked_against_the_chat_skills(self, env, checked, entity_name):
        self._validate(env, entity_name)
        [(_content, candidates)] = checked
        assert _pins(candidates) == [(CHAT_PROJECT_ID, 10, 101), (PUBLIC_PROJECT_ID, 10, 200)]

    @pytest.mark.parametrize('entity_name,user_input,version_details', [
        ('application', 'no mention here', {'agent_type': 'openai', 'skills': _agent_skills(6)}),
        ('application', '~agent-skill-1', {'agent_type': 'pipeline', 'skills': _agent_skills(6)}),
        ('application', '~agent-skill-1', None),
    ])
    def test_turns_the_predict_never_resolves_are_not_checked(
        self, env, checked, entity_name, user_input, version_details,
    ):
        self._validate(env, entity_name, user_input=user_input, version_details=version_details)
        assert checked == []

    def test_no_target_participant_is_not_checked(self, env, checked):
        env.mod.validate_turn_skill_limit(ParticipantRowsSession([]), 3, None, '~a', CHAT_PROJECT_ID, lambda: None)
        assert checked == []

    @pytest.mark.parametrize('user_input', ['plain question', '', None])
    def test_a_turn_without_a_mention_never_reads_the_agent_version(self, env, checked, user_input):
        reads = []
        env.mod.validate_turn_skill_limit(
            ParticipantRowsSession([]), 3, _recipient('application'), user_input, CHAT_PROJECT_ID,
            lambda: reads.append('version') or {'agent_type': 'openai', 'skills': []},
        )
        assert reads == [] and checked == []

    def test_a_turn_with_a_mention_reads_the_agent_version_once(self, env, checked):
        reads = []
        self_session = ParticipantRowsSession([])
        env.mod.validate_turn_skill_limit(
            self_session, 3, _recipient('application'), '~agent-skill-1', CHAT_PROJECT_ID,
            lambda: reads.append('version') or {'agent_type': 'openai', 'skills': _agent_skills(1)},
        )
        assert reads == ['version'] and len(checked) == 1


class TestChatSkillsReachTheResponder:
    @pytest.mark.parametrize('entity_name', ['application', 'skill'])
    def test_agents_and_skills_get_the_chat_skills_without_a_tilde(self, env, entity_name):
        version_details = {'agent_type': 'openai'}
        result = _apply(env, entity_name, version_details=version_details)
        assert _pins(result[env.mod.CHAT_SKILLS_KEY]) == [(CHAT_PROJECT_ID, 10, 101), (PUBLIC_PROJECT_ID, 10, 200)]
        assert version_details == {'agent_type': 'openai'}

    def test_chat_skills_carry_their_description_for_the_registry(self, env):
        env.schemas[CHAT_PROJECT_ID][0].description = 'Use when reviewing UI screenshots.'
        result = _apply(env, 'application', version_details={'agent_type': 'openai'})
        assert result[env.mod.CHAT_SKILLS_KEY][0]['description'] == 'Use when reviewing UI screenshots.'

    def test_a_model_chat_gets_the_chat_skills_without_resolving_mentions(self, env):
        result = _apply(env, 'dummy')
        assert [s['skill_version_id'] for s in result[env.mod.CHAT_SKILLS_KEY]] == [101, 200]
        assert result['user_input'] == 'analyze this screenshot'
        assert env.mod.INVOKED_SKILLS_KEY not in result

    def test_a_model_chat_resolves_tilde_mentions_into_the_internal_key(self, env):
        result = _apply(env, 'dummy', user_input='use ~Reviewer')
        assert [s['skill_version_id'] for s in result[env.mod.INVOKED_SKILLS_KEY]] == [101, 200]
        assert 'invoked_skills' not in result

    def test_a_pipeline_gets_no_chat_skills(self, env):
        result = _apply(env, 'application', version_details={'agent_type': 'pipeline'})
        assert env.mod.CHAT_SKILLS_KEY not in result

    def test_a_chat_without_usable_skill_participants_changes_nothing(self, env):
        session = ParticipantRowsSession([participant_row(56, PUBLIC_PROJECT_ID, 201)])
        msg_group = types.SimpleNamespace(conversation_id=3, sent_to_id=55, sent_to=_recipient('dummy'))
        result = {'user_input': 'hi'}
        env.mod.apply_participant_skill_mentions(session, msg_group, result, CHAT_PROJECT_ID)
        assert result == {'user_input': 'hi'}

    def test_the_internal_keys_leave_the_payload_for_the_rpc_kwargs(self, env):
        payload = {'user_input': 'hi', env.mod.CHAT_SKILLS_KEY: ['chat'], env.mod.INVOKED_SKILLS_KEY: ['invoked']}
        assert env.mod.pop_chat_skills(payload) == {'chat_skills': ['chat'], 'invoked_skills': ['invoked']}
        assert payload == {'user_input': 'hi'}


class TestParticipantLookup:
    def test_one_schema_session_per_owner_project(self, env, monkeypatch):
        opened = []
        sessions = env.mod.db.with_project_schema_session

        def counting_session(project_id):
            opened.append(project_id)
            return sessions(project_id)

        monkeypatch.setattr(env.mod.db, 'with_project_schema_session', counting_session)
        env.schemas[CHAT_PROJECT_ID].append(_backend._harness.FakeSkill(11, 'Writer', [_backend.FakeVersion(110)]))
        session = ParticipantRowsSession([
            participant_row(56, CHAT_PROJECT_ID, 101),
            (types.SimpleNamespace(id=58, entity_meta={'id': 11, 'project_id': CHAT_PROJECT_ID}), {'version_id': 110}),
            participant_row(57, PUBLIC_PROJECT_ID, 200),
        ])
        msg_group = types.SimpleNamespace(conversation_id=3, sent_to_id=55)

        candidates = env.mod.participant_skill_mention_candidates(session, msg_group.conversation_id, msg_group.sent_to_id, CHAT_PROJECT_ID)

        assert sorted(opened) == sorted([CHAT_PROJECT_ID, PUBLIC_PROJECT_ID])
        assert [c['skill_version_id'] for c in candidates] == [101, 110, 200]

    def test_participants_are_read_in_a_stable_order(self, env):
        session = CountingRowsSession([participant_row(56, CHAT_PROJECT_ID, 101)])
        msg_group = types.SimpleNamespace(conversation_id=3, sent_to_id=55)
        env.mod.participant_skill_mention_candidates(session, msg_group.conversation_id, msg_group.sent_to_id, CHAT_PROJECT_ID)
        assert session.ordered_by


class TestPredictPayloadListsChatSkills:
    @pytest.fixture
    def build(self, builder, mentions):
        build, llm, rpc = builder
        build.__globals__['merge_disclosable_skills'] = mentions.merge_disclosable_skills
        build.__globals__['log'] = types.SimpleNamespace(debug=lambda *a, **k: None)
        return build, llm

    def test_agent_registry_has_its_own_skills_and_the_chat_skills(self, build):
        run, llm = build
        own = _skill('reviewer', 9)
        run.__globals__['resolve_runtime_skills'] = lambda version: [own]
        request = _predict.parsed(llm, agent=True, auto=False)
        request.version_details.update(skills=[own])
        result = run(request, 2, skip_expansion=True, chat_skills=[_skill('reviewer', 4), _skill('ui-review', 5)])
        assert [s['skill_id'] for s in result['attached_skills']] == [9, 5]

    def test_an_agent_without_own_skills_lists_the_chat_skills(self, build):
        run, llm = build
        result = run(_predict.parsed(llm, agent=True, auto=False), 2, skip_expansion=True,
                     chat_skills=[_skill('ui-review', 5)])
        assert [s['skill_id'] for s in result['attached_skills']] == [5]

    def test_chat_skills_are_tilde_candidates_for_the_agent(self, build):
        run, llm = build
        consumed = []
        run.__globals__['consume_message_skills'] = lambda content, candidates: consumed.append(candidates) or (content, [])
        chat_skill = _skill('ui-review', 5)
        run(_predict.parsed(llm, agent=True, auto=False), 2, skip_expansion=True, chat_skills=[chat_skill])
        assert consumed == [[chat_skill]]

    def test_a_model_chat_lists_the_chat_skills_from_the_server_lookup(self, build):
        run, llm = build
        result = run(_predict.parsed(llm, agent=False, auto=False), 2, skip_expansion=True,
                     chat_skills=[_skill('ui-review', 5), _skill('UI-review', 6)])
        assert [s['skill_id'] for s in result['attached_skills']] == [5]

    def test_a_model_chat_injects_only_server_resolved_mentions(self, build):
        run, llm = build
        resolved = _skill('ui-review', 5)
        result = run(_predict.parsed(llm, agent=False, auto=False), 2, skip_expansion=True, invoked_skills=[resolved])
        assert result['invoked_skills'] == [resolved]

    def test_an_agent_message_over_the_skill_cap_is_rejected_as_a_payload_error(self, build):
        run, llm = build

        class SkillInvocationLimitError(Exception):
            pass

        def over_the_cap(content, candidates):
            raise SkillInvocationLimitError('This message references 7 skills with ~skill-name. Maximum allowed is 5 per message.')

        run.__globals__['SkillInvocationLimitError'] = SkillInvocationLimitError
        run.__globals__['consume_message_skills'] = over_the_cap
        with pytest.raises(run.__globals__['PredictPayloadError'], match='Maximum allowed is 5 per message'):
            run(_predict.parsed(llm, agent=True, auto=False), 2, skip_expansion=True)

    @pytest.mark.parametrize('agent', [False, True])
    def test_a_request_body_cannot_supply_skills(self, build, agent):
        run, llm = build
        request = _predict.parsed(llm, agent=agent, auto=False)
        request.attached_skills = [_skill('forged', 99)]
        request.invoked_skills = [_skill('forged', 99)]
        if agent:
            request.version_details['chat_skills'] = [_skill('forged', 99)]
        result = run(request, 2, skip_expansion=True)
        assert 'attached_skills' not in result
        assert result['invoked_skills'] == []


def _predict_dispatches(path):
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and any(k.arg == 'routing_projection' and isinstance(k.value, ast.Call)
                                              and getattr(k.value.func, 'attr', None) == 'pop' for k in node.keywords):
            yield node


@pytest.mark.parametrize('relative_path, expected', [('rpc/chat_all.py', 2), ('api/v2/regenerate.py', 1)])
def test_every_predict_dispatch_hands_over_the_chat_skills(relative_path, expected):
    dispatches = list(_predict_dispatches(PLUGIN_ROOT / relative_path))
    assert len(dispatches) == expected
    for call in dispatches:
        assert any(
            k.arg is None and isinstance(k.value, ast.Call) and getattr(k.value.func, 'id', None) == 'pop_chat_skills'
            for k in call.keywords
        )


def test_a_skill_participant_message_over_the_cap_becomes_a_payload_generation_error():
    tree = ast.parse((PLUGIN_ROOT / 'rpc/chat_all.py').read_text())
    guards = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Try) and any(
            isinstance(call, ast.Call) and getattr(call.func, 'id', None) == 'apply_participant_skill_mentions'
            for stmt in node.body for call in ast.walk(stmt)
        )
    ]

    assert len(guards) == 1
    handlers = [h for h in guards[0].handlers if getattr(h.type, 'id', None) == 'SkillInvocationLimitError']
    assert len(handlers) == 1
    raised = handlers[0].body[-1]
    assert isinstance(raised, ast.Raise) and getattr(raised.exc.func, 'id', None) == 'PayloadGenerationError'


def test_a_rejected_chat_payload_stops_the_reply_placeholder_streaming():
    tree = ast.parse((PLUGIN_ROOT / 'rpc/chat_all.py').read_text())
    handlers = [
        handler
        for node in ast.walk(tree) if isinstance(node, ast.Try)
        and any(isinstance(call, ast.Call) and getattr(call.func, 'id', None) == 'generate_payload'
                for stmt in node.body for call in ast.walk(stmt))
        for handler in node.handlers if getattr(handler.type, 'id', None) == 'PayloadGenerationError'
    ]

    assert handlers
    for handler in handlers:
        resets = [
            stmt for stmt in handler.body
            if isinstance(stmt, ast.Assign) and isinstance(stmt.targets[0], ast.Attribute)
            and stmt.targets[0].attr == 'is_streaming' and getattr(stmt.value, 'value', None) is False
        ]
        assert resets, f'handler at line {handler.lineno} leaves the reply streaming'


def _predict_sio_source():
    tree = ast.parse((PLUGIN_ROOT / 'rpc/chat_all.py').read_text())
    return next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == 'predict_sio')


def _calls(node, name):
    return [
        c for c in ast.walk(node)
        if isinstance(c, ast.Call) and (getattr(c.func, 'id', None) == name or getattr(c.func, 'attr', None) == name)
    ]


def test_an_over_limit_turn_is_rejected_before_it_is_stored_broadcast_or_named():
    predict = _predict_sio_source()
    [check] = _calls(predict, 'validate_turn_skill_limit')
    stored = min(c.lineno for c in _calls(predict, 'ConversationMessageGroup'))
    named = min(c.lineno for c in _calls(predict, 'check_and_generate_conversation_name'))
    committed = min(c.lineno for c in _calls(predict, 'commit'))

    assert check.lineno < min(stored, named, committed)
    assert isinstance(check.args[-1], ast.Lambda), 'the agent version must be read only for a turn with a ~mention'


def test_the_over_limit_rejection_sends_the_refusal_and_writes_nothing():
    predict = _predict_sio_source()
    guard = next(
        n for n in ast.walk(predict)
        if isinstance(n, ast.Try) and any(_calls(stmt, 'validate_turn_skill_limit') for stmt in n.body)
    )
    [handler] = [h for h in guard.handlers if getattr(h.type, 'id', None) == 'SkillInvocationLimitError']
    body = ast.Module(handler.body, [])

    [raised] = _calls(body, 'SioValidationError')
    error = next(k.value for k in raised.keywords if k.arg == 'error')
    assert _calls(error, 'refusal')
    assert not _calls(body, 'commit') and not _calls(body, 'emit') and not _calls(body, 'add')


def test_the_chat_payload_error_handlers_send_the_coded_content():
    tree = ast.parse((PLUGIN_ROOT / 'rpc/chat_all.py').read_text())
    handlers = [
        handler
        for node in ast.walk(tree) if isinstance(node, ast.Try)
        and any(_calls(stmt, 'generate_payload') for stmt in node.body)
        for handler in node.handlers if getattr(handler.type, 'id', None) == 'PayloadGenerationError'
    ]
    sent = [
        value for handler in handlers for call in ast.walk(ast.Module(handler.body, []))
        if isinstance(call, ast.Dict) for key, value in zip(call.keys, call.values)
        if getattr(key, 'value', None) == 'content'
    ] + [
        k.value for handler in handlers for call in _calls(ast.Module(handler.body, []), 'SioValidationError')
        for k in call.keywords if k.arg == 'error'
    ]

    assert len(sent) == len(handlers) == 2
    assert all(isinstance(v, ast.Attribute) and v.attr == 'content' for v in sent)


def test_a_participant_limit_error_keeps_its_code():
    tree = ast.parse((PLUGIN_ROOT / 'rpc/chat_all.py').read_text())
    guard = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.Try) and any(_calls(stmt, 'apply_participant_skill_mentions') for stmt in n.body)
    )
    [raised] = _calls(ast.Module(guard.handlers[0].body, []), 'PayloadGenerationError')
    assert any(k.arg == 'code' for k in raised.keywords)
