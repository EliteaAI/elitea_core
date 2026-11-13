"""#7523: a chat message mentions project members by their project user id, joining them to the chat first.

    python tests/run_tests.py unit/test_7523_mention_project_members.py -v
"""
import ast
import pathlib
import types

import pytest
from fixtures.cut_source import load_cut

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
ACCESS_MODULE = PLUGIN_ROOT / 'utils' / 'conversation_access.py'
SIO_UTILS = PLUGIN_ROOT / 'utils' / 'sio_utils.py'
CHAT_ALL = PLUGIN_ROOT / 'rpc' / 'chat_all.py'
PROJECT_ID, SENDER, OWNER = 5, 1, 2
ANNA, BEN, KATE = 11, 12, 13
NAMES = {SENDER: 'Sam Sender', OWNER: 'Olga Owner', ANNA: 'Anna Admin', BEN: 'Ben Brown', KATE: 'Kate Kim'}
OUTSIDER = 99


class IntegrityError(Exception):
    pass


def _top_level(path, names):
    return [
        node for node in ast.parse(path.read_text()).body
        if getattr(node, 'name', None) in names
        or any(getattr(target, 'id', None) in names for target in getattr(node, 'targets', []))
    ]


@pytest.fixture
def mentions():
    calls = {'members': [], 'names': []}
    state = {'members': [SENDER, OWNER, ANNA, BEN, KATE], 'names_fail': False}

    def get_users_ids_in_project(project_id):
        calls['members'].append(project_id)
        return list(state['members'])

    def list_users(user_ids):
        calls['names'].append(list(user_ids))
        if state['names_fail']:
            raise RuntimeError('auth down')
        return [{'id': uid, 'name': NAMES.get(uid, ''), 'email': f'user{uid}@example.com'} for uid in user_ids]

    rpc = types.SimpleNamespace(timeout=lambda _t: types.SimpleNamespace(
        admin_get_users_ids_in_project=get_users_ids_in_project))
    namespace = load_cut(
        _top_level(ACCESS_MODULE, {'MENTION_REFUSED', 'check_mentions', 'format_refused_mention_names'}),
        {
            'rpc_tools': types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)),
            'auth': types.SimpleNamespace(list_users=list_users),
            'log': types.SimpleNamespace(warning=lambda *a, **k: None),
            'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
        },
        'conversation_access.py',
    )
    return types.SimpleNamespace(check=namespace['check_mentions'], calls=calls, state=state)


def chat(private=False, author_id=OWNER, member_ids=(OWNER,), agents=0):
    users = [types.SimpleNamespace(id=10 + i, entity_name='user', entity_meta={'id': m}) for i, m in enumerate(member_ids)]
    bots = [types.SimpleNamespace(id=90 + i, entity_name='application', entity_meta={'id': ANNA}) for i in range(agents)]
    return types.SimpleNamespace(id=31, author_id=author_id, is_private=private, participants=users + bots)


def test_whole_team_chat_mentions_a_project_member_who_never_opened_it(mentions):
    assert mentions.check(PROJECT_ID, chat(), SENDER, [BEN]) == ([BEN], None)
    assert mentions.calls['members'] == [PROJECT_ID]


def test_whole_team_chat_mentions_everyone_in_the_project(mentions):
    assert mentions.check(PROJECT_ID, chat(), SENDER, [OWNER, ANNA, BEN, KATE]) == ([OWNER, ANNA, BEN, KATE], None)


def test_someone_outside_the_project_is_refused_without_revealing_their_name(mentions):
    to_mention, refusal = mentions.check(PROJECT_ID, chat(), SENDER, [OUTSIDER])

    assert to_mention == []
    assert refusal == {
        'error': "User 99 can't be mentioned: they don't have access to this chat.",
        'rejected_user_ids': [OUTSIDER],
    }
    assert mentions.calls['names'] == []


def test_one_refused_person_stops_every_mention_of_the_message(mentions):
    to_mention, refusal = mentions.check(PROJECT_ID, chat(), SENDER, [BEN, OUTSIDER, KATE])

    assert to_mention == []
    assert refusal['rejected_user_ids'] == [OUTSIDER]


def test_restricted_chat_mentions_only_the_people_the_owner_selected(mentions):
    restricted = chat(private=True, member_ids=(OWNER, SENDER, BEN))

    assert mentions.check(PROJECT_ID, restricted, SENDER, [OWNER, BEN]) == ([OWNER, BEN], None)


def test_restricted_chat_refuses_a_project_member_the_owner_did_not_select_and_names_them(mentions):
    restricted = chat(private=True, member_ids=(OWNER, SENDER, BEN))

    to_mention, refusal = mentions.check(PROJECT_ID, restricted, SENDER, [BEN, ANNA, KATE])

    assert to_mention == []
    assert refusal == {
        'error': "Anna Admin, Kate Kim can't be mentioned: they don't have access to this chat.",
        'rejected_user_ids': [ANNA, KATE],
    }


def test_restricted_chat_lets_its_author_be_mentioned_even_without_a_participant_row(mentions):
    restricted = chat(private=True, author_id=OWNER, member_ids=(SENDER,))

    assert mentions.check(PROJECT_ID, restricted, SENDER, [OWNER]) == ([OWNER], None)


def test_restricted_chat_refuses_a_selected_person_who_left_the_project(mentions):
    mentions.state['members'] = [SENDER, OWNER]
    restricted = chat(private=True, member_ids=(OWNER, SENDER, BEN))

    to_mention, refusal = mentions.check(PROJECT_ID, restricted, SENDER, [BEN])

    assert to_mention == []
    assert refusal['rejected_user_ids'] == [BEN]
    assert refusal['error'].startswith('User 12 ')


def test_whole_team_chat_refuses_a_participant_who_left_the_project(mentions):
    mentions.state['members'] = [SENDER, OWNER]

    _, refusal = mentions.check(PROJECT_ID, chat(member_ids=(OWNER, BEN)), SENDER, [BEN])

    assert refusal['rejected_user_ids'] == [BEN]


def test_participant_ids_stored_as_strings_still_count_as_selected(mentions):
    restricted = chat(private=True, member_ids=(str(OWNER), str(BEN)))

    assert mentions.check(PROJECT_ID, restricted, SENDER, [BEN]) == ([BEN], None)


def test_an_agent_participant_does_not_make_a_person_selected(mentions):
    restricted = chat(private=True, member_ids=(OWNER,), agents=1)

    _, refusal = mentions.check(PROJECT_ID, restricted, SENDER, [ANNA])

    assert refusal['rejected_user_ids'] == [ANNA]


def test_the_senders_own_id_is_ignored(mentions):
    assert mentions.check(PROJECT_ID, chat(), SENDER, [SENDER, BEN]) == ([BEN], None)


def test_mentioning_only_yourself_needs_no_project_lookup(mentions):
    assert mentions.check(PROJECT_ID, chat(), SENDER, [SENDER]) == ([], None)
    assert mentions.calls['members'] == []


@pytest.mark.parametrize('mentioned', [[], None])
def test_no_mentions_needs_no_project_lookup(mentions, mentioned):
    assert mentions.check(PROJECT_ID, chat(), SENDER, mentioned) == ([], None)
    assert mentions.calls['members'] == []


def test_a_person_mentioned_twice_is_mentioned_once(mentions):
    assert mentions.check(PROJECT_ID, chat(), SENDER, [BEN, KATE, BEN]) == ([BEN, KATE], None)


def test_a_project_member_without_a_name_is_shown_by_email(mentions):
    mentions.state['members'].append(77)
    restricted = chat(private=True, member_ids=(OWNER,))

    _, refusal = mentions.check(PROJECT_ID, restricted, SENDER, [77])

    assert refusal['error'] == "user77@example.com can't be mentioned: they don't have access to this chat."


def test_names_that_cannot_be_resolved_fall_back_to_ids(mentions):
    mentions.state['names_fail'] = True
    restricted = chat(private=True, member_ids=(OWNER,))

    _, refusal = mentions.check(PROJECT_ID, restricted, SENDER, [KATE])

    assert refusal['error'] == "User 13 can't be mentioned: they don't have access to this chat."


@pytest.fixture
def refusal_frame():
    return load_cut(_top_level(SIO_UTILS, {'refusal_frame'}), {}, 'sio_utils.py')['refusal_frame']


def test_refusal_frame_carries_the_rejected_ids_beside_the_text(refusal_frame):
    frame = refusal_frame('chat_predict', 'Kate Kim can\'t be mentioned', 'c-1', 'q-1',
                          details={'rejected_user_ids': [KATE]})

    assert frame['content'] == "Kate Kim can't be mentioned"
    assert frame['rejected_user_ids'] == [KATE]


def test_refusal_frame_details_never_replace_the_fields_the_client_reads(refusal_frame):
    frame = refusal_frame('chat_predict', 'text', 'c-1', 'q-1', details={'content': {'x': 1}, 'message_id': 'other'})

    assert (frame['content'], frame['message_id']) == ('text', 'q-1')


def test_refusal_frame_without_details_is_unchanged(refusal_frame):
    assert refusal_frame('chat_predict', 'text', 'c-1', 'q-1') == {
        'event': 'chat_predict', 'content': 'text', 'type': 'error', 'stream_id': 'c-1', 'message_id': 'q-1',
    }


def _chat_all_method(name):
    tree = ast.parse(CHAT_ALL.read_text())
    return next(node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name)


class Column:
    def __getitem__(self, _key):
        return self

    @property
    def astext(self):
        return self

    def cast(self, _type):
        return self

    def in_(self, ids):
        return ('ids', list(ids))

    def __eq__(self, other):
        return ('entity_name', other)


class FakeParticipant:
    entity_name = Column()
    entity_meta = Column()

    def __init__(self, entity_name, entity_meta, meta, id=None):
        self.id, self.entity_name, self.entity_meta, self.meta = id, entity_name, entity_meta, meta


class FakeSession:
    def __init__(self, known=(), failing_commits=0, after_rollback=lambda: None):
        self.known = list(known)
        self.failing_commits = failing_commits
        self.after_rollback = after_rollback
        self.commits = self.rollbacks = 0
        self.added, self.queries = [], []

    def query(self, model):
        assert model is FakeParticipant
        session = self

        class Query:
            def filter(self, *conditions):
                session.queries.append(conditions)
                wanted = {str(uid) for uid in conditions[1][1]}
                return types.SimpleNamespace(all=lambda: [p for p in session.known if str(p.entity_meta['id']) in wanted])

        return Query()

    def add(self, participant):
        self.added.append(participant)

    def commit(self):
        if self.failing_commits:
            self.failing_commits -= 1
            raise IntegrityError('duplicate participant')
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1
        self.after_rollback()


@pytest.fixture
def join():
    emitted, author_lookups = [], []

    def get_authors_data(ids):
        author_lookups.append(list(ids))
        return [{'id': uid, 'name': NAMES.get(uid), 'avatar': f'a{uid}'} for uid in ids]

    def from_orm(participant):
        return types.SimpleNamespace(dict=lambda: {'entity_meta': participant.entity_meta, 'meta': participant.meta})

    method = _chat_all_method('add_mentioned_users_as_participants')
    method.decorator_list = []
    namespace = load_cut(
        [method],
        {
            'Participant': FakeParticipant,
            'Integer': int,
            'ParticipantEntityUser': lambda id: types.SimpleNamespace(dict=lambda: {'id': id}),
            'ParticipantBase': types.SimpleNamespace(from_orm=from_orm),
            'ParticipantTypes': types.SimpleNamespace(user=types.SimpleNamespace(value='user')),
            'get_authors_data': get_authors_data,
            'SioEvents': types.SimpleNamespace(chat_participant_update='chat_participant_update'),
            'IntegrityError': IntegrityError,
            'log': types.SimpleNamespace(warning=lambda *a, **k: None),
        },
        'chat_all.py',
    )
    sio = types.SimpleNamespace(emit=lambda **kwargs: emitted.append(kwargs))
    module = types.SimpleNamespace(context=types.SimpleNamespace(sio=sio))
    return types.SimpleNamespace(
        run=lambda session, conversation, ids: namespace['add_mentioned_users_as_participants'](module, session, conversation, ids, 'room-1'),
        emitted=emitted,
        author_lookups=author_lookups,
    )


def test_newcomers_join_the_chat_and_the_room_hears_about_each(join):
    session, conversation = FakeSession(), chat(member_ids=(OWNER,))

    join.run(session, conversation, [BEN, KATE])

    assert [p.entity_meta['id'] for p in conversation.participants] == [OWNER, BEN, KATE]
    assert [(p.entity_name, p.meta) for p in session.added] == [
        ('user', {'user_name': 'Ben Brown', 'user_avatar': f'a{BEN}'}),
        ('user', {'user_name': 'Kate Kim', 'user_avatar': f'a{KATE}'}),
    ]
    assert session.commits == 1
    assert [(e['event'], e['room'], e['data']['entity_meta']['id'], e['data']['meta']['user_name']) for e in join.emitted] == [
        ('chat_participant_update', 'room-1', BEN, 'Ben Brown'), ('chat_participant_update', 'room-1', KATE, 'Kate Kim'),
    ]


def test_everyone_joins_with_one_lookup_and_one_commit_however_many_people(join):
    people = list(range(1000, 1300))
    session = FakeSession(known=[FakeParticipant('user', {'id': 1000}, {'user_name': 'old'}, id=7)])

    join.run(session, chat(member_ids=(OWNER,)), people)

    assert (len(session.queries), len(join.author_lookups), session.commits) == (1, 1, 1)
    assert session.queries[0][1] == ('ids', people)
    assert len(session.added) == len(people) - 1


def test_a_person_with_a_participant_row_from_another_chat_reuses_it(join):
    known = FakeParticipant('user', {'id': BEN}, {'user_name': 'Ben Brown', 'user_avatar': 'old'}, id=7)
    session, conversation = FakeSession(known=[known]), chat(member_ids=(OWNER,))

    join.run(session, conversation, [BEN])

    assert conversation.participants[-1] is known
    assert session.added == []
    assert known.meta == {'user_name': 'Ben Brown', 'user_avatar': 'old'}


def test_a_reused_participant_row_without_meta_gets_the_name_filled_in(join):
    known = FakeParticipant('user', {'id': BEN}, {}, id=7)

    join.run(FakeSession(known=[known]), chat(member_ids=(OWNER,)), [BEN])

    assert known.meta == {'user_name': 'Ben Brown', 'user_avatar': f'a{BEN}'}


def test_people_already_in_the_chat_are_not_joined_again(join):
    session, conversation = FakeSession(), chat(member_ids=(OWNER, str(BEN)))

    join.run(session, conversation, [OWNER, BEN])

    assert len(conversation.participants) == 2
    assert (session.queries, session.added, session.commits, join.emitted) == ([], [], 0, [])


def test_an_agent_with_the_same_id_does_not_count_as_the_person_being_in_the_chat(join):
    session, conversation = FakeSession(), chat(member_ids=(OWNER,), agents=1)

    join.run(session, conversation, [ANNA])

    assert [p.entity_meta['id'] for p in session.added] == [ANNA]


def test_a_conflict_with_another_message_rolls_back_reports_false_and_announces_nobody(join):
    session = FakeSession(failing_commits=1)

    assert join.run(session, chat(member_ids=(OWNER,)), [BEN]) is False

    assert (session.rollbacks, session.commits) == (1, 0)
    assert join.emitted == []


def test_a_successful_add_and_nothing_to_add_both_report_true(join):
    conversation = chat(member_ids=(OWNER,))

    assert join.run(FakeSession(), conversation, [BEN]) is True
    assert join.run(FakeSession(), conversation, [BEN]) is True


def test_the_second_call_after_a_conflict_adds_only_the_people_still_missing(join):
    conversation = chat(member_ids=(OWNER,))
    ben_added_by_the_other_message = types.SimpleNamespace(id=7, entity_name='user', entity_meta={'id': BEN})

    def reload_committed_participants():
        conversation.participants = [p for p in conversation.participants if p.entity_meta['id'] == OWNER]
        conversation.participants.append(ben_added_by_the_other_message)

    session = FakeSession(failing_commits=1, after_rollback=reload_committed_participants)

    assert join.run(session, conversation, [BEN, KATE]) is False
    assert join.run(session, conversation, [BEN, KATE]) is True

    assert [p.entity_meta['id'] for p in conversation.participants] == [OWNER, BEN, KATE]
    assert [e['data']['entity_meta']['id'] for e in join.emitted] == [KATE]


def test_predict_sio_retries_the_add_exactly_once_when_it_reports_a_conflict():
    retry = next(
        node for node in ast.walk(_predict_sio())
        if isinstance(node, ast.If) and 'add_mentioned_users_as_participants' in ast.unparse(node.test)
    )
    first_try = ast.unparse(retry.test)
    assert first_try == 'not self.add_mentioned_users_as_participants(session, conversation, mentioned_user_ids, room)'
    assert [ast.unparse(statement) for statement in retry.body] == [first_try.removeprefix('not ')]
    assert retry.orelse == []


def test_joining_by_mention_sends_no_added_to_chat_notification():
    called = {
        node.func.id for node in ast.walk(_chat_all_method('add_mentioned_users_as_participants'))
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    assert 'notify_user_added_to_conversation' not in called
    assert 'add_participant_to_conversation' not in called
    assert 'get_or_create_one' not in called


def _predict_sio():
    return _chat_all_method('predict_sio')


def _first_line_calling(func, name):
    return min(
        node.lineno for node in ast.walk(func)
        if isinstance(node, ast.Call) and getattr(node.func, 'id', getattr(node.func, 'attr', None)) == name
    )


def test_mentions_are_checked_before_anything_is_written():
    predict_sio = _predict_sio()
    checked = _first_line_calling(predict_sio, 'check_mentions')

    assert checked > _first_line_calling(predict_sio, 'check_post_access')
    for write in ('get_or_create_one', 'add_participant_to_conversation', 'commit', 'add_mentioned_users_as_participants'):
        assert checked < _first_line_calling(predict_sio, write), write


def test_a_refused_mention_reaches_the_socket_with_the_rejected_ids():
    raises = [
        node.exc for node in ast.walk(_predict_sio())
        if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call)
        and any(kw.arg == 'details' for kw in node.exc.keywords)
    ]
    assert len(raises) == 1
    details = next(kw.value for kw in raises[0].keywords if kw.arg == 'details')
    assert ast.unparse(details) == "{'rejected_user_ids': mention_refusal['rejected_user_ids']}"


def test_mentioned_people_join_only_after_the_message_is_stored():
    predict_sio = _predict_sio()
    stored = min(
        node.lineno for node in ast.walk(predict_sio)
        if isinstance(node, ast.Call) and getattr(node.func, 'attr', None) == 'refresh'
        and ast.unparse(node.args[0]) == 'msg_group'
    )
    assert stored < _first_line_calling(predict_sio, 'add_mentioned_users_as_participants')


def test_a_message_mentioning_people_gets_no_ai_reply_by_default():
    guard = next(
        node for node in ast.walk(_predict_sio())
        if isinstance(node, ast.If) and 'dummy' in ast.unparse(node.body[0])
    )
    assert ast.unparse(guard.test) == (
        'parsed.participant_id is None and (not parsed.user_ids) and (not mentioned_user_ids)'
    )
