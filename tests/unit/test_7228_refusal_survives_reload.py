"""#7228 follow-up: a governed chat refusal stays on the reply, so a reload shows the same card."""
import ast
import json
import pathlib
import re
from typing import Optional

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
CHAT_ALL = (ROOT / 'rpc' / 'chat_all.py').read_text()
CODE, SENTENCE = 'BINDING_DIGEST_MISMATCH', 'The agent changed after its identity binding was approved; the run was not started.'


def _load(path, names, namespace):
    tree = ast.parse((ROOT / path).read_text())
    body = [
        n for n in tree.body
        if (isinstance(n, ast.FunctionDef) and n.name in names)
        or (isinstance(n, ast.Assign) and getattr(n.targets[0], 'id', '') in names)
    ]
    exec(compile(ast.Module(body, []), path, 'exec'), namespace)  # pylint: disable=W0122
    return namespace


class Item:
    __mapper_args__ = {'polymorphic_identity': 'text'}

    def __init__(self, message_group, item_type, content, order_index):
        self.message_group, self.item_type, self.content, self.order_index = message_group, item_type, content, order_index


class Group:
    def __init__(self, meta=None):
        self.meta, self.is_streaming, self.items = meta, True, []


class Session:
    def __init__(self):
        self.commits = 0

    def add(self, item):
        item.message_group.items.append(item)

    def commit(self):
        self.commits += 1


@pytest.fixture(scope='module')
def admission():
    namespace = {
        're': re, 'Optional': Optional, 'DEVELOPMENT_GRANT_REQUIRED': 'DEVELOPMENT_GRANT_REQUIRED',
        'DEVELOPMENT_LABEL': 'development:',
        'CHAT_GRANT_MESSAGE': 'This credential needs a grant before you can use it in a chat.',
    }
    return _load('utils/admission.py', {'MESSAGES', 'GENERIC_MESSAGE', 'storable_refusal'}, namespace)


@pytest.fixture(scope='module')
def persist():
    namespace = {'json': json, 'flag_modified': lambda *_: None, 'TextMessageItem': Item}
    return _load('utils/message_stream.py', {'persist_run_refusal'}, namespace)['persist_run_refusal']


def test_refused_reply_keeps_the_card_and_one_text_item(admission, persist):
    group, session = Group({'execution_generation': 'g'}), Session()
    persist(session, group, *admission['storable_refusal']({'code': CODE, 'error': SENTENCE}))
    assert (group.is_streaming, group.meta['is_error']) == (False, True)
    assert json.loads(group.meta['error']) == {'code': CODE, 'error': SENTENCE}
    assert group.meta['execution_generation'] == 'g'
    assert [i.content for i in group.items] == [SENTENCE] and session.commits == 1


def test_a_stored_reply_survives_the_next_predict_cleanup(persist):
    # the cleanup deletes closed replies without a text item (chat_all "paused_empty_responses")
    assert 'TextMessageItem.message_group_id == paused_msg.id' in CHAT_ALL
    group = Group()
    persist(Session(), group, CODE, SENTENCE)
    assert len(group.items) == 1 and group.is_streaming is False


def test_stored_text_is_the_table_sentence_never_the_callers_string(admission):
    kept = admission['storable_refusal']({'code': CODE, 'error': 'SELECT * FROM secrets /tmp/x'})
    assert kept == (CODE, SENTENCE, None)


def test_ref_is_kept_only_when_plain(admission):
    store = admission['storable_refusal']
    assert store({'code': CODE, 'error': SENTENCE, 'ref': 'AB-12'})[2] == 'AB-12'
    assert store({'code': CODE, 'error': SENTENCE, 'ref': 'bad ref; drop'})[2] is None


@pytest.mark.parametrize('error', [
    'Conversation is locked', {'error': 'no code'}, {'code': 'INDEX_NOT_FOUND', 'error': 'x'},
    'HITL_VALUE_INVALID: bad value', {'code': 'HITL_VALUE_INVALID', 'error': 'x'},
])
def test_other_refusals_store_nothing(admission, error):
    assert admission['storable_refusal'](error) is None


def test_hook_is_only_on_the_chat_predict_dispatch_not_the_resume_path():
    assert CHAT_ALL.count('persist_run_refusal(') == 1
    hook = CHAT_ALL.index('persist_run_refusal(')
    assert CHAT_ALL.rindex('def ', 0, hook) < CHAT_ALL.index('storable_refusal(refused.error)') < hook
    resume = CHAT_ALL[CHAT_ALL.index('def continue_predict_sio'):]
    assert 'persist_run_refusal' not in resume and 'storable_refusal' not in resume
