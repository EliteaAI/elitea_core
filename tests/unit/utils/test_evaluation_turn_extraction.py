"""Unit tests for evaluation_turn_extraction.py — the EVAL-H7 turn-extraction contract (§8.3).

Pure functions (stdlib only), loaded directly from their path. These lock the verified
discriminator (user vs agent), the output-text rule (text items only, order_index order,
non-text + trace excluded), and the input/output pairing so E2E-06/11 can assert against them.
"""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def extract(utils_path):
    return load_utils_module(utils_path, 'evaluation_turn_extraction')


def _item(item_type, order_index, content):
    return types.SimpleNamespace(item_type=item_type, order_index=order_index, content=content)


# --- classify_role ------------------------------------------------------------

def test_user_is_user(extract):
    assert extract.classify_role('user') == 'user'


@pytest.mark.parametrize('entity', ['application', 'prompt', 'llm', 'toolkit', 'dummy'])
def test_non_user_is_agent(extract, entity):
    assert extract.classify_role(entity) == 'agent'


def test_none_entity_is_agent(extract):
    # a missing/unknown participant is not the user participant -> agent side
    assert extract.classify_role(None) == 'agent'


# --- group_text: which items count, in what order -----------------------------

def test_group_text_single(extract):
    assert extract.group_text([_item('text_message', 0, 'hello')]) == 'hello'


def test_group_text_legacy_text_identity(extract):
    # 'text' is a legacy polymorphic identity for the same chat_messages_text subtable
    assert extract.group_text([_item('text', 0, '[Pipeline execution completed]')]) == \
        '[Pipeline execution completed]'


def test_group_text_orders_by_order_index(extract):
    items = [_item('text_message', 2, 'third'),
             _item('text_message', 0, 'first'),
             _item('text_message', 1, 'second')]
    assert extract.group_text(items, separator=' ') == 'first second third'


def test_group_text_excludes_non_text_items(extract):
    items = [_item('text_message', 0, 'answer'),
             _item('canvas_message', 1, 'def f(): ...'),
             _item('attachment_message', 2, None),
             _item('context_message', 3, {'rag': 'chunk'})]
    assert extract.group_text(items) == 'answer'


def test_group_text_empty_group(extract):
    assert extract.group_text([]) == ''


def test_group_text_skips_blank_content(extract):
    items = [_item('text_message', 0, '  '), _item('text_message', 1, 'real')]
    assert extract.group_text(items) == 'real'


# --- pair_turns: input/output case assembly -----------------------------------

def test_pair_simple_qa(extract):
    turns = [('user', 'what is 2+2?'), ('agent', '4')]
    assert extract.pair_turns(turns) == [('what is 2+2?', '4')]


def test_pair_multiturn(extract):
    turns = [('user', 'q1'), ('agent', 'a1'), ('user', 'q2'), ('agent', 'a2')]
    assert extract.pair_turns(turns) == [('q1', 'a1'), ('q2', 'a2')]


def test_pair_multiple_agent_turns_join(extract):
    turns = [('user', 'q'), ('agent', 'part1'), ('agent', 'part2')]
    assert extract.pair_turns(turns) == [('q', 'part1\n\npart2')]


def test_pair_user_with_no_answer_is_provisional(extract):
    turns = [('user', 'unanswered')]
    assert extract.pair_turns(turns) == [('unanswered', None)]


def test_pair_leading_agent_turn_skipped(extract):
    # a system/agent greeting before any user prompt is not a case
    turns = [('agent', 'greeting'), ('user', 'q'), ('agent', 'a')]
    assert extract.pair_turns(turns) == [('q', 'a')]


def test_pair_empty(extract):
    assert extract.pair_turns([]) == []
