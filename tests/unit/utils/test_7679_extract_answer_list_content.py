"""EL-7679 AC-9: extract_answer reads list-of-blocks content and skips thinking blocks."""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module


@pytest.fixture(scope='module')
def draft_llm_utils():
    utils_stub = types.ModuleType('utils')
    utils_stub.get_public_project_id = lambda: 1
    return load_utils_module(
        TESTS_DIR.parent / 'utils',
        'draft_llm_utils',
        extra_stubs={'plugins.elitea_core.utils.utils': utils_stub},
    )


def _history(content):
    return {'result': {'chat_history': [
        {'role': 'user', 'content': 'x'},
        {'role': 'assistant', 'content': content},
    ]}}


def test_claude_thinking_shaped_content(draft_llm_utils):
    content = [
        {'type': 'thinking', 'thinking': 'The user wants {an agent}.', 'signature': 'abc'},
        {'type': 'text', 'text': '{"name": "X", '},
        {'type': 'text', 'text': '"instructions": "Y"}'},
    ]

    assert draft_llm_utils.extract_answer(_history(content)) == '{"name": "X", "instructions": "Y"}'


def test_bare_strings_beside_thinking_block(draft_llm_utils):
    content = ['', {'type': 'thinking', 'thinking': '...'}, '{"name": "X"}']

    assert draft_llm_utils.extract_answer(_history(content)) == '{"name": "X"}'


def test_reasoning_blocks_are_skipped(draft_llm_utils):
    content = [{'type': 'reasoning', 'summary': [{'text': 'hmm'}]}, {'type': 'text', 'text': '{"a": 1}'}]

    assert draft_llm_utils.extract_answer(_history(content)) == '{"a": 1}'


def test_string_content_unchanged(draft_llm_utils):
    assert draft_llm_utils.extract_answer(_history('{"a": 1}')) == '{"a": 1}'


def test_list_with_only_thinking_falls_back_to_trace(draft_llm_utils):
    result = _history([{'type': 'thinking', 'thinking': '...'}])
    result['result']['thinking_steps'] = [{'text': '{"from": "trace"}'}]

    assert draft_llm_utils.extract_answer(result) == '{"from": "trace"}'
