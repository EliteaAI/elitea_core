"""#7383: a scheduled run has no one to click the Changes card, so its Builder block is non-interactive."""
import ast
import pathlib
import sys
import types

TESTS_DIR = pathlib.Path(__file__).resolve().parents[1]
PLUGIN_ROOT = TESTS_DIR.parent
sys.path.insert(0, str(TESTS_DIR))

from fixtures.cut_source import load_cut  # noqa: E402

CHAT_ALL = (PLUGIN_ROOT / 'rpc' / 'chat_all.py').read_text()


def _builder_for_run():
    nodes = [node for node in ast.parse(CHAT_ALL).body if getattr(node, 'name', None) == '_builder_for_run']
    return load_cut(nodes, {}, 'chat_all.py')['_builder_for_run']


def test_a_scheduled_run_gets_a_non_interactive_builder():
    conversation = types.SimpleNamespace(meta={'schedule_run_id': 'r1'})
    assert _builder_for_run()({'core_prompt': 'x'}, conversation) == {'core_prompt': 'x', 'interactive': False}


def test_a_chat_keeps_the_builder_block_as_is():
    for conversation in (types.SimpleNamespace(meta={}), types.SimpleNamespace(meta=None), None):
        assert _builder_for_run()({'core_prompt': 'x'}, conversation) == {'core_prompt': 'x'}
