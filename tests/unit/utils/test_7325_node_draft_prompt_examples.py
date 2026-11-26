"""EL-7325 T10: the shipped node prompts (configurations plugin) agree with the validator here.

Every supported type has a spec key, and the one example in each spec passes ``finalize`` against
a context that has the names the example uses (AC-P2: examples are valid drafts, not just JSON).
Skipped when the configurations plugin is not checked out next to elitea_core.

Run via:
    python tests/run_tests.py unit/utils/test_7325_node_draft_prompt_examples.py -v
"""
import importlib
import importlib.util
import json
import pathlib
import re
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
CFG_PD = PLUGIN_ROOT.parent / 'configurations' / 'models' / 'pd'
sys.path.insert(0, str(PLUGIN_ROOT / 'tests'))

from fixtures.node_draft import load  # noqa: E402  pylint: disable=C0413

pytestmark = pytest.mark.skipif(not CFG_PD.exists(), reason='configurations plugin not checked out')

PKG = 'cfg_7325_examples'
EXAMPLE_MARKER = 'Example output:'


def _load_defaults():
    pkg = types.ModuleType(PKG)
    pkg.__path__ = [str(CFG_PD)]
    sys.modules[PKG] = pkg
    try:
        for name in ('service_prompt_keys', 'service_prompt_defaults'):
            spec = importlib.util.spec_from_file_location(f'{PKG}.{name}', CFG_PD / f'{name}.py')
            mod = importlib.util.module_from_spec(spec)
            sys.modules[f'{PKG}.{name}'] = mod
            spec.loader.exec_module(mod)
        return sys.modules[f'{PKG}.service_prompt_defaults'].SERVICE_PROMPT_DEFAULTS
    finally:
        for name in [n for n in sys.modules if n.startswith(PKG)]:
            del sys.modules[name]


DEFAULTS = _load_defaults() if CFG_PD.exists() else {}
VALIDATE, SCHEMA, MODELS = (load('utils.node_draft.validate'), load('utils.node_draft.schema'),
                           load('models.pd.node_draft'))

# Names the examples refer to: the context a real request would have.
NODE_IDS = {'summarize', 'create_jira_issue', 'END'}
STATE = {'input': 'str', 'messages': 'list', 'issues': 'list', 'summary': 'str', 'severity': 'str',
         'jira_key': 'str', 'report': 'str'}
RESOURCES = {
    'toolkit': {'jira_platform': {'id': 2, 'attached': False, 'needs_login': False,
                                  'tools': {'create_issue': {'issue_json': True}}}},  # elitea_sdk jira create_issue(issue_json)
    'mcp': {'docs_mcp': {'id': 3, 'attached': False, 'needs_login': False, 'tools': {'search': {'query': True}}}},
    'agent': {'Daily Issue Summary Agent': {'id': 12, 'attached': False, 'needs_login': False, 'tools': {},
                                            'entity_type': 'agent'}},
}


def _example(text):
    start = text.index('{', text.index(EXAMPLE_MARKER))
    obj, _ = json.JSONDecoder().raw_decode(text[start:])
    return obj


@pytest.mark.parametrize('node_type', SCHEMA.SUPPORTED_TYPES)
def test_every_supported_type_has_a_spec(node_type):
    assert DEFAULTS.get(f'pipeline_node_spec_{node_type}', '').strip()


@pytest.mark.parametrize('node_type', SCHEMA.SUPPORTED_TYPES)
def test_spec_example_is_a_valid_draft(node_type):
    example = _example(DEFAULTS[f'pipeline_node_spec_{node_type}'])
    target = MODELS.DraftTarget(
        node_id='step', node_type=node_type, mode='build', current={'id': 'step', 'type': node_type}, base=None,
        state=dict(STATE), node_ids=NODE_IDS | {'step'}, resources=RESOURCES,
    )
    result = VALIDATE.finalize(example, target)
    assert result.warnings == [], result.warnings
    assert set(SCHEMA.allowed_keys(node_type)) >= set(example['node'])


def test_edit_core_example_is_a_valid_patch():
    text = DEFAULTS['pipeline_node_edit']
    start = text.index('{', text.index('->'))
    example, _ = json.JSONDecoder().raw_decode(text[start:])
    current = {'id': 'step', 'type': 'llm', 'input': ['issues'], 'output': ['summary'], 'transition': 'END',
               'input_mapping': {'task': {'type': 'fstring', 'value': 'Summarize {issues}.'}}}
    target = MODELS.DraftTarget(node_id='step', node_type='llm', mode='edit', current=current, base=None,
                                  state=dict(STATE), node_ids=NODE_IDS | {'step'}, resources=RESOURCES)
    assert VALIDATE.finalize(example, target).changed_paths == ['input_mapping.task']


def test_spec_keys_match_supported_types():
    """`pipeline_node_spec_generic` is the fallback for a type with no spec, not a node type."""
    spec_types = {re.sub('^pipeline_node_spec_', '', k) for k in DEFAULTS
                  if k.startswith('pipeline_node_spec_') and k != 'pipeline_node_spec_generic'}
    assert spec_types == set(SCHEMA.SUPPORTED_TYPES)


def test_generic_fallback_spec_shipped():
    assert DEFAULTS.get('pipeline_node_spec_generic', '').strip()
