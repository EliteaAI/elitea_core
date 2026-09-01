"""Default output-token normalization for EliteaAI/elitea_issues#3562."""

import ast
import importlib.util
import pathlib
from types import SimpleNamespace


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'llm_settings.py'
SPEC = importlib.util.spec_from_file_location('issue_3562_llm_settings', MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
get_default_max_tokens = MODULE.get_default_max_tokens
normalize_runtime_max_tokens = MODULE.normalize_runtime_max_tokens


def test_missing_and_legacy_default_have_no_runtime_custom_cap():
    assert normalize_runtime_max_tokens(None) is None
    assert normalize_runtime_max_tokens(-1) is None


def test_positive_custom_cap_is_preserved():
    assert normalize_runtime_max_tokens(1234) == 1234


def test_legacy_default_accessor_no_longer_returns_magic_numbers():
    assert get_default_max_tokens(False) is None
    assert get_default_max_tokens(True) is None


def test_continuation_error_metadata_is_persisted_without_flattening():
    source = (MODULE_PATH.parent / 'message_stream.py').read_text()
    tree = ast.parse(source)
    selected = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in {'safe_decode_bytes_in_dict', 'update_message_group_meta'}
    ]
    namespace = {
        'ConversationMessageGroup': object,
        'flag_modified': lambda *_args: None,
        'log': SimpleNamespace(warning=lambda *_args: None),
        'sync_trace_steps': lambda *_args: None,
    }
    exec(compile(ast.Module(selected, []), '<message-stream>', 'exec'), namespace)
    continuation_error = {
        'code': 'output_continuation_exhausted',
        'user_message': 'The model response is incomplete.',
        'partial_output': '# Partial response',
        'attempts': 4,
    }
    message_group = SimpleNamespace(id=1, meta={}, conversation=None)

    namespace['update_message_group_meta'](
        message_group,
        {
            'response_metadata': {
                'additional_response_meta': {'continuation_error': continuation_error},
            },
        },
    )

    assert message_group.meta['continuation_error'] == continuation_error
