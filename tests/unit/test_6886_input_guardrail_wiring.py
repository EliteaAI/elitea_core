"""#6886: every entry point that dispatches or stores user text is wired to the guardrail.

The helpers are tested in unit/utils/test_6886_input_guardrail.py; these pin the call
sites, so a new handler that catches broadly before the refusal (and turns a 422 into
a 500) or a checkpoint dropped in a refactor fails here.
"""
import pathlib
import re

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _source(relative):
    return (PLUGIN_ROOT / relative).read_text()


def _handler_order(source, handler):
    """Index of ``handler`` among the except clauses of the try it sits in."""
    at = source.index(handler)
    indent = re.search(r'\n([ \t]*)except', source[at - 200:at + len(handler)]).group(1)
    try_at = source.rindex(f'\n{indent}try:', 0, at)
    clauses = re.findall(rf'\n{indent}except ([^\n:]*)', source[try_at:at + len(handler)])
    return clauses


@pytest.mark.parametrize('relative', [
    'api/v2/predict.py',
    'api/v2/continue_predict.py',
    'api/v2/predict_llm.py',
    'api/v2/pipeline_run.py',
    'api/v2/messages.py',
    'api/v2/webhook.py',
    'api/v2/inject.py',
    'utils/skill_run_utils.py',
])
def test_rest_entry_points_answer_422_with_the_value_free_body(relative):
    source = _source(relative)
    handler = 'except InputGuardrailBlockedError as e:'

    assert f'{handler}\n' in source
    assert re.search(r'except InputGuardrailBlockedError as e:\s+return e\.body\(\), 422', source)
    # Nothing broader is caught first in the same try
    assert not any(c.split(' as ')[0].strip() in ('Exception', 'BaseException') for c in _handler_order(source, handler)[:-1])


def test_mcp_tool_call_returns_the_refusal_as_its_result():
    assert re.search(r'except InputGuardrailBlockedError as e:\s+result = e\.body\(\)', _source('utils/mcp_service.py'))


def test_conversation_rpc_returns_the_refusal():
    assert re.search(
        r"except InputGuardrailBlockedError as e:\s+return \{'success': False, \*\*e\.body\(\)\}",
        _source('rpc/chat_conversation.py'),
    )


def test_checkpoint_b_wraps_both_start_task_paths():
    assert _source('module.py').count('apply_input_guardrail(kwargs)') == 2


def test_checkpoint_a_runs_before_the_chat_message_is_stored():
    source = _source('rpc/chat_all.py')
    check = source.index('check_chat_input(')

    assert check < source.index('check_and_generate_conversation_name', check)
    assert "'type': 'input_guardrail_blocked'" in source
    assert "'type': 'input_guardrail_notice'" in source
    assert 'check_resume_input(parsed, parsed.project_id' in source


def test_checkpoint_a_runs_before_an_injection_is_stored_or_forwarded():
    source = _source('api/v2/inject.py')
    check = source.index('check_chat_input(')

    assert check < source.index('TextMessageItem(')
    assert check < source.index("'type': 'inject'")


def test_webhook_history_preview_is_redacted_before_truncation():
    source = _source('rpc/pipeline_webhook.py')

    assert source.index('redact_text(payload_str)') < source.index('[:500]')


def test_tracing_redactor_is_registered():
    assert 'register_value_redactor(redact_text)' in _source('module.py')


def test_blocked_chat_turn_is_stored_redacted():
    source = _source('rpc/chat_all.py')
    store = source.index('def _store_blocked_turn')

    assert 'self._store_blocked_turn(' in source
    assert 'redacted_input(' in source[store:]
    assert "'is_error': True" in source[store:store + 4000]


def test_accepted_history_is_listed_on_both_chat_dispatch_paths():
    assert _source('rpc/chat_all.py').count(
        'payload[INSPECTED_PAYLOAD_KEY] = accepted_history_hashes(chat_history_groups)'
    ) == 2


def test_chat_history_content_is_not_logged():
    assert not re.search(r'log\.\w+\([^)]*chat_history\'\]\s*\}', _source('rpc/chat_all.py'))


def test_regenerate_checks_an_edited_turn_before_storing_it():
    source = _source('api/v2/regenerate.py')
    check = source.index('check_chat_input(')

    assert check < source.index('item.content = content')
    assert re.search(r'except InputGuardrailBlockedError as e:\s+session\.rollback\(\)\s+return e\.body\(\), 422', source)
    assert 'except SioValidationError as e:' in source


def test_conversation_naming_does_not_log_the_input_or_the_name():
    source = _source('methods/conversation_naming.py')

    assert 'user_input[:' not in source
    assert "'{generated_name}'" not in source
