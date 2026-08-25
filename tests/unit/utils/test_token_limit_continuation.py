import importlib.util
import pathlib


MODULE_PATH = pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'token_limit_continuation.py'
SPEC = importlib.util.spec_from_file_location('token_limit_continuation', MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
is_token_limit_continuation = MODULE.is_token_limit_continuation
prepare_token_limit_payload = MODULE.prepare_token_limit_payload
trim_continuation_overlap = MODULE.trim_continuation_overlap


def _payload(*, user_input='Write a long answer', reasoning_effort='high'):
    return {
        'user_input': user_input,
        'chat_history': [{'role': 'user', 'content': 'Earlier question'}],
        'llm': {'kwargs': {'reasoning_effort': reasoning_effort}},
        'application': {
            'version_details': {
                'llm_settings': {'reasoning_effort': reasoning_effort},
            },
        },
    }


def test_explicit_token_continuation_supports_zero_visible_content():
    assert is_token_limit_continuation(
        explicitly_requested=True,
        truncated_content='',
        hitl_resume=False,
        mcp_auth_resume=False,
        has_pending_interrupts=False,
    )


def test_legacy_visible_partial_content_remains_supported():
    assert is_token_limit_continuation(
        explicitly_requested=False,
        truncated_content='Partial answer',
        hitl_resume=False,
        mcp_auth_resume=False,
        has_pending_interrupts=False,
    )


def test_checkpoint_resumes_take_precedence_over_output_continuation():
    for resume_fields in (
        {'hitl_resume': True, 'mcp_auth_resume': False, 'has_pending_interrupts': False},
        {'hitl_resume': False, 'mcp_auth_resume': True, 'has_pending_interrupts': False},
        {'hitl_resume': False, 'mcp_auth_resume': False, 'has_pending_interrupts': True},
    ):
        assert not is_token_limit_continuation(
            explicitly_requested=True,
            truncated_content='Partial answer',
            **resume_fields,
        )


def test_overlap_trimming_preserves_valid_numbered_list_boundary():
    existing_tail = '32. item-32\n33. item-33'
    incoming_content = '34. item-34\n35. item-35'

    assert trim_continuation_overlap(existing_tail, incoming_content) == incoming_content


def test_overlap_trimming_removes_repeated_complete_tokens():
    existing_tail = 'The answer ends with a repeated phrase'

    assert trim_continuation_overlap(
        existing_tail,
        'repeated phrase before the missing ending',
    ) == ' before the missing ending'


def test_overlap_trimming_does_not_remove_partial_word_match():
    incoming_content = 'tion before the missing ending'

    assert trim_continuation_overlap('The prior completion', incoming_content) == incoming_content


def test_visible_partial_content_uses_only_visible_tail_as_assistant_prefill():
    payload = _payload()
    visible_content = ('prefix ' * 100) + 'visible cutoff'

    prepare_token_limit_payload(payload, visible_content)

    assert payload['chat_history'][-2] == {
        'role': 'user',
        'content': 'Write a long answer',
    }
    assert payload['chat_history'][-1]['role'] == 'assistant'
    assert payload['chat_history'][-1]['content'] == visible_content[-600:]
    assert 'output only the missing ending' in payload['user_input']
    assert payload['truncated_content'] == visible_content
    assert payload['llm']['kwargs']['reasoning_effort'] == 'low'
    assert payload['application']['version_details']['llm_settings']['reasoning_effort'] == 'low'


def test_reasoning_only_exhaustion_starts_complete_visible_answer_without_prefill():
    payload = _payload()

    prepare_token_limit_payload(payload, '')

    assert payload['chat_history'][-1] == {
        'role': 'user',
        'content': 'Write a long answer',
    }
    assert not any(message['role'] == 'assistant' for message in payload['chat_history'])
    assert 'before producing any user-visible answer' in payload['user_input']
    assert 'Produce the complete answer now' in payload['user_input']
    assert payload['truncated_content'] == ''
    assert payload['llm']['kwargs']['reasoning_effort'] == 'low'
