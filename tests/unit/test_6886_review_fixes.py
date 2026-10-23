"""#6886 review fixes: the sender's blocked turn is addressable, and retried turns drop stale cards.

Source pins, like test_6886_input_guardrail_wiring.py: the handlers need a live module context.
"""
import pathlib
import re

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _source(relative):
    return (PLUGIN_ROOT / relative).read_text()


def _function(source, name):
    start = source.index(f'def {name}')
    nxt = re.search(r'\n    @web\.method|\n    def ', source[start + 10:])
    return source[start:start + 10 + nxt.start()] if nxt else source[start:]


def test_blocked_event_carries_the_stored_ids_to_the_sender():
    source = _source('rpc/chat_all.py')
    store = _function(source, '_store_blocked_turn')
    emit = source[source.index("'type': 'input_guardrail_blocked'") - 300:][:1200]

    assert "stored_ids['response_message_id'] = str(response_msg.uuid)" in store
    assert "stored_ids['question_item_uuid'] = str(question_item.uuid)" in store
    assert store.rstrip().endswith('return stored_ids')
    assert 'stored_ids = self._store_blocked_turn(' in source
    assert '**stored_ids' in emit


def test_blocked_event_never_carries_the_detected_value():
    source = _source('rpc/chat_all.py')
    store = _function(source, '_store_blocked_turn')

    assert not re.search(r'stored_ids\[[^\]]+\]\s*=\s*[^\n]*(user_input|stored_input|content)', store)


def test_regenerate_drops_the_refusal_stored_on_the_discarded_answer():
    source = _source('api/v2/regenerate.py')
    fresh = source.index('fresh_meta = begin_execution_generation(')

    assert "fresh_meta.pop('input_guardrail', None)" in source[fresh:]
    # the question keeps its own verdict: only the answer group's meta is rebuilt
    assert "reply_msg.meta = {**(reply_msg.meta or {}), 'input_guardrail': verdict}" in source
    assert "reply_msg.meta.pop('input_guardrail'" not in source


def test_regenerate_notifies_the_editor_when_the_edit_was_redacted_or_warned():
    source = _source('api/v2/regenerate.py')
    commit = source.index('msg_group.is_streaming = True')
    notice = source.index("'type': 'input_guardrail_notice'")

    assert commit < notice < source.index('rpc_func = CHAT_PREDICT_MAPPER')
    assert 'to=parsed.sid' in source[notice:notice + 800]
    assert "'question_id': str(reply_msg.uuid)" in source
    assert "verdict.get('action') == 'redact'" in source[notice:notice + 800]


def test_blocked_resume_verdict_is_dropped_when_the_resume_is_accepted():
    source = _source('rpc/chat_all.py')
    resume = source.index("stored_guardrail = (response_msg.meta or {}).get('input_guardrail')")

    assert "stored_guardrail.get('action') == 'block'" in source[resume:resume + 400]
    assert resume < source.index('response_msg.is_streaming = True', resume)
