"""Pins the envelope `platform_state_response` detects a join timeout by (#6416).

`predict_sio_llm` reports a timed-out blocking call by returning the task id *without* a `result`
key. `draft_llm_utils.platform_state_response` keys on exactly that, so if the shape ever gains a
`result: None` the draft endpoints would silently go back to reporting every timeout as
"LLM returned an empty response" — the defect #6416 was filed for — and nothing else would notice.

Read from source: the surrounding RPC needs the whole pylon runtime to import.
"""
import pathlib
import re

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _blocking_join_block() -> str:
    source = (PLUGIN_ROOT / 'rpc' / 'application.py').read_text()
    start = source.index('def predict_sio_llm(')
    end = source.index('@web.rpc', start)
    body = source[start:end]
    return body[body.index('if is_blocking:'):]


def test_a_completed_join_returns_the_result_under_a_result_key():
    assert re.search(r'return \{"result": result\}', _blocking_join_block())


def test_a_timed_out_join_returns_only_a_task_id():
    block = _blocking_join_block()

    assert re.search(r'return \{"task_id": task_id\}', block)
    assert '"result": None' not in block
