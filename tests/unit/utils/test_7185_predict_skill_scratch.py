"""#7185: every predict payload carries where the run's skill scripts keep their files, as the platform resolved it
from the request's conversation and the run's user, never the request's value as is."""
import importlib.util
import pathlib

import pytest


def _load_sibling(file_name, module_name):
    spec = importlib.util.spec_from_file_location(module_name, pathlib.Path(__file__).with_name(file_name))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_predict = _load_sibling('test_auto_routing_predict_payload.py', 'predict_payload_harness_7185')
builder = _predict.builder
RESOLVED = {'conversation_uuid': '6f1c2b8e-3d4a-4f5b-9c7d-0e1f2a3b4c5d', 'bucket': 'team-files'}


@pytest.mark.parametrize('agent', [False, True])
@pytest.mark.parametrize('conversation_id, stream_id, asked_for', [
    ('conversation-from-request', 'stream-1', 'conversation-from-request'),
    (None, 'stream-1', 'stream-1'),
])
def test_the_payload_carries_the_scratch_the_platform_resolved_for_the_run(builder, agent, conversation_id, stream_id, asked_for):
    build, llm, _rpc = builder
    asked = []
    build.__globals__['skill_scratch_for_run'] = lambda project_id, conversation_uuid, user_id: asked.append(
        (project_id, conversation_uuid, user_id)) or RESOLVED
    request = _predict.parsed(llm, agent)
    request.conversation_id, request.stream_id = conversation_id, stream_id

    payload = build(request, 2, skip_expansion=True)

    assert payload['skill_scratch'] == RESOLVED
    assert asked == [(7, asked_for, 2)]
