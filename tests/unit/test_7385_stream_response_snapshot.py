"""EL-7385 B1: stream_response keeps the live text of a chat answer, even when nobody is in the room.

Contract under test: methods/stream.py. Sibling imports are faked; the snapshot helpers are the real
module running on fakeredis, so the stamp the client dedups by is the real seq.
"""
import sys
import types

import pytest

from unit._tool_spend_loader import load

fakeredis = pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

MESSAGE = "m-1"


def _module(name, **attrs):
    module = types.ModuleType(name)
    vars(module).update(attrs)
    return module


class Manager:
    def __init__(self, participants):
        self.participants = participants

    def get_participants(self, _namespace, _room):
        return iter(self.participants)


@pytest.fixture
def make_handler(monkeypatch):
    def make(participants=(), redis_fails=False):
        none = lambda *a, **k: None
        stubs = {
            "models.enums.all": {"IndexDataStatus": types.SimpleNamespace(in_progress="ip", failed="f")},
            "models.pd.index": {"IndexDataRemovedEvent": object},
            "sio.all": {
                "get_event_room": lambda event, room_id: f"room_{event}_{room_id}",
                "SioEvents": types.SimpleNamespace(
                    application_predict="application_predict",
                    chat_predict=types.SimpleNamespace(value="chat_predict")),
            },
            "utils.application_tools": {
                "handle_index_data_failure": none, "ensure_index_data_has_task_id": none,
                "clean_up_schedule_in_toolkit": none},
            "utils.parallel_hitl": {"decision_ack_key": none},
        }
        for dotted, names in stubs.items():
            name = "plugins.elitea_core." + dotted
            monkeypatch.setitem(sys.modules, name, _module(name, **names))
        for name in ("plugins.elitea_core.methods", "plugins.elitea_core.sio",
                     "plugins.elitea_core.models.enums"):
            package = _module(name)
            package.__path__ = []
            monkeypatch.setitem(sys.modules, name, package)
        quiet = types.SimpleNamespace(info=none, debug=none, warning=none, error=none, exception=none)
        monkeypatch.setitem(sys.modules, "pylon.core.tools", _module(
            "pylon.core.tools", web=types.SimpleNamespace(method=lambda *a, **k: (lambda f: f)), log=quiet))
        monkeypatch.setitem(sys.modules, "tools", _module("tools", prettify=str))
        load("utils/chat_constants.py", fresh=True)
        load("utils/run_snapshot.py", fresh=True)
        stream = load("methods/stream.py", fresh=True)

        redis = fakeredis.FakeRedis(decode_responses=True)
        emits = []

        def get_redis_client():
            if redis_fails:
                raise ConnectionError("redis down")
            return redis

        handler = types.SimpleNamespace(
            context=types.SimpleNamespace(
                sio=types.SimpleNamespace(
                    manager=Manager(participants),
                    emit=lambda **kw: emits.append(kw)),
                event_manager=types.SimpleNamespace(fire_event=none)),
            get_redis_client=get_redis_client,
        )
        handler._keep_run_snapshot = lambda payload: stream.Method._keep_run_snapshot(handler, payload)
        handler.stream = lambda payload: stream.Method.stream_response(handler, "chat_predict", payload)
        snapshot = sys.modules["plugins.elitea_core.utils.run_snapshot"]
        return handler, redis, emits, snapshot
    return make


def chunk(content, created_at, run="run-a", sio_event="chat_predict"):
    return {
        "type": "agent_llm_chunk", "stream_id": "conv-1", "message_id": MESSAGE, "sio_event": sio_event,
        "content": content, "thinking": "", "created_at": created_at,
        "response_metadata": {"tool_run_id": run},
    }


def test_text_is_kept_while_nobody_is_in_the_room(make_handler):
    handler, redis, emits, snapshot = make_handler(participants=())
    handler.stream(chunk("Hel", "t1"))
    handler.stream(chunk("lo", "t2"))
    assert emits == []  # the empty-room skip still applies to the emit
    assert snapshot.read_snapshot(redis, MESSAGE)["run-a"]["content"] == "Hello"


def test_the_live_frame_carries_the_seq_of_its_append(make_handler):
    handler, _redis, emits, _snapshot = make_handler(participants=("sid-1",))
    handler.stream(chunk("a", "t1"))
    handler.stream(chunk("b", "t2"))
    assert [e["data"]["response_metadata"]["snapshot_seq"] for e in emits] == [1, 2]


def test_llm_end_drops_the_steps_it_closes(make_handler):
    handler, redis, _emits, snapshot = make_handler()
    handler.stream(chunk("a", "t1", run="run-a"))
    handler.stream(chunk("b", "t2", run="run-b"))
    handler.stream({
        "type": "agent_llm_end", "stream_id": "conv-1", "message_id": MESSAGE, "sio_event": "chat_predict",
        "response_metadata": {"thinking_steps": [{"tool_run_id": "run-a"}]},
    })
    assert set(snapshot.read_snapshot(redis, MESSAGE)) == {"run-b"}


def test_other_streams_are_not_kept(make_handler):
    handler, redis, _emits, snapshot = make_handler()
    stream = sys.modules["plugins.elitea_core.methods.stream"]
    stream.Method.stream_response(handler, "application_predict", chunk("a", "t1", sio_event="application_predict"))
    assert snapshot.read_snapshot(redis, MESSAGE) == {}


def test_a_redis_failure_still_emits_without_a_stamp(make_handler):
    handler, _redis, emits, _snapshot = make_handler(participants=("sid-1",), redis_fails=True)
    handler.stream(chunk("a", "t1"))
    assert len(emits) == 1
    assert "snapshot_seq" not in emits[0]["data"]["response_metadata"]
