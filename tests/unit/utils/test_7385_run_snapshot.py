"""EL-7385 B1: the live text of a running chat answer is kept for a client that joins mid-run.

Contract under test: utils/run_snapshot.py. The append script runs on fakeredis with Lua, so the real
script is exercised: per-step accumulation, the shared seq when two replicas process the same frame,
the size cap, and the reads and drops the stream / room handlers use.
"""
import importlib.util
import json
import pathlib
import sys
import types

import pytest

fakeredis = pytest.importorskip("fakeredis")
pytest.importorskip("lupa")

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PACKAGE = "el7385_snapshot_pkg"


def _load():
    for name in (PACKAGE, f"{PACKAGE}.utils"):
        pkg = types.ModuleType(name)
        pkg.__path__ = []
        sys.modules[name] = pkg
    modules = {}
    for leaf in ("chat_constants", "run_snapshot"):
        dotted = f"{PACKAGE}.utils.{leaf}"
        spec = importlib.util.spec_from_file_location(dotted, PLUGIN_ROOT / "utils" / f"{leaf}.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[dotted] = module
        spec.loader.exec_module(module)
        modules[leaf] = module
    return modules["run_snapshot"], modules["chat_constants"]


snap, constants = _load()

MESSAGE = "11111111-1111-1111-1111-111111111111"


def chunk(content="", thinking="", run="run-a", created_at="2026-11-09T10:00:00.000001+00:00"):
    return {
        "type": "agent_llm_chunk",
        "message_id": MESSAGE,
        "content": content,
        "thinking": thinking,
        "created_at": created_at,
        "response_metadata": {"tool_run_id": run, "metadata": {}},
    }


@pytest.fixture
def redis():
    return fakeredis.FakeRedis(decode_responses=True)


class TestAppend:
    @staticmethod
    def test_deltas_accumulate_per_llm_step(redis):
        assert snap.append_chunk(redis, MESSAGE, chunk("Hel", created_at="t1")) == 1
        assert snap.append_chunk(redis, MESSAGE, chunk("lo", thinking="hmm", created_at="t2")) == 2
        assert snap.append_chunk(redis, MESSAGE, chunk("Other", run="run-b", created_at="t3")) == 1

        kept = snap.read_snapshot(redis, MESSAGE)
        assert kept["run-a"] == {"content": "Hello", "thinking": "hmm", "seq": 2, "truncated": False, "metadata": {}}
        assert kept["run-b"]["content"] == "Other"

    @staticmethod
    def test_a_frame_seen_by_two_replicas_is_appended_once_with_one_seq(redis):
        frame = chunk("abc", created_at="t1")
        first = snap.append_chunk(redis, MESSAGE, frame)
        second = snap.append_chunk(redis, MESSAGE, dict(frame))
        assert first == second == 1
        assert snap.read_snapshot(redis, MESSAGE)["run-a"]["content"] == "abc"

    @staticmethod
    def test_two_frames_with_the_same_text_are_both_kept(redis):
        # A model can repeat a token; only created_at tells the frames apart
        snap.append_chunk(redis, MESSAGE, chunk("ha", created_at="t1"))
        snap.append_chunk(redis, MESSAGE, chunk("ha", created_at="t2"))
        assert snap.read_snapshot(redis, MESSAGE)["run-a"]["content"] == "haha"

    @staticmethod
    def test_unicode_text_round_trips(redis):
        snap.append_chunk(redis, MESSAGE, chunk("Привіт 👋", created_at="t1"))
        assert snap.read_snapshot(redis, MESSAGE)["run-a"]["content"] == "Привіт 👋"

    @staticmethod
    def test_snapshot_and_frame_marker_expire(redis):
        snap.append_chunk(redis, MESSAGE, chunk("x", created_at="t1"))
        assert 0 < redis.ttl(snap.snapshot_key(MESSAGE)) <= constants.RUN_SNAPSHOT_TTL
        markers = redis.keys(f"{snap.FRAME_KEY_PREFIX}{MESSAGE}:*")
        assert len(markers) == 1
        assert 0 < redis.ttl(markers[0]) <= constants.RUN_SNAPSHOT_FRAME_TTL

    @staticmethod
    def test_past_the_cap_text_stops_growing_and_is_marked(redis, monkeypatch):
        monkeypatch.setattr(snap, "RUN_SNAPSHOT_MAX_CHARS", 5)
        snap.append_chunk(redis, MESSAGE, chunk("abcd", created_at="t1"))
        seq = snap.append_chunk(redis, MESSAGE, chunk("efgh", created_at="t2"))
        kept = snap.read_snapshot(redis, MESSAGE)["run-a"]
        assert kept["content"] == "abcd"
        assert kept["truncated"] is True
        # seq still moves: the client must not apply the dropped chunk on top of the snapshot twice
        assert seq == 2 and kept["seq"] == 2

    @staticmethod
    @pytest.mark.parametrize("frame", [
        chunk(""),
        {**chunk("x"), "response_metadata": {}},
        {**chunk("x"), "response_metadata": None},
    ])
    def test_frames_with_nothing_to_keep_are_ignored(redis, frame):
        assert snap.append_chunk(redis, MESSAGE, frame) is None
        assert snap.read_snapshot(redis, MESSAGE) == {}

    @staticmethod
    def test_no_message_id_is_ignored(redis):
        assert snap.append_chunk(redis, "", chunk("x")) is None

    @staticmethod
    def test_block_list_content_is_kept_as_text(redis):
        snap.append_chunk(redis, MESSAGE, {**chunk(), "content": [{"type": "text", "text": "a"}]})
        assert json.loads(snap.read_snapshot(redis, MESSAGE)["run-a"]["content"]) == [{"type": "text", "text": "a"}]


class TestMetadata:
    @staticmethod
    def test_the_first_chunk_places_the_step(redis):
        # A sub-agent step must render under its agent on seed, as live chunks place it
        first = chunk("a", created_at="t1")
        first["response_metadata"]["metadata"] = {"langgraph_node": "researcher", "parent_agent_name": "Lead"}
        later = chunk("b", created_at="t2")
        later["response_metadata"]["metadata"] = {"langgraph_node": "other"}
        snap.append_chunk(redis, MESSAGE, first)
        snap.append_chunk(redis, MESSAGE, later)
        assert snap.read_snapshot(redis, MESSAGE)["run-a"]["metadata"] == {
            "langgraph_node": "researcher", "parent_agent_name": "Lead"}


class TestDrop:
    @staticmethod
    def test_finished_steps_are_dropped_and_the_rest_stays(redis):
        snap.append_chunk(redis, MESSAGE, chunk("a", run="run-a", created_at="t1"))
        snap.append_chunk(redis, MESSAGE, chunk("b", run="run-b", created_at="t2"))
        end = {"type": "agent_llm_end", "response_metadata": {
            "tool_run_id": "run-a", "thinking_steps": [{"tool_run_id": "run-a"}, {"text": "no id"}],
        }}
        assert snap.finished_run_ids(end) == ["run-a"]
        snap.drop_runs(redis, MESSAGE, snap.finished_run_ids(end))
        assert set(snap.read_snapshot(redis, MESSAGE)) == {"run-b"}

    @staticmethod
    def test_drop_snapshot_removes_everything(redis):
        snap.append_chunk(redis, MESSAGE, chunk("a", created_at="t1"))
        snap.drop_snapshot(redis, MESSAGE)
        assert snap.read_snapshot(redis, MESSAGE) == {}

    @staticmethod
    def test_drops_without_ids_are_no_ops(redis):
        snap.drop_runs(redis, MESSAGE, [])
        snap.drop_runs(redis, "", ["run-a"])
        snap.drop_snapshot(redis, "")


class TestFrames:
    @staticmethod
    def test_one_frame_per_streaming_group_with_kept_text(redis):
        snap.append_chunk(redis, MESSAGE, chunk("so far", created_at="t1"))
        frames = snap.snapshot_frames(redis, "conv-1", [(MESSAGE, "question-1"), ("no-text", "question-2")])
        assert frames == [{
            "type": "chat_run_snapshot",
            "stream_id": "conv-1",
            "message_id": MESSAGE,
            "question_id": "question-1",
            "response_metadata": {"tool_runs": {
                "run-a": {"content": "so far", "thinking": "", "seq": 1, "truncated": False, "metadata": {}},
            }},
        }]

    @staticmethod
    def test_a_corrupt_entry_is_skipped(redis):
        redis.hset(snap.snapshot_key(MESSAGE), "bad", "not json")
        snap.append_chunk(redis, MESSAGE, chunk("ok", created_at="t1"))
        assert set(snap.read_snapshot(redis, MESSAGE)) == {"run-a"}


class TestKeys:
    @staticmethod
    def test_keys_never_match_the_canvas_expiry_listener():
        # module.listen_in_memory_event saves a canvas on every expiry in this Redis DB it can parse
        canvas_pattern = __import__("re").compile(r"(?P<is_shadow>shadow:)?canvas:(?P<project_id>\d+)_(?P<canvas_uuid>.+)")
        assert canvas_pattern.match(snap.snapshot_key(MESSAGE)) is None
        assert canvas_pattern.match(snap.frame_key(MESSAGE, "f")) is None

    @staticmethod
    def test_fingerprint_is_stable_and_frame_specific():
        a = snap.frame_fingerprint("run-a", "t1", "x", "")
        assert a == snap.frame_fingerprint("run-a", "t1", "x", "")
        assert a != snap.frame_fingerprint("run-a", "t2", "x", "")
        assert a != snap.frame_fingerprint("run-b", "t1", "x", "")
