"""EL-7385 B2: chat run state reaches the chat lists of the chat's users, and nobody else.

Contract under test: utils/run_status.py. Models and sio_utils are faked: the audience query answers
with the participant rows a test gives it, and the socket records every emit.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PACKAGE = "el7385_status_pkg"


def _module(name, **attrs):
    module = types.ModuleType(name)
    vars(module).update(attrs)
    sys.modules[name] = module
    return module


class Column:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return ("eq", self.name, other)

    __hash__ = object.__hash__


def _load():
    for name in (PACKAGE, f"{PACKAGE}.utils", f"{PACKAGE}.models", f"{PACKAGE}.models.enums"):
        pkg = types.ModuleType(name)
        pkg.__path__ = []
        sys.modules[name] = pkg
    _module("pylon"), _module("pylon.core")
    log = types.SimpleNamespace(**{n: (lambda *a, **k: None) for n in ("info", "warning", "error", "exception", "debug")})
    pylon_tools = sys.modules.get("pylon.core.tools")
    if pylon_tools is None or not hasattr(pylon_tools, "log"):
        _module("pylon.core.tools", log=log)
    _module(f"{PACKAGE}.models.enums.all", ParticipantTypes=types.SimpleNamespace(
        user=types.SimpleNamespace(value="user")))
    participant = types.SimpleNamespace(
        entity_meta=Column("entity_meta"), id=Column("participant.id"), entity_name=Column("entity_name"))
    mapping = types.SimpleNamespace(participant_id=Column("mapping.participant_id"),
                                    conversation_id=Column("mapping.conversation_id"))
    _module(f"{PACKAGE}.models.participants", Participant=participant, ParticipantMapping=mapping)
    _module(f"{PACKAGE}.utils.sio_utils",
            SioEvents=types.SimpleNamespace(chat_run_status=types.SimpleNamespace(value="chat_run_status")),
            get_user_room=lambda user_id: f"room_notifications_{user_id}")
    dotted = f"{PACKAGE}.utils.run_status"
    spec = importlib.util.spec_from_file_location(dotted, PLUGIN_ROOT / "utils" / "run_status.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[dotted] = module
    spec.loader.exec_module(module)
    return module


status = _load()


class Session:
    """Answers the audience query with the given entity_meta rows and records its filters."""

    def __init__(self, metas, fail=False):
        self.metas = metas
        self.fail = fail
        self.filters = []

    def query(self, *_):
        if self.fail:
            raise RuntimeError("db down")
        return self

    def join(self, *_):
        return self

    def filter(self, *conditions):
        self.filters.extend(conditions)
        return self

    def all(self):
        return [(meta,) for meta in self.metas]


class Sio:
    def __init__(self, fail_for=()):
        self.emits = []
        self.fail_for = set(fail_for)

    def emit(self, event, data, room):
        if room in self.fail_for:
            raise RuntimeError("socket gone")
        self.emits.append((event, data, room))


def group(author_id=7, conversation=True):
    conv = types.SimpleNamespace(id=42, uuid="conv-uuid", author_id=author_id) if conversation else None
    return types.SimpleNamespace(uuid="msg-uuid", conversation=conv)


class TestAudience:
    @staticmethod
    def test_author_and_user_participants_only():
        session = Session([{"id": 7}, {"id": "9"}, {"id": None}, None, {}])
        assert status.run_status_audience(session, 42, 7) == {7, 9}
        # Only user participants of this conversation are read
        assert ("eq", "mapping.conversation_id", 42) in session.filters
        assert ("eq", "entity_name", "user") in session.filters

    @staticmethod
    def test_author_alone_when_nobody_else_joined():
        assert status.run_status_audience(Session([]), 42, "7") == {7}


class TestNotice:
    @staticmethod
    def test_each_user_gets_ids_and_status_in_their_own_room():
        sio = Sio()
        notice = status.build_notice(Session([{"id": 9}]), 5, group(), status.RunStatus.running)
        status.send_notice(sio, notice)
        payload = {
            "project_id": 5, "conversation_id": 42, "conversation_uuid": "conv-uuid",
            "message_group_uuid": "msg-uuid", "status": "running",
        }
        assert sio.emits == [
            ("chat_run_status", payload, "room_notifications_7"),
            ("chat_run_status", payload, "room_notifications_9"),
        ]

    @staticmethod
    def test_a_user_outside_the_chat_never_gets_it():
        # AC-18a: a project member who is neither author nor participant
        sio = Sio()
        status.send_notice(sio, status.build_notice(Session([{"id": 9}]), 5, group(), status.RunStatus.finished))
        assert "room_notifications_11" not in {room for _, _, room in sio.emits}

    @staticmethod
    def test_payload_carries_no_content():
        notice = status.build_notice(Session([]), 5, group(), status.RunStatus.failed)
        assert set(notice.payload) == {
            "project_id", "conversation_id", "conversation_uuid", "message_group_uuid", "status",
        }

    @staticmethod
    def test_audience_failure_gives_no_notice_and_no_raise():
        assert status.build_notice(Session([], fail=True), 5, group(), status.RunStatus.running) is None
        status.send_notice(Sio(), None)

    @staticmethod
    def test_group_without_conversation_gives_no_notice():
        assert status.build_notice(Session([]), 5, group(conversation=False), status.RunStatus.running) is None

    @staticmethod
    def test_one_failed_emit_does_not_stop_the_others():
        sio = Sio(fail_for={"room_notifications_7"})
        status.send_notice(sio, status.build_notice(Session([{"id": 9}]), 5, group(), status.RunStatus.stopped))
        assert [room for _, _, room in sio.emits] == ["room_notifications_9"]

    @staticmethod
    @pytest.mark.parametrize("name", ["running", "waiting", "finished", "failed", "stopped"])
    def test_status_values_are_the_wire_strings(name):
        assert getattr(status.RunStatus, name) == name
