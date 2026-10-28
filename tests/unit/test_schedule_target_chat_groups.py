"""Run helpers that deliver a scheduled run into an existing chat.

Contract under test: utils/pipeline_execution.py, loaded from source with the leaves faked by the
owner-run harness.
"""
import types

import pytest
from unit.utils.test_7011_pipeline_execution_owner import (  # noqa: F401 -- fixture
    AGENT_ID,
    OWNER_ID,
    Record,
    _predict,
    world,
)

CHAT_ID = 40
DELIVERY_PARTICIPANT_ID = 91
SCHEDULE_META = {"schedule_id": 3, "schedule_run_id": 9}


@pytest.fixture
def chat(world):  # noqa: F811
    row = Record(name="Team chat", participants=[])
    row.id, row.uuid = CHAT_ID, "chat-uuid"

    class ChatSession(type(world.execution.db.get_session(1))):
        def query(self, _model):
            return types.SimpleNamespace(get=lambda _id: row if _id == CHAT_ID else None)

    world.execution.db = types.SimpleNamespace(get_session=lambda _project_id: ChatSession(world))
    return row


def _deliver(world, **kwargs):  # noqa: F811
    return world.execution.create_trigger_run_conversation(
        project_id=1, version=world.version, user_id=OWNER_ID,
        trigger_type=world.execution.TriggerType.SCHEDULED, trigger_message="Report on sales",
        conversation_name="unused", extra_meta=SCHEDULE_META,
        target_conversation=CHAT_ID, delivery_participant_id=DELIVERY_PARTICIPANT_ID, **kwargs,
    )


def _groups(world):  # noqa: F811
    return [row for row in world.added if hasattr(row, "conversation")]


def test_a_target_chat_gets_no_new_conversation(world, chat):  # noqa: F811
    _deliver(world)

    assert not any(hasattr(row, "is_private") for row in world.added)
    assert all(group.conversation is chat for group in _groups(world))


def test_the_trigger_group_comes_from_the_owner_and_carries_the_schedule_ids(world, chat):  # noqa: F811
    _deliver(world)

    trigger = _groups(world)[0]
    assert trigger.author_participant.id == OWNER_ID
    assert trigger.sent_to_id == DELIVERY_PARTICIPANT_ID
    assert trigger.meta == {"scheduled_trigger": True, **SCHEDULE_META}


def test_the_answer_group_comes_from_the_participant_and_replies_to_the_trigger(world, chat):  # noqa: F811
    _deliver(world)

    trigger, answer = _groups(world)
    assert answer.author_participant_id == DELIVERY_PARTICIPANT_ID
    assert answer.reply_to_id == trigger.id
    assert answer.is_streaming is True
    assert answer.meta == {"scheduled_run": True, **SCHEDULE_META}


def test_a_target_chat_returns_its_uuid_the_participant_and_the_answer_group_uuid(world, chat):  # noqa: F811
    result = _deliver(world)

    assert result == ("chat-uuid", DELIVERY_PARTICIPANT_ID, _groups(world)[1].uuid)


def test_a_missing_target_chat_raises_before_any_write(world, chat):  # noqa: F811
    with pytest.raises(LookupError):
        world.execution.create_trigger_run_conversation(
            project_id=1, version=world.version, user_id=OWNER_ID, trigger_type="scheduled",
            trigger_message="x", conversation_name="x", target_conversation=999,
            delivery_participant_id=DELIVERY_PARTICIPANT_ID,
        )
    assert world.added == []


def test_without_a_target_the_run_still_gets_its_own_private_chat(world):  # noqa: F811
    world.execution.create_trigger_run_conversation(
        project_id=1, version=world.version, user_id=OWNER_ID, trigger_type="scheduled",
        trigger_message="x", conversation_name="Scheduled run: Daily report",
    )

    conversation = next(row for row in world.added if hasattr(row, "is_private"))
    assert conversation.is_private is True
    assert {p.id for p in conversation.participants} == {OWNER_ID, AGENT_ID}
    assert all(group.meta == {"scheduled_trigger": True} for group in _groups(world)[:1])


def test_the_predict_call_forwards_the_participant_when_given(world):  # noqa: F811
    call = _predict(world, participant_id=DELIVERY_PARTICIPANT_ID, trigger_source="scheduled",
                    is_system_user=False)

    assert call["data"]["participant_id"] == DELIVERY_PARTICIPANT_ID
    assert call["trigger_source"] == "scheduled"
    assert call["non_interactive"] is True


def test_the_predict_call_omits_the_participant_by_default(world):  # noqa: F811
    assert "participant_id" not in _predict(world)["data"]


SECRET = "rpc to 10.0.0.5 failed at /srv/plugins/x.py"


def _failed_run(world, group_meta):  # noqa: F811
    events, logged = [], []
    group = types.SimpleNamespace(meta=group_meta)

    class Groups:
        uuid = "answer-uuid"

    class GroupSession(type(world.execution.db.get_session(1))):
        def query(self, _model):
            return types.SimpleNamespace(filter=lambda *_: types.SimpleNamespace(first=lambda: group))

    world.execution.db = types.SimpleNamespace(get_session=lambda _project_id: GroupSession(world))
    world.execution.ConversationMessageGroup = Groups
    world.execution.log = types.SimpleNamespace(
        error=lambda *args: logged.append(args), exception=lambda *args: logged.append(args))
    world.execution.this = types.SimpleNamespace(module=types.SimpleNamespace(context=types.SimpleNamespace(
        event_manager=types.SimpleNamespace(fire_event=lambda name, payload: events.append(payload)))))
    world.execution.finish_trigger_run_with_error(1, "answer-uuid", RuntimeError(SECRET))
    return events[0], logged


def test_a_failed_run_in_a_target_chat_shows_a_fixed_message_with_a_code(world):  # noqa: F811
    event, _ = _failed_run(world, {"scheduled_run": True, **SCHEDULE_META})

    assert SECRET not in event["content"]
    assert SECRET not in event["response_metadata"]["error"]
    assert event["response_metadata"]["code"] == "run_start_failed"
    assert event["response_metadata"]["is_error"] is True


def test_a_failed_run_in_a_target_chat_logs_the_exception(world):  # noqa: F811
    _, logged = _failed_run(world, {"scheduled_run": True})

    assert any(SECRET in repr(args) for args in logged)


def test_a_failed_run_in_a_private_run_chat_keeps_the_error_text(world):  # noqa: F811
    event, _ = _failed_run(world, {})

    assert event["content"] == f"Pipeline run could not start: {SECRET}"
    assert "code" not in event["response_metadata"]
