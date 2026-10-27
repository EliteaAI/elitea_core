"""The schedule list handler reads and validates the `delivery_conversation_uuid` option.

It reuses the fixture of the list handler test: the real handler and models, a fake list query.

Run via:
    python tests/run_tests.py unit/test_schedule_list_by_chat_api.py -v
"""
import uuid

from unit import test_7011_schedule_list_api as list_api_tests
from unit.test_7011_schedule_list_api import call, make_row, ok

# Pytest finds a fixture by its module-level name; an assignment keeps the shared fixture without an unused import.
env = list_api_tests.env

CHAT = uuid.uuid4()


def chat_of(env):
    return getattr(env.query_calls[0]["filters"], "delivery_conversation_uuid", None)


def test_valid_chat_uuid_reaches_the_query_as_a_uuid(env):
    env.grant(5, "list")
    ok(env, delivery_conversation_uuid=CHAT)
    assert chat_of(env) == CHAT


def test_chat_uuid_combines_with_other_filters(env):
    env.grant(9, "list", "admin")
    ok(env, delivery_conversation_uuid=CHAT, entity_id=12, status="failed")
    filters = env.query_calls[0]["filters"]
    assert (filters.delivery_conversation_uuid, filters.entity_id, filters.status) == (CHAT, 12, "failed")


def test_regular_caller_scope_stays_own_only_with_chat_filter(env):
    env.grant(5, "list")
    env.rows = [make_row(1, 5), make_row(2, 6)]
    body = ok(env, delivery_conversation_uuid=CHAT)
    assert {r["owner"]["id"] for r in body["rows"]} == {5}
    assert env.query_calls[0]["scope"].is_admin is False


def test_admin_scope_is_kept_with_chat_filter(env):
    env.grant(9, "list", "admin")
    env.rows = [make_row(1, 5), make_row(2, 6)]
    body = ok(env, delivery_conversation_uuid=CHAT)
    assert body["total"] == 2
    assert env.query_calls[0]["scope"].is_admin is True


def test_list_without_chat_option_passes_no_chat_filter(env):
    env.grant(5, "list")
    ok(env)
    assert chat_of(env) is None


def test_invalid_chat_uuid_gives_400_naming_the_option(env):
    env.grant(5, "list")
    body, status = call(env, delivery_conversation_uuid="not-a-uuid")
    assert status == 400
    assert body["field"] == "delivery_conversation_uuid"
    assert "delivery_conversation_uuid" in body["error"]
    assert env.query_calls == []


def test_empty_chat_value_gives_400(env):
    env.grant(5, "list")
    body, status = call(env, delivery_conversation_uuid="")
    assert status == 400 and body["field"] == "delivery_conversation_uuid"


def test_chat_without_schedules_gives_empty_list(env):
    env.grant(5, "list")
    body = ok(env, delivery_conversation_uuid=CHAT)
    assert body["rows"] == [] and body["total"] == 0
