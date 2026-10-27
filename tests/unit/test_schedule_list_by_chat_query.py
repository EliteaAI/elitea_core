"""The schedule list query filtered by the chat a schedule delivers to, on SQLite.

The query and the schedule table are real. The plugin package around them is stubbed.

Run via:
    python tests/run_tests.py unit/test_schedule_list_by_chat_query.py -v
"""
import types
import uuid

import pytest
from fixtures import schedule_sqlite as sqlite_db
from unit.test_7011_schedule_yaml_import import load_plugin_module, sandbox

CHAT = uuid.uuid4()
OTHER_CHAT = uuid.uuid4()
ADMIN = types.SimpleNamespace(user_id=1, is_admin=True)
OWNER = types.SimpleNamespace(user_id=7, is_admin=False)


class Env:
    def seed(self, created_by=7, **fields):
        return sqlite_db.seed_schedule(self.session_factory, self.EntitySchedule, created_by, **fields)

    def names(self, scope=ADMIN, **filters):
        values = {"entity_type": None, "status": None, "enabled": None, "owner_ids": None, "entity_id": None,
                  "version_id": None, "search": None, "sort_by": "next_run", "sort_order": "asc"}
        with self.session_factory() as session:
            total, rows = self.module.list_schedules(
                session, scope, types.SimpleNamespace(**{**values, **filters}), 20, 0)
        assert total == len(rows)
        return sorted(row.name for row in rows)


@pytest.fixture
def env(monkeypatch):
    env = Env()
    with sandbox(monkeypatch, env) as (Base, engine):
        env.EntitySchedule = sqlite_db.create_schedule_table(load_plugin_module, Base, engine)
        env.module = load_plugin_module('utils/schedule_queries.py', 'utils.schedule_queries')
        sqlite_db.add_agent_and_pipeline(env.session_factory, env.Application, env.ApplicationVersion)
        yield env


def test_list_keeps_only_schedules_that_deliver_to_the_chat(env):
    env.seed(name="here", delivery_conversation_uuid=CHAT)
    env.seed(name="elsewhere", delivery_conversation_uuid=OTHER_CHAT)
    env.seed(name="no-chat")
    assert env.names(delivery_conversation_uuid=CHAT) == ["here"]


def test_chat_filter_combines_with_other_filters_by_and(env):
    env.seed(name="agent", delivery_conversation_uuid=CHAT, application_id=1, version_id=10)
    env.seed(name="pipeline", delivery_conversation_uuid=CHAT, entity_type="pipeline",
             application_id=2, version_id=11)
    assert env.names(delivery_conversation_uuid=CHAT, entity_id=2) == ["pipeline"]
    assert env.names(delivery_conversation_uuid=CHAT, entity_id=1, status="failed") == []


def test_regular_user_gets_only_own_schedules_of_the_chat(env):
    env.seed(name="mine", created_by=7, delivery_conversation_uuid=CHAT)
    env.seed(name="theirs", created_by=8, delivery_conversation_uuid=CHAT)
    assert env.names(OWNER, delivery_conversation_uuid=CHAT) == ["mine"]
    assert env.names(OWNER, delivery_conversation_uuid=CHAT, owner_ids=[8]) == ["mine"]


def test_admin_gets_every_owner_schedule_of_the_chat(env):
    env.seed(name="mine", created_by=7, delivery_conversation_uuid=CHAT)
    env.seed(name="theirs", created_by=8, delivery_conversation_uuid=CHAT)
    assert env.names(delivery_conversation_uuid=CHAT) == ["mine", "theirs"]


def test_list_without_the_chat_filter_returns_every_row(env):
    env.seed(name="here", delivery_conversation_uuid=CHAT)
    env.seed(name="no-chat")
    assert env.names(delivery_conversation_uuid=None) == ["here", "no-chat"]
    assert env.names() == ["here", "no-chat"]


def test_chat_without_schedules_gives_an_empty_page(env):
    env.seed(name="here", delivery_conversation_uuid=CHAT)
    assert env.names(delivery_conversation_uuid=uuid.uuid4()) == []
