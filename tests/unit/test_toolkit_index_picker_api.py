"""The index picker: the summary list of a toolkit, and the toolkit list filter `has_indexes` (SQLite env, real models)."""
import sys
import types
from contextlib import contextmanager

import pytest
from unit import test_toolkit_index_legacy_api_block as api_block
from unit import test_toolkit_index_schedule_service as service_env
from unit import test_toolkit_index_test_run_delete as run_delete

PACKAGE, env = service_env.PACKAGE, service_env.env
api, tools_env = api_block.api, run_delete.tools_env
BODY = {"cron": "0 3 * * *", "enabled": True, "created_by": 7}


@pytest.mark.parametrize("configuration, expected", [
    ({"chunk_size": 500}, True),
    ('{"chunk_size": 500}', True),
    ({}, False),
    (None, False),
    ("{}", False),
    ("not json", False),
    ("[1, 2]", False),
])
def test_an_index_is_schedulable_only_with_saved_settings(api, configuration, expected):
    assert api.index_meta._has_saved_settings(configuration) is expected  # noqa: SLF001


def set_meta(env, indexes_meta):
    with env.session_factory() as session:
        session.get(sys.modules[f"{PACKAGE}.models.all"].EliteATool, 3).meta = {"indexes_meta": indexes_meta}
        session.commit()


def test_scheduled_names_come_from_the_rows_and_the_old_settings_key(api):
    api.seed_index("from_row")
    set_meta(api, {"from_key": {"schedules": {"7": BODY}}, "no_schedule": {"schedules": {}}})
    assert api.index_meta._scheduled_index_names(7, 3) == {"from_row", "from_key"}  # noqa: SLF001


def test_scheduled_names_fall_back_to_the_settings_before_the_schedule_columns_exist(api):
    set_meta(api, {"from_key": {"schedules": {"7": BODY}}})
    with api.session_factory() as session:
        session.execute(sys.modules["sqlalchemy"].text(f"DROP TABLE {api.EntitySchedule.__tablename__}"))
        session.commit()
    assert api.index_meta._scheduled_index_names(7, 3) == {"from_key"}  # noqa: SLF001


class FakeQuery:
    def __init__(self, rows):
        self.rows = rows

    def filter(self, *args):
        return self

    def order_by(self, *args):
        return self

    def all(self):
        return self.rows


def get_summary(api, monkeypatch, rows, query="true"):
    @contextmanager
    def session_for_schema(connection_string, schema):
        yield types.SimpleNamespace(query=lambda *columns: FakeQuery(rows))

    module = api.index_meta
    monkeypatch.setattr(module, "load_and_validate_toolkit_for_index", lambda config: ("3", "conn", None))
    monkeypatch.setattr(module, "inspect", lambda engine: types.SimpleNamespace(has_table=lambda *args, **kwargs: True))
    monkeypatch.setattr(module, "get_session_for_schema", session_for_schema)
    api.request.args = {"summary": query}
    return module.PromptLibAPI().get(7, 3)


def test_the_summary_lists_name_state_time_and_the_two_flags(api, monkeypatch):
    api.seed_index("docs")
    rows = [("docs", "indexed", 1700000000, {"chunk_size": 500}), ("fresh", "in_progress", 1690000000, None)]
    result, status = get_summary(api, monkeypatch, rows)
    assert status == 200
    assert result == [
        {"name": "docs", "state": "indexed", "updated_on": 1700000000, "scheduled": True, "schedulable": True},
        {"name": "fresh", "state": "in_progress", "updated_on": 1690000000, "scheduled": False, "schedulable": False}]


def test_a_failing_summary_answers_400_without_the_error_text(api, monkeypatch):
    def broken(*args):
        raise RuntimeError("secret connection detail")

    monkeypatch.setattr(api.index_meta, "_index_summary", broken)
    monkeypatch.setattr(api.index_meta, "load_and_validate_toolkit_for_index", lambda config: ("3", "conn", None))
    monkeypatch.setattr(api.index_meta, "inspect", lambda engine: types.SimpleNamespace(has_table=lambda *a, **k: True))
    api.request.args = {"summary": "true"}
    body, status = api.index_meta.PromptLibAPI().get(7, 3)
    assert status == 400 and "secret" not in repr(body)


def with_pgvector(tools_env, monkeypatch, connection_string, counts):
    vectorstore = types.ModuleType(f"{PACKAGE}.utils.vectorstore")
    vectorstore.get_pgvector_connection_string = lambda project_id: connection_string
    monkeypatch.setitem(sys.modules, vectorstore.__name__, vectorstore)
    monkeypatch.setattr(tools_env.module, "batch_count_toolkit_indexes", counts, raising=False)
    tools = sys.modules[f"{PACKAGE}.models.all"].EliteATool
    with tools_env.session_factory() as session:
        for row in session.query(tools).all():
            row.settings = {"pgvector_configuration": {"x": 1}}
        session.add(tools(id=4, name="Other", type="artifact", settings={"pgvector_configuration": {"x": 1}}))
        session.add(tools(id=5, name="Plain", type="artifact", settings={}))
        session.commit()
        return tools_env.module._toolkit_ids_with_indexes(7, session.query(tools))  # noqa: SLF001


def test_has_indexes_keeps_the_toolkits_whose_store_holds_an_index(tools_env, monkeypatch):
    seen = []
    ids = with_pgvector(tools_env, monkeypatch, "conn", lambda schemas: seen.append(schemas) or {"3": 2, "4": 0})
    assert ids == [3] and sorted(seen[0]) == ["3", "4"]  # toolkit 5 has no vector store settings


def test_has_indexes_finds_no_toolkit_without_a_connection_string(tools_env, monkeypatch):
    assert with_pgvector(tools_env, monkeypatch, None, lambda schemas: {"3": 2}) == []


def test_has_indexes_finds_no_toolkit_when_the_count_fails(tools_env, monkeypatch):
    def failing(schemas):
        raise RuntimeError("store down")

    assert with_pgvector(tools_env, monkeypatch, "conn", failing) == []
