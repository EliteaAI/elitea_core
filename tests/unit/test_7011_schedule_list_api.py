"""EL-7011 US-05: the schedules list handler, scope, filters and paging (R1.2-R5.3).

Loads the real `api/v2/schedules.py`, the real pydantic models and the real `schedule_access`
helper. Only the runtime edges are faked: `tools`, `flask`, the list query and the owner lookup.
The fake query follows the contract of `list_schedules`: own-only scope for a regular caller,
`owner_ids` for an admin, then the page and the total.
"""
import importlib.util
import pathlib
import sys
import types
from datetime import UTC, datetime

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = "schedulespkg_7011_list"
PREFIX = "models.applications.schedules."
ROW_KEYS = {
    "id", "name", "description", "entity_type", "run_mode", "application_id", "entity_name",
    "version_id", "version_name", "cron", "timezone", "enabled", "disabled_reason", "next_run",
    "last_run", "last_run_status", "last_run_reason", "last_conversation_id", "expires_at",
    "owner", "allowed_actions",
}


class FakeArgs(dict):
    """Werkzeug MultiDict subset: each key holds a list; `get` and `to_dict` give the first value."""

    def get(self, key, default=None, type=None):
        if key not in self:
            return default
        return type(self[key][0]) if type else self[key][0]

    def getlist(self, key):
        return list(super().get(key, []))

    def to_dict(self, flat=True):
        return {k: v[0] for k, v in self.items()}


class Row(types.SimpleNamespace):
    @property
    def _mapping(self):
        return vars(self)


def make_row(row_id, owner, **over):
    fields = {
        "id": row_id, "name": f"s{row_id}", "description": None, "entity_type": "agent", "run_mode": None,
        "application_id": 1, "entity_name": "Agent", "version_id": 10, "version_name": "latest",
        "cron": "0 9 * * *", "timezone": "UTC", "enabled": True, "disabled_reason": None, "next_run": None,
        "last_run": None, "last_run_status": None, "last_run_reason": None, "last_conversation_id": None,
        "expires_at": None, "created_by": owner, "tags": ["leaked"],
    }
    fields.update(over)
    return Row(**fields)


class Env:
    """What the fakes record and what a test sets."""

    def __init__(self):
        self.user_id = 9
        self.permissions = set()
        self.rows = []
        self.names = {}
        self.names_error = None
        self.query_calls = []
        self.author_calls = []
        self.args = FakeArgs()

    def grant(self, user_id, *verbs):
        self.user_id = user_id
        self.permissions = {PREFIX + verb for verb in verbs}


def _load(monkeypatch, rel_path, name):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / rel_path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


def _register(monkeypatch, name, module):
    monkeypatch.setitem(sys.modules, name, module)
    return module


@pytest.fixture
def env(monkeypatch):
    env = Env()

    for name in ("pylon", "pylon.core"):
        _register(monkeypatch, name, types.ModuleType(name)).__path__ = []
    quiet = lambda *a, **k: None
    _register(monkeypatch, "pylon.core.tools", types.ModuleType("pylon.core.tools")).log = (
        types.SimpleNamespace(warning=quiet, info=quiet, error=quiet, debug=quiet, exception=quiet)
    )

    def check_api(spec):
        def decorator(func):
            def wrapper(*args, **kwargs):
                if not set(spec["permissions"]) <= env.permissions:
                    return {"error": "forbidden"}, 403
                return func(*args, **kwargs)

            return wrapper

        return decorator

    class APIModeHandler:
        pass

    class APIBase:
        pass

    api_tools = types.SimpleNamespace(
        APIModeHandler=APIModeHandler, APIBase=APIBase, endpoint_metrics=lambda func: func,
        with_modes=lambda params: list(params),
    )

    class Session:
        def __enter__(self):
            return "session"

        def __exit__(self, *exc):
            return False

    tools = types.ModuleType("tools")
    tools.api_tools = api_tools
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE="administration", DEFAULT_MODE="default")
    tools.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=check_api),
        current_user=lambda: {"id": env.user_id},
        resolve_permissions=lambda **kwargs: set(env.permissions),
    )
    tools.db = types.SimpleNamespace(get_session=lambda project_id: Session())
    tools.register_openapi = lambda **meta: (lambda func: func)
    tools.serialize = lambda data: data
    _register(monkeypatch, "tools", tools)

    flask = types.ModuleType("flask")
    flask.request = types.SimpleNamespace(args=env.args, json={}, method="GET")
    _register(monkeypatch, "flask", flask)

    for name in (PKG, f"{PKG}.api", f"{PKG}.models", f"{PKG}.models.pd", f"{PKG}.utils"):
        _register(monkeypatch, name, types.ModuleType(name)).__path__ = []
    v2 = _register(monkeypatch, f"{PKG}.api.v2", types.ModuleType(f"{PKG}.api.v2"))
    v2.__path__ = [str(PLUGIN_ROOT / "api" / "v2")]

    _load(monkeypatch, "models/pd/collection_base.py", f"{PKG}.models.pd.collection_base")
    _load(monkeypatch, "models/pd/schedule_list.py", f"{PKG}.models.pd.schedule_list")
    _load(monkeypatch, "utils/constants.py", f"{PKG}.utils.constants")
    _load(monkeypatch, "utils/schedule_access.py", f"{PKG}.utils.schedule_access")

    def get_authors_data(author_ids):
        env.author_calls.append(list(author_ids))
        if env.names_error:
            raise env.names_error
        users = {uid: v if isinstance(v, dict) else {"name": v} for uid, v in env.names.items()}
        return [{"id": uid, **users[uid]} for uid in author_ids if uid in users]

    authors = _register(monkeypatch, f"{PKG}.utils.authors", types.ModuleType(f"{PKG}.utils.authors"))
    authors.get_authors_data = get_authors_data

    def list_schedules(session, scope, filters, limit, offset):
        env.query_calls.append({"scope": scope, "filters": filters, "limit": limit, "offset": offset})
        rows = env.rows
        if not scope.is_admin:
            rows = [r for r in rows if r.created_by == scope.user_id]
        elif getattr(filters, "owner_ids", None):
            rows = [r for r in rows if r.created_by in filters.owner_ids]
        return len(rows), rows[offset:offset + limit]

    queries = _register(monkeypatch, f"{PKG}.utils.schedule_queries", types.ModuleType(f"{PKG}.utils.schedule_queries"))
    queries.list_schedules = list_schedules

    env.handler = _load(monkeypatch, "api/v2/schedules.py", f"{PKG}.api.v2.schedules")
    return env


def call(env, **args):
    env.args.clear()
    env.args.update({k: [str(x) for x in v] if isinstance(v, list) else [str(v)] for k, v in args.items()})
    return env.handler.PromptLibAPI().get(project_id=1)


def ok(env, **args):
    body, status = call(env, **args)
    assert status == 200
    return body


# R1.2 (TC-US05-02)
def test_r1_2_el7011_every_row_has_owner_id_and_name(env):
    env.grant(9, "list", "admin")
    env.rows = [make_row(1, 5), make_row(2, 6)]
    env.names = {5: "Ann", 6: "Bob"}
    body = ok(env)
    assert [r["owner"] for r in body["rows"]] == [{"id": 5, "name": "Ann"}, {"id": 6, "name": "Bob"}]
    assert len(env.author_calls) == 1 and set(env.author_calls[0]) == {5, 6}


def test_r1_2_el7011_owner_without_a_name_shows_the_email(env):
    """BUG-15: a user with no display name shows the email, like the Owner filter does."""
    env.grant(9, "list", "admin")
    env.rows = [make_row(1, 5)]
    env.names = {5: {"name": None, "email": "sched.user01@centry.user"}}
    assert ok(env)["rows"][0]["owner"] == {"id": 5, "name": "sched.user01@centry.user"}


# R1.3 (TC-US05-03)
@pytest.mark.parametrize("error", [None, RuntimeError("auth down")], ids=["empty lookup", "lookup raises"])
def test_r1_3_el7011_owner_name_is_null_when_lookup_fails(env, error):
    env.grant(9, "list", "admin")
    env.rows = [make_row(1, 5), make_row(2, 6)]
    env.names_error = error
    body = ok(env)
    assert [r["owner"] for r in body["rows"]] == [{"id": 5, "name": None}, {"id": 6, "name": None}]


# R1.2
def test_r1_2_el7011_empty_page_skips_owner_lookup(env):
    env.grant(9, "list", "admin")
    body = ok(env)
    assert body == {"total": 0, "rows": []}
    assert env.author_calls == []


# R1.4 (TC-US05-04)
def test_r1_4_el7011_caller_without_list_gets_403(env):
    env.grant(5, "details")
    env.rows = [make_row(1, 5)]
    body, status = call(env)
    assert status == 403 and "rows" not in body
    assert env.query_calls == []


# R2.2 (TC-US05-07)
@pytest.mark.parametrize("owner_ids", ["6", "5,6"])
def test_r2_2_el7011_regular_caller_owner_ids_is_ignored(env, owner_ids):
    env.grant(5, "list")
    env.rows = [make_row(1, 5), make_row(2, 6), make_row(3, 6)]
    body = ok(env, owner_ids=owner_ids)
    assert {r["owner"]["id"] for r in body["rows"]} == {5}
    assert body["total"] == 1
    assert env.query_calls[0]["scope"].is_admin is False


# R2.2
def test_r2_2_el7011_admin_repeated_owner_ids_reach_the_query(env):
    env.grant(9, "list", "admin")
    env.rows = [make_row(1, 5), make_row(2, 6), make_row(3, 7)]
    body = ok(env, owner_ids=[5, 6])
    assert env.query_calls[0]["filters"].owner_ids == [5, 6]
    assert {r["owner"]["id"] for r in body["rows"]} == {5, 6}


# R3.4 (TC-US05-13)
@pytest.mark.parametrize("name,value", [("enabled", "maybe"), ("entity_id", "abc"), ("owner_ids", "a,b")])
def test_r3_4_el7011_wrong_filter_type_gives_400_with_name(env, name, value):
    env.grant(9, "list", "admin")
    env.rows = [make_row(1, 5)]
    body, status = call(env, **{name: value})
    assert status == 400 and name in str(body)
    assert env.query_calls == []


# R4.1 (TC-US05-14)
def test_r4_1_el7011_admin_gets_run_only_on_own_row_and_change_owner(env):
    env.grant(9, "list", "admin", "run")
    env.rows = [make_row(1, 9), make_row(2, 5)]
    own, other = ok(env)["rows"]
    assert "run" in own["allowed_actions"]
    assert "run" not in other["allowed_actions"] and "change_owner" in other["allowed_actions"]


# R4.1 (TC-US05-15)
def test_r4_1_el7011_regular_caller_gets_no_change_owner_and_no_run(env):
    env.grant(5, "list", "run")
    env.rows = [make_row(1, 5, version_id=None), make_row(2, 6)]
    rows = ok(env)["rows"]
    assert [r["id"] for r in rows] == [1]
    assert rows[0]["allowed_actions"] == []


# R4.3 (TC-US05-24)
def test_r4_3_el7011_empty_description_and_run_mode_stay_null(env):
    env.grant(5, "list")
    env.rows = [make_row(1, 5, description=None, run_mode=None)]
    row = ok(env)["rows"][0]
    assert "description" in row and "run_mode" in row
    assert row["description"] is None and row["run_mode"] is None


# R4.3
def test_r4_3_el7011_stored_description_and_run_mode_are_returned(env):
    env.grant(5, "list")
    env.rows = [make_row(1, 5, description="Weekday ticket summary", entity_type="pipeline", run_mode="entry_point")]
    row = ok(env)["rows"][0]
    assert (row["description"], row["run_mode"]) == ("Weekday ticket summary", "entry_point")


# R4.3: the UI parses next_run and last_run with new Date()
def test_r4_3_el7011_dates_are_iso_strings_with_one_offset(env):
    env.grant(5, "list")
    env.rows = [make_row(1, 5, next_run=datetime(2026, 10, 21, 6, 0, tzinfo=UTC))]
    row = ok(env)["rows"][0]
    assert row["next_run"] == "2026-10-21T06:00:00Z"
    assert row["last_run"] is None


# R5.2 (TC-US05-19)
@pytest.mark.parametrize("limit", [0, 101, -1])
def test_r5_2_el7011_limit_out_of_range_gives_400(env, limit):
    env.grant(9, "list", "admin")
    body, status = call(env, limit=limit)
    assert status == 400 and "limit" in str(body)
    assert env.query_calls == []


# R5.2 (TC-US05-19)
@pytest.mark.parametrize("limit", [1, 100])
def test_r5_2_el7011_limit_at_the_bounds_is_accepted(env, limit):
    env.grant(9, "list", "admin")
    ok(env, limit=limit)
    assert env.query_calls[0]["limit"] == limit


# R5.1 (TC-US05-22)
def test_r5_1_el7011_default_page_is_20_rows_with_full_total(env):
    env.grant(9, "list", "admin")
    env.rows = [make_row(i, 5) for i in range(30)]
    body = ok(env)
    assert len(body["rows"]) == 20 and body["total"] == 30
    assert (env.query_calls[0]["limit"], env.query_calls[0]["offset"]) == (20, 0)


# R5.3 (TC-US05-21)
def test_r5_3_el7011_rows_have_no_tags_field(env):
    env.grant(9, "list", "admin")
    env.rows = [make_row(1, 5), make_row(2, 6)]
    rows = ok(env)["rows"]
    assert len(rows) == 2
    for row in rows:
        assert "tags" not in row
        assert set(row) == ROW_KEYS
