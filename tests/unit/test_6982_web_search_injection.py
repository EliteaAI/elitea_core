"""#6982: Web Search toggle - toolkit resolution, availability, injection and the shared flag.

The module reads its world through a few seams (settings lookup, toolkit rows, the stored-toolkit
loader, RPC role lookups, module state). Each test wires fakes into those seams and runs the
production decision code unchanged.
"""
import sys
import types

import pytest

from unit._tool_spend_loader import load

load("utils/tool_spend_features.py")
ws = load("utils/web_search.py")

PUBLIC, PROJECT, OTHER = 1, 3, 99
TAVILY_SCHEMA = {"title": "tavily", "metadata": {"tool_spend": {
    "feature": "web_search", "label": "Web search", "backend": "tavily", "configuration_type": "tavily",
    "operations": ["search", "extract", "crawl", "map"], "max_units_per_call": 12,
    "chat_tools": ["web_search", "web_extract"],
}}}


class World:
    """Projects' web_search_settings picks, toolkit rows, and how get_toolkit_config answers."""

    def __init__(self):
        self.picks = {}          # project_id -> toolkit_id
        self.rows = {}           # (project_id, toolkit_id) -> row
        self.loaded = []         # (owner_pid, user_id, toolkit_id) calls into get_toolkit_config
        self.loader_error = None
        self.enabled = True
        self.project_enabled = True
        self.schemas = {"tavily": TAVILY_SCHEMA}
        self.admin_roles = {}    # (project_id, user_id) -> [role names]

    def add(self, pid, tid, shared=False, type_="tavily", selected=None, name="Tavily"):
        settings = {"tavily_configuration": {"elitea_title": "tav", "private": False}}
        if selected is not None:
            settings["selected_tools"] = selected
        self.rows[(pid, tid)] = {"id": tid, "name": name, "type": type_, "meta": {"shared": shared},
                                 "settings": settings}


@pytest.fixture
def world(monkeypatch):
    w = World()
    module = types.SimpleNamespace(
        toolkit_schemas=w.schemas,
        descriptor=types.SimpleNamespace(config={"cost_budgets": {"features": {"web_search": {
            "enabled": True}}}}),
        get_effective_feature_limits=lambda pid, uid, feature: {"feature_enabled": w.project_enabled},
    )
    monkeypatch.setattr(ws, "this", types.SimpleNamespace(module=module))
    monkeypatch.setattr(ws, "_public_project_id", lambda: PUBLIC)
    monkeypatch.setattr(ws, "_settings_toolkit_id", lambda pid: w.picks.get(pid))

    def row(pid, tid):
        r = w.rows.get((pid, tid))
        return None if r is None else {k: r[k] for k in ("id", "name", "type", "meta")}
    monkeypatch.setattr(ws, "_load_toolkit_row", row)

    class ValidatorNotSupportedError(RuntimeError):
        pass

    def get_toolkit_config(pid, uid, tid):
        w.loaded.append((pid, uid, tid))
        if w.loader_error == "schema":
            raise ValidatorNotSupportedError("no schema")
        if w.loader_error == "missing" or (pid, tid) not in w.rows:
            return {"error": "No such toolkit"}
        r = w.rows[(pid, tid)]
        settings = dict(r["settings"])
        settings["tavily_configuration"] = {"api_key": "SECRET", "configuration_project_id": pid}
        return {"type": r["type"], "name": r["name"], "description": "d", "meta": r["meta"], "settings": settings}

    for name, attrs in {
        "plugins.elitea_core.utils.application_tools": {"ValidatorNotSupportedError": ValidatorNotSupportedError},
        "plugins.elitea_core.utils.predict_utils": {"get_toolkit_config": get_toolkit_config},
    }.items():
        mod = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        monkeypatch.setitem(sys.modules, name, mod)

    def roles(project_id, user_id):
        return [{"name": n} for n in w.admin_roles.get((project_id, user_id), [])]
    rpc = types.SimpleNamespace(timeout=lambda _t: types.SimpleNamespace(admin_get_user_roles=roles))
    monkeypatch.setattr(ws, "rpc_tools", types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=rpc)))
    return w


def inject(project_id=PROJECT, internal=("web_search",), existing=None, user=5):
    return ws.inject_internal_web_search_tool(list(internal), user, project_id, existing or [])


# --- resolution ------------------------------------------------------------

def test_platform_default_is_the_public_shared_pick(world):
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=True)
    tool = inject()
    assert world.loaded == [(PUBLIC, 5, 10)], "must be expanded in its OWNER project"
    assert tool["id"] is None and tool["name"] == "Web Search"
    assert tool["settings"]["selected_tools"] == ["web_search", "web_extract"]


def test_a_project_pick_is_ignored(world):
    # KISS (owner): only the public project's shared pick drives the toggle
    world.picks.update({PUBLIC: 10, PROJECT: 20})
    world.add(PUBLIC, 10, shared=True)
    world.add(PROJECT, 20)
    inject()
    assert world.loaded == [(PUBLIC, 5, 10)]


def test_public_pick_that_is_not_shared_is_refused(world):
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=False)
    assert inject() is None
    assert world.loaded == []


def test_public_pick_that_is_not_a_web_search_toolkit_is_refused(world):
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=True, type_="github")
    assert inject() is None


def test_missing_public_row_means_unavailable(world):
    world.picks[PUBLIC] = 404
    assert inject() is None


def test_no_platform_pick_means_unavailable(world):
    world.add(PROJECT, 20)
    assert inject() is None
    assert world.loaded == []


# --- injection guards ------------------------------------------------------

def test_toggle_off_injects_nothing(world):
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=True)
    assert inject(internal=("image_generation",)) is None
    assert world.loaded == []


def test_manual_web_search_toolkit_suppresses_injection(world):
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=True)
    assert inject(existing=[{"type": "tavily", "id": 77, "settings": {}}]) is None
    assert world.loaded == []


def test_crawl_and_map_never_reach_chat(world):
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=True, selected=["web_search", "web_crawl", "web_map"])
    assert inject()["settings"]["selected_tools"] == ["web_search"]


def test_empty_intersection_skips_instead_of_exposing_all_tools(world):
    # The SDK treats selected_tools=[] as "every tool", which would expose crawl/map
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=True, selected=["web_crawl"])
    assert inject() is None


@pytest.mark.parametrize("error", ["schema", "missing"])
def test_loader_failures_mean_unavailable_never_raise(world, error):
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=True)
    world.loader_error = error
    assert inject() is None


def test_platform_switch_off(world):
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=True)
    ws.this.module.descriptor.config["cost_budgets"]["features"]["web_search"]["enabled"] = False
    assert inject() is None
    assert world.loaded == []


def test_unexpected_error_is_swallowed(world, monkeypatch):
    world.picks[PUBLIC] = 10
    monkeypatch.setattr(ws, "_load_toolkit_row", lambda *a: (_ for _ in ()).throw(RuntimeError("db down")))
    assert inject() is None


# --- availability ----------------------------------------------------------

@pytest.mark.parametrize("setup,reason", [
    (lambda w: setattr(w, "project_enabled", False), "disabled_project"),
    (lambda w: None, "no_toolkit"),
])
def test_availability_reasons(world, setup, reason):
    setup(world)
    assert ws.web_search_availability(PROJECT, 5)["reason"] == reason


def test_availability_reports_resolved_toolkit(world):
    world.picks[PUBLIC] = 10
    world.add(PUBLIC, 10, shared=True, name="Platform Tavily")
    got = ws.web_search_availability(PROJECT, 5)
    assert got == {"available": True, "reason": None, "toolkit": {"id": 10, "name": "Platform Tavily"}}


def test_availability_not_ready_without_schema(world):
    world.schemas.clear()
    assert ws.web_search_availability(PROJECT, 5)["reason"] == "not_ready"


def test_availability_without_project_still_checks_the_toolkit(world):
    assert ws.web_search_availability(None, 5)["reason"] == "no_toolkit"


# --- shared flag -----------------------------------------------------------

PRIVATE_REF = {"tavily_configuration": {"elitea_title": "t", "private": True}}
PROJECT_REF = {"tavily_configuration": {"elitea_title": "t", "private": False}}


def test_unshared_toolkit_needs_no_checks(world):
    assert ws.shared_toolkit_error(OTHER, 5, PRIVATE_REF, {}) is None


def test_only_public_project_toolkits_can_be_shared(world):
    world.admin_roles[(OTHER, 5)] = ["admin"]
    assert "public project" in ws.shared_toolkit_error(OTHER, 5, PROJECT_REF, {"shared": True})


def test_only_public_admins_can_share(world):
    world.admin_roles[(PUBLIC, 5)] = ["editor"]
    assert "admins" in ws.shared_toolkit_error(PUBLIC, 5, PROJECT_REF, {"shared": True})


def test_role_names_merely_containing_admin_do_not_count(world):
    world.admin_roles[(PUBLIC, 5)] = ["billing-admin"]
    assert ws.shared_toolkit_error(PUBLIC, 5, PROJECT_REF, {"shared": True}) is not None


def test_shared_toolkit_needs_a_project_credential(world):
    world.admin_roles[(PUBLIC, 5)] = ["admin"]
    assert "private" in ws.shared_toolkit_error(PUBLIC, 5, PRIVATE_REF, {"shared": True})
    assert ws.shared_toolkit_error(PUBLIC, 5, PROJECT_REF, {"shared": True}) is None


def test_non_admin_cannot_unshare_by_omitting_the_flag(world):
    world.admin_roles[(PUBLIC, 5)] = ["editor"]
    assert ws.shared_toolkit_error(PUBLIC, 5, PROJECT_REF, {}, was_shared=True) is not None


def test_is_public_admin_uses_exact_role_names(world):
    world.admin_roles[(PUBLIC, 5)] = ["viewer", "public_admin", "billing-admin"]
    assert ws.is_public_admin(PUBLIC, 5) is False
    world.admin_roles[(PUBLIC, 5)] = ["viewer", "admin"]
    assert ws.is_public_admin(PUBLIC, 5) is True
