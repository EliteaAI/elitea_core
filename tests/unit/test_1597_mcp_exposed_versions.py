"""#1597: an external MCP client reaches every agent version tagged ``mcp`` and no other; the default version under
the agent's name, any other one as ``<agent>_<version>``; under enforce only versions with an active binding."""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = "plugins.elitea_core"


def drop_stubbed_sqlalchemy(monkeypatch):
    """Other suites leave a stub ``sqlalchemy`` in ``sys.modules``; the rule module imports the real one."""
    for name in [name for name in sys.modules if name == "sqlalchemy" or name.startswith("sqlalchemy.")]:
        if getattr(sys.modules[name], "__file__", None) is None:
            monkeypatch.delitem(sys.modules, name)


@pytest.fixture()
def rule(monkeypatch):
    for name in ("plugins", PKG, f"{PKG}.models", f"{PKG}.utils"):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    identity = types.ModuleType(f"{PKG}.models.agent_identity")
    identity.AgentIdentityBinding = object
    monkeypatch.setitem(sys.modules, identity.__name__, identity)
    drop_stubbed_sqlalchemy(monkeypatch)
    spec = importlib.util.spec_from_file_location(
        f"{PKG}.utils.mcp_exposed_version", PLUGIN_ROOT / "utils" / "mcp_exposed_version.py")
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def version(version_id, name, *tags):
    return types.SimpleNamespace(id=version_id, name=name, tags=[types.SimpleNamespace(name=tag) for tag in tags])


def agent(agent_id, name, versions, default=None, description="does things"):
    by_id = {item.id: item for item in versions}
    chosen = by_id.get(default) or next((item for item in versions if item.name == "base"), None)
    return types.SimpleNamespace(id=agent_id, name=name, description=description, versions=versions,
                                 get_default_version=lambda: chosen)


def reached(rule, applications, bound=None, governed=False):
    return [(exposed.tool_name, exposed.version_id)
            for exposed in rule.exposed_versions(applications, bound or {}, governed)]


def test_the_tagged_version_runs_and_the_newer_untagged_one_is_not_reachable(rule):
    helper = agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2")])
    assert reached(rule, [helper]) == [("Helper", 10)]


def test_a_tagged_version_that_is_not_the_default_gets_the_version_in_its_name(rule):
    helper = agent(1, "Helper", [version(10, "base"), version(11, "v2", "mcp")])
    assert reached(rule, [helper]) == [("Helper_v2", 11)]


def test_every_tagged_version_is_a_tool_and_the_default_keeps_the_agent_name(rule):
    helper = agent(1, "Helper", [version(12, "v3", "mcp"), version(10, "base", "mcp"), version(11, "v2", "mcp")])
    assert reached(rule, [helper]) == [("Helper", 10), ("Helper_v2", 11), ("Helper_v3", 12)]


def test_set_as_default_moves_the_agent_name_to_the_new_default(rule):
    versions = [version(10, "base", "mcp"), version(11, "v2", "mcp")]
    assert reached(rule, [agent(1, "Helper", versions, default=11)]) == [("Helper", 11), ("Helper_base", 10)]


def test_a_default_that_no_longer_exists_falls_back_to_base(rule):
    versions = [version(10, "base", "mcp"), version(11, "v2", "mcp")]
    assert reached(rule, [agent(1, "Helper", versions, default=99)]) == [("Helper", 10), ("Helper_v2", 11)]


def test_an_agent_with_no_tagged_version_and_other_tags_are_not_exposed(rule):
    assert reached(rule, [agent(1, "Helper", [version(10, "base", "MCP", "code"), version(11, "v2")])]) == []


def test_names_are_sanitised_as_a_whole(rule):
    helper = agent(1, "My Helper", [version(10, "base"), version(11, "release 1.2", "mcp")])
    assert reached(rule, [helper]) == [("My_Helper_release_1_2", 11)]


def test_the_oldest_agent_keeps_a_tool_name_whatever_order_the_agents_are_listed_in(rule):
    first = agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")])
    twin = agent(2, "Helper", [version(20, "base", "mcp"), version(21, "v2", "mcp")])
    assert reached(rule, [twin, first]) == [("Helper", 10), ("Helper_v2", 11)]


def test_an_agents_own_name_is_claimed_before_any_version_name(rule):
    helper = agent(1, "Helper", [version(10, "base"), version(11, "v2", "mcp")])
    named_alike = agent(3, "Helper_v2", [version(30, "base", "mcp")])
    assert reached(rule, [helper, named_alike]) == [("Helper_v2", 30)]


def test_tagging_a_version_of_an_older_agent_does_not_take_the_name_a_newer_agent_serves(rule):
    helper = agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2")])
    newer = agent(2, "Helper_v2", [version(20, "base", "mcp")])
    assert reached(rule, [helper, newer]) == [("Helper", 10), ("Helper_v2", 20)]
    helper.versions[1].tags.append(types.SimpleNamespace(name="mcp"))
    assert reached(rule, [helper, newer]) == [("Helper", 10), ("Helper_v2", 20)]


def test_the_versions_are_returned_by_agent_with_the_one_named_after_it_first(rule):
    first = agent(1, "Alpha", [version(10, "base", "mcp"), version(11, "v2", "mcp")])
    second = agent(2, "Beta", [version(20, "base", "mcp"), version(21, "v2", "mcp")], default=21)
    assert reached(rule, [second, first]) == [("Alpha", 10), ("Alpha_v2", 11), ("Beta", 21), ("Beta_base", 20)]


def test_a_session_scoped_to_one_version_reaches_it_alone_under_the_agent_name(rule):
    older = agent(1, "Helper_v2", [version(5, "base", "mcp")])
    helper = agent(2, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp"), version(12, "v3")])
    assert reached(rule, [older, helper]) == [("Helper_v2", 5), ("Helper", 10)]
    scoped = rule.exposed_versions([helper], {}, False, scoped_version_id=11)
    assert [(exposed.tool_name, exposed.version_id, exposed.description) for exposed in scoped] == [
        ("Helper", 11, "Version 'v2' of agent 'Helper'. does things")]
    assert rule.exposed_versions([helper], {}, False, scoped_version_id=12) == []
    assert rule.exposed_versions([helper], {2: {10}}, True, scoped_version_id=11) == []


def test_the_version_tool_says_which_version_it_is(rule):
    helper = agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")])
    default, other = rule.exposed_versions([helper], {}, False)
    assert default.description == "does things"
    assert other.description == "Version 'v2' of agent 'Helper'. does things"
    assert (default.rule, other.version_name, other.application_id) == ("tagged", "v2", 1)


def test_an_agent_without_a_description_still_lists(rule):
    helper = agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp")], description=None)
    default, other = rule.exposed_versions([helper], {}, False)
    assert (default.description, other.description) == ("", "Version 'v2' of agent 'Helper'.")


@pytest.mark.parametrize("bound, expected", [
    ({1: {10, 11}}, [("Helper", 10), ("Helper_v2", 11)]),
    ({1: {11}}, [("Helper_v2", 11)]),
    ({1: {12}}, []),
    ({2: {10}}, []),
    ({}, []),
])
def test_under_enforce_a_tagged_version_also_needs_an_active_binding(rule, bound, expected):
    helper = agent(1, "Helper", [version(10, "base", "mcp"), version(11, "v2", "mcp"), version(12, "v3")])
    assert reached(rule, [helper], bound, governed=True) == expected
    assert {exposed.rule for exposed in rule.exposed_versions([helper], bound, True)} <= {"bound"}


def test_off_and_audit_only_read_no_bindings(rule):
    class Unread:
        def execute(self, *args, **kwargs):
            raise AssertionError("bindings are read under enforce only")

    helper = agent(1, "Helper", [version(10, "base", "mcp")])
    assert [exposed.version_id for exposed in rule.read_exposed_versions(Unread(), 1, [helper], False)] == [10]
