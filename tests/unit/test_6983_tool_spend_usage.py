"""#6983: Tool Spend limits for Settings -> Usage.

elitea_core returns limits only; usage reads its own spend counters, so there is no
usage -> elitea_core -> usage RPC ring on the Usage page. A feature whose limits cannot be
resolved is left out rather than shown as unlimited.
"""
import types

from unit._tool_spend_loader import load

usage = load("rpc/tool_spend_usage.py")

FEATURES = {"web_search": {"label": "Web search", "backends": {"tavily": {}}}}


def module(limits):
    def resolve(project_id, user_id, feature):
        if isinstance(limits, Exception):
            raise limits
        return limits
    return types.SimpleNamespace(toolkit_schemas=FEATURES, get_effective_feature_limits=resolve)


def test_project_scope_reads_project_limit(monkeypatch):
    monkeypatch.setattr(usage, "feature_map", lambda schemas: schemas)
    rows = usage.RPC.get_tool_spend_limits(module({
        "project_limit_usd": 10.0, "project_source": "explicit", "member_limit_usd": 1.0, "feature_enabled": True,
    }), project_id=3)
    assert rows == [{"feature": "web_search", "label": "Web search", "limit_usd": 10.0,
                     "limit_source": "explicit", "enabled": True}]


def test_member_scope_reads_member_limit(monkeypatch):
    monkeypatch.setattr(usage, "feature_map", lambda schemas: schemas)
    rows = usage.RPC.get_tool_spend_limits(module({
        "project_limit_usd": 10.0, "member_limit_usd": 1.0, "member_source": "default",
    }), project_id=3, user_id=7)
    assert (rows[0]["limit_usd"], rows[0]["limit_source"]) == (1.0, "default")


def test_no_limit_reads_unlimited_even_when_resolver_says_default(monkeypatch):
    monkeypatch.setattr(usage, "feature_map", lambda schemas: schemas)
    rows = usage.RPC.get_tool_spend_limits(module({"project_limit_usd": None, "project_source": "default"}), project_id=3)
    assert rows[0]["limit_source"] == "unlimited"


def test_disabled_feature_is_flagged(monkeypatch):
    monkeypatch.setattr(usage, "feature_map", lambda schemas: schemas)
    rows = usage.RPC.get_tool_spend_limits(module({"feature_enabled": False}), project_id=3)
    assert rows[0]["enabled"] is False


def test_failed_resolution_drops_the_feature(monkeypatch):
    monkeypatch.setattr(usage, "feature_map", lambda schemas: schemas)
    assert usage.RPC.get_tool_spend_limits(module(RuntimeError("db down")), project_id=3) == []


def test_labels_come_from_toolkit_metadata(monkeypatch):
    monkeypatch.setattr(usage, "feature_map", lambda schemas: schemas)
    assert usage.RPC.get_tool_spend_labels(module({})) == {"web_search": "Web search"}
