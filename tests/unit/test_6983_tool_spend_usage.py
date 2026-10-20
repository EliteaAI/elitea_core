"""#6983: member-facing Tool Spend usage (Settings -> Usage) and the feature-aware budget warning.

The Usage page gets one row per Tool Spend feature, built from the same feature_state the
admin pages read, so both show the same spend, limit and source. A feature whose read fails is
left out rather than reported as zero spend.
"""
import types

from unit._tool_spend_loader import load

admin = load("utils/feature_budget_admin.py")
usage = load("rpc/tool_spend_usage.py")

FEATURES = {"web_search": {"label": "Web search", "backends": {"tavily": {}}}}


def state(**values):
    base = {
        "effective_limit": 10.0, "limit_source": "default", "spend": 2.5, "remaining": 7.5,
        "percent_used": 25.0, "calls": 31, "feature_enabled_effective": True, "spend_available": True,
    }
    base.update(values)
    return base


class TestFeatureUsageRow:
    def test_shape_matches_the_llm_usage_payload(self):
        row = usage.feature_usage(state())
        assert row == {
            "effective_limit": 10.0, "limit_source": "default", "currency": "USD", "spend": 2.5,
            "remaining": 7.5, "percent_used": 25.0, "calls": 31, "enabled": True, "spend_available": True,
        }

    def test_no_limit_reads_unlimited(self):
        row = usage.feature_usage(state(effective_limit=None, limit_source="default", remaining=None, percent_used=None))
        assert row["limit_source"] == "unlimited" and row["percent_used"] is None

    def test_disabled_feature_is_flagged(self):
        assert usage.feature_usage(state(feature_enabled_effective=False))["enabled"] is False


class TestToolSpendUsageRpc:
    def _bind(self, monkeypatch, states):
        def fake_state(project_id, feature, user_id=None):
            value = states[feature]
            if isinstance(value, Exception):
                raise value
            return dict(value, user_id=user_id)
        monkeypatch.setattr(usage, "feature_state", fake_state)
        monkeypatch.setattr(usage, "feature_map", lambda schemas: schemas)
        return types.SimpleNamespace(toolkit_schemas=FEATURES)

    def test_one_row_per_feature_with_label(self, monkeypatch):
        module = self._bind(monkeypatch, {"web_search": state()})
        rows = usage.RPC.get_tool_spend_usage(module, project_id=3)
        assert [(r["feature"], r["label"], r["calls"]) for r in rows] == [("web_search", "Web search", 31)]

    def test_failed_read_drops_the_feature_instead_of_zeroing_it(self, monkeypatch):
        module = self._bind(monkeypatch, {"web_search": RuntimeError("rpc down")})
        assert usage.RPC.get_tool_spend_usage(module, project_id=3) == []


class TestFeatureStateCalls:
    def _rpc(self, monkeypatch, spend):
        class Rpc:
            def timeout(self, _seconds):
                return self

            def elitea_core_get_effective_feature_limits(self, **kwargs):
                return {"project_limit_usd": None, "member_limit_usd": None}

            def elitea_core_get_project_feature_budget(self, **kwargs):
                return None

            def elitea_core_get_user_feature_budget(self, **kwargs):
                return None

            def usage_get_feature_spend(self, *args):
                return spend
        monkeypatch.setattr(admin.rpc_tools, "RpcMixin", lambda: types.SimpleNamespace(rpc=Rpc()), raising=False)

    def test_project_and_member_call_counts(self, monkeypatch):
        self._rpc(monkeypatch, {"available": True, "project_spent_nano": 0, "member_spent_nano": 0,
                                "project_calls": 9, "member_calls": 2})
        assert admin.feature_state(3, "web_search")["calls"] == 9
        assert admin.feature_state(3, "web_search", user_id=7)["calls"] == 2

    def test_older_usage_plugin_without_counts_reads_zero(self, monkeypatch):
        self._rpc(monkeypatch, {"available": True, "project_spent_nano": 0, "member_spent_nano": 0})
        assert admin.feature_state(3, "web_search")["calls"] == 0
