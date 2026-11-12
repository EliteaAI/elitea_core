"""Admin budgets listing for a chosen month (api/v2/project_budgets.py, #7510).

The rule under test: a past month reports the limit and source recorded for that month and
never today's. Where nothing was recorded the row says so (limit_available false) instead of
borrowing the current limit, and "unlimited" stays distinct from "not recorded". Spend that
could not be read is null, not $0.00.

Run standalone: python3 tests/unit/test_7510_project_budgets_period_api.py
"""

import datetime
import importlib.util
import os
import sys
import types
import unittest

PLUGIN_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PKG = "elitea_pkg_7510"


def _utc_month(offset=0):
    now = datetime.datetime.now(datetime.timezone.utc)
    index = now.year * 12 + now.month - 1 + offset
    #
    return f"{index // 12:04d}{index % 12 + 1:02d}"


CURRENT = _utc_month(0)
LAST = _utc_month(-1)
OLDER = _utc_month(-2)


class FakeRequest:
    def __init__(self):
        self.args = {}


def _load_module(request):
    """Load the endpoint inside a fake package so its relative imports resolve."""
    tools_stub = types.ModuleType("tools")
    tools_stub.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, with_modes=lambda params: params,
        endpoint_metrics=lambda func: func,
    )
    tools_stub.auth = types.SimpleNamespace(
        decorators=types.SimpleNamespace(check_api=lambda *a, **kw: (lambda func: func)),
    )
    tools_stub.config = types.SimpleNamespace(DEFAULT_MODE="default", ADMINISTRATION_MODE="administration")
    tools_stub.rpc_tools = types.SimpleNamespace(RpcMixin=object)
    tools_stub.register_openapi = lambda *a, **kw: (lambda func: func)
    #
    flask_stub = types.ModuleType("flask")
    flask_stub.request = request
    #
    names = ["tools", "flask", PKG, f"{PKG}.api", f"{PKG}.api.v2", f"{PKG}.utils", f"{PKG}.utils.budget_periods",
             f"{PKG}.utils.tool_spend_features"]
    saved = {name: sys.modules.get(name) for name in names}
    sys.modules["tools"] = tools_stub
    sys.modules["flask"] = flask_stub
    #
    for name in (PKG, f"{PKG}.api", f"{PKG}.api.v2", f"{PKG}.utils"):
        package = types.ModuleType(name)
        package.__path__ = []
        sys.modules[name] = package
    #
    # Only the period validation of a feature name is needed from it, and that has its own tests
    features = types.ModuleType(f"{PKG}.utils.tool_spend_features")
    features.validate_feature_period = lambda feature, period_kind: None
    sys.modules[features.__name__] = features
    #
    try:
        for name, path in (
            (f"{PKG}.utils.budget_periods", os.path.join(PLUGIN_ROOT, "utils", "budget_periods.py")),
            (f"{PKG}.api.v2.project_budgets", os.path.join(PLUGIN_ROOT, "api", "v2", "project_budgets.py")),
        ):
            spec = importlib.util.spec_from_file_location(name, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[name] = module
            spec.loader.exec_module(module)
    except Exception:
        _restore(saved)
        raise
    #
    # The handler imports budget_periods when it runs, so the fake package has to stay in
    # sys.modules until the call returns; the caller restores it
    return module, saved


def _restore(saved):
    for name, original in saved.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original


class FakeRpc:
    """Answers the calls the listing makes and records which ones it made."""

    def __init__(self, snapshots=None, period_spend=None, current_spend=None, current_limits=None):
        self.snapshots = snapshots or {}
        self.period_spend = period_spend if period_spend is not None else {
            "available": True, "spend": {}, "earliest_period": "202601",
        }
        self.current_spend = current_spend or {}
        self.current_limits = current_limits or {}
        self.called = []

    def timeout(self, _seconds):
        return self

    def project_list_paginated(self, **kwargs):
        self.called.append("project_list_paginated")
        projects = [{"id": 1, "name": "alpha", "owner_id": 9}, {"id": 2, "name": "beta", "owner_id": 9}]
        return {"rows": projects, "total": 2, "counts": {"team": 2, "personal": 0}}

    def elitea_core_list_project_budgets(self):
        # Today's settings. A past month must never be built from these
        return {1: {"monthly_limit": 999.0, "enabled": True, "currency": "USD", "member_default_limit": 5.0}}

    def auth_list_users(self, user_ids):
        return []

    def usage_get_projects_spend(self, **kwargs):
        self.called.append("usage_get_projects_spend")
        return self.current_spend

    def usage_get_projects_period_spend(self, **kwargs):
        self.called.append("usage_get_projects_period_spend")
        return self.period_spend

    def elitea_core_get_effective_project_limits(self, **kwargs):
        self.called.append("elitea_core_get_effective_project_limits")
        return self.current_limits

    def elitea_core_list_project_budget_periods(self, **kwargs):
        self.called.append("elitea_core_list_project_budget_periods")
        return self.snapshots


def _call(args, rpc):
    request = FakeRequest()
    request.args = args
    module, saved = _load_module(request)
    #
    try:
        handler = module.AdminAPI()
        module.rpc_tools.RpcMixin = lambda: types.SimpleNamespace(rpc=rpc)
        #
        return handler.get()
    finally:
        _restore(saved)


def _rows(payload):
    return {row["project_id"]: row for row in payload["rows"]}


class TestCurrentMonthUnchanged(unittest.TestCase):
    def test_no_period_reads_live_limits_and_spend(self):
        rpc = FakeRpc(current_spend={1: 12.5}, current_limits={1: 999.0, 2: None})
        payload, status = _call({}, rpc)
        row = _rows(payload)[1]
        #
        self.assertEqual(status, 200)
        self.assertEqual(payload["period"], CURRENT)
        self.assertTrue(payload["is_current"])
        self.assertEqual(row["effective_limit"], 999.0)
        self.assertEqual(row["limit_source"], "explicit")
        self.assertEqual(row["spend"], 12.5)
        self.assertTrue(row["limit_available"])
        self.assertEqual(row["percent_used"], round(12.5 / 999.0 * 100, 2))
        self.assertNotIn("elitea_core_list_project_budget_periods", rpc.called)
        self.assertNotIn("usage_get_projects_period_spend", rpc.called)

    def test_an_explicit_current_period_behaves_like_none(self):
        rpc = FakeRpc(current_limits={1: 999.0})
        payload, _ = _call({"period": CURRENT}, rpc)
        self.assertTrue(payload["is_current"])
        self.assertEqual(_rows(payload)[1]["effective_limit"], 999.0)

    def test_current_rows_still_carry_the_editable_settings(self):
        rpc = FakeRpc(current_limits={1: 999.0})
        row = _rows(_call({}, rpc)[0])[1]
        self.assertEqual(row["monthly_limit"], 999.0)
        self.assertEqual(row["member_default_limit"], 5.0)
        self.assertTrue(row["enabled"])


class TestPastMonthUsesTheRecord(unittest.TestCase):
    def test_limit_and_source_come_from_the_snapshot_not_from_today(self):
        # Today the project is capped at 999 explicitly; in LAST it was a 25 default
        rpc = FakeRpc(
            snapshots={1: {"effective_limit": 25.0, "limit_source": "default", "currency": "USD"}},
            period_spend={"available": True, "spend": {1: 10.0}, "earliest_period": "202601"},
            current_limits={1: 999.0},
        )
        payload, status = _call({"period": LAST}, rpc)
        row = _rows(payload)[1]
        #
        self.assertEqual(status, 200)
        self.assertFalse(payload["is_current"])
        self.assertEqual(row["effective_limit"], 25.0)
        self.assertEqual(row["limit_source"], "default")
        self.assertEqual(row["percent_used"], 40.0)
        self.assertTrue(row["limit_available"])
        self.assertNotIn("elitea_core_get_effective_project_limits", rpc.called)

    def test_a_project_with_no_snapshot_is_not_available_not_unlimited(self):
        rpc = FakeRpc(
            snapshots={},
            period_spend={"available": True, "spend": {1: 10.0}, "earliest_period": "202601"},
            current_limits={1: 999.0},
        )
        row = _rows(_call({"period": LAST}, rpc)[0])[1]
        #
        self.assertFalse(row["limit_available"])
        self.assertIsNone(row["effective_limit"])
        self.assertIsNone(row["limit_source"])
        self.assertIsNone(row["percent_used"])
        # Spend was recorded, so it is still shown
        self.assertEqual(row["spend"], 10.0)

    def test_a_recorded_unlimited_month_stays_distinct_from_not_recorded(self):
        rpc = FakeRpc(snapshots={
            1: {"effective_limit": None, "limit_source": "unlimited", "currency": "USD"},
        })
        rows = _rows(_call({"period": LAST}, rpc)[0])
        #
        self.assertTrue(rows[1]["limit_available"])
        self.assertIsNone(rows[1]["effective_limit"])
        self.assertEqual(rows[1]["limit_source"], "unlimited")
        self.assertFalse(rows[2]["limit_available"])

    def test_a_zero_limit_month_is_a_real_limit(self):
        rpc = FakeRpc(
            snapshots={1: {"effective_limit": 0.0, "limit_source": "explicit", "currency": "USD"}},
            period_spend={"available": True, "spend": {1: 3.0}, "earliest_period": "202601"},
        )
        row = _rows(_call({"period": LAST}, rpc)[0])[1]
        # 0 is a limit of zero, not "no limit"; used% has no meaningful value against it
        self.assertEqual(row["effective_limit"], 0.0)
        self.assertTrue(row["limit_available"])
        self.assertIsNone(row["percent_used"])

    def test_todays_editable_settings_are_not_shown_for_a_past_month(self):
        rpc = FakeRpc()
        row = _rows(_call({"period": LAST}, rpc)[0])[1]
        #
        self.assertIsNone(row["monthly_limit"])
        self.assertIsNone(row["member_default_limit"])
        self.assertIsNone(row["enabled"])


class TestSpendAvailability(unittest.TestCase):
    def test_unreadable_spend_is_null_not_zero(self):
        rpc = FakeRpc(
            snapshots={1: {"effective_limit": 25.0, "limit_source": "default", "currency": "USD"}},
            period_spend={"available": False, "spend": {}, "earliest_period": None},
        )
        row = _rows(_call({"period": LAST}, rpc)[0])[1]
        #
        self.assertFalse(row["spend_available"])
        self.assertIsNone(row["spend"])
        self.assertIsNone(row["percent_used"])
        self.assertIsNone(row["remaining"])

    def test_a_failing_spend_call_is_treated_as_unavailable(self):
        class Broken(FakeRpc):
            def usage_get_projects_period_spend(self, **kwargs):
                raise RuntimeError("usage is down")
        #
        row = _rows(_call({"period": LAST}, Broken())[0])[1]
        self.assertFalse(row["spend_available"])
        self.assertIsNone(row["spend"])

    def test_genuine_zero_spend_is_zero_and_available(self):
        rpc = FakeRpc(
            snapshots={1: {"effective_limit": 25.0, "limit_source": "default", "currency": "USD"}},
            period_spend={"available": True, "spend": {1: 0.0}, "earliest_period": "202601"},
        )
        row = _rows(_call({"period": LAST}, rpc)[0])[1]
        self.assertTrue(row["spend_available"])
        self.assertEqual(row["spend"], 0.0)
        self.assertEqual(row["percent_used"], 0.0)


class TestPeriodValidation(unittest.TestCase):
    def test_bad_periods_are_a_400_and_nothing_is_read(self):
        for bad in ("2026-10", "abcdef", "202613", _utc_month(1), _utc_month(-4)):
            rpc = FakeRpc()
            payload, status = _call({"period": bad}, rpc)
            self.assertEqual(status, 400, bad)
            self.assertIn("error", payload)
            self.assertEqual(rpc.called, [], bad)

    def test_the_response_reports_the_reporting_range(self):
        payload, _ = _call({"period": OLDER}, FakeRpc())
        year, month = int(OLDER[:4]), int(OLDER[4:])
        self.assertEqual(payload["period"], OLDER)
        self.assertEqual(payload["period_start"], f"{year:04d}-{month:02d}-01")
        self.assertTrue(payload["period_end"].startswith(f"{year:04d}-{month:02d}-"))

    def test_tool_spend_has_no_monthly_history(self):
        payload, status = _call({"period": LAST, "feature": "web_search"}, FakeRpc())
        self.assertEqual(status, 400)
        self.assertIn("error", payload)


if __name__ == "__main__":
    unittest.main(verbosity=2)
