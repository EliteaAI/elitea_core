"""Admin budgets listing for a chosen month (api/v2/project_budgets.py, #7510).

The rule under test: a past month reports spend only. Limits are not kept per month, so a
past row carries no limit, used or source, and today's limit is never applied to old spend.
Spend that could not be read is null, not $0.00. The current month is unchanged.

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

    def __init__(self, period_spend=None, current_spend=None, current_limits=None):
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
        projects = [
            {"id": 1, "name": "alpha", "owner_id": 9},
            {"id": 2, "name": "project_user_9", "owner_id": 9},
        ]
        return {"rows": projects, "total": 2, "counts": {"team": 2, "personal": 0}}

    def elitea_core_list_project_budgets(self):
        # Today's settings. A past month must never be built from these
        return {1: {"monthly_limit": 999.0, "enabled": True, "currency": "USD", "member_default_limit": 5.0}}

    def auth_list_users(self, user_ids):
        return [{"id": 9, "name": "Ada Lovelace", "email": "ada@example.com"}]

    def usage_get_projects_spend(self, **kwargs):
        self.called.append("usage_get_projects_spend")
        return self.current_spend

    def usage_get_projects_period_spend(self, **kwargs):
        self.called.append("usage_get_projects_period_spend")
        return self.period_spend

    def elitea_core_get_effective_project_limits(self, **kwargs):
        self.called.append("elitea_core_get_effective_project_limits")
        return self.current_limits


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
        self.assertEqual(row["percent_used"], round(12.5 / 999.0 * 100, 2))
        self.assertNotIn("elitea_core_list_project_budget_periods", rpc.called)
        self.assertNotIn("usage_get_projects_period_spend", rpc.called)

    def test_a_current_row_has_exactly_the_fields_it_had_before_this_change(self):
        row = _rows(_call({}, FakeRpc(current_limits={1: 999.0}))[0])[1]
        #
        self.assertEqual(set(row), {
            "project_id", "name", "display_name", "owner_name", "owner_email", "is_personal",
            "monthly_limit", "member_default_limit", "effective_limit", "limit_source", "currency",
            "enabled", "spend", "remaining", "percent_used",
        })

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


class TestPastMonthIsSpendOnly(unittest.TestCase):
    SPEND = {"available": True, "spend": {1: 10.0, 2: 0.0}, "earliest_period": "202601"}

    def test_a_row_has_spend_and_none_of_the_limit_fields(self):
        rpc = FakeRpc(period_spend=self.SPEND, current_limits={1: 999.0})
        payload, status = _call({"period": LAST}, rpc)
        row = _rows(payload)[1]
        #
        self.assertEqual(status, 200)
        self.assertFalse(payload["is_current"])
        self.assertEqual(row["spend"], 10.0)
        self.assertTrue(row["spend_available"])
        for field in ("effective_limit", "limit_source", "percent_used", "remaining",
                      "monthly_limit", "member_default_limit", "enabled", "limit_available"):
            self.assertNotIn(field, row, field)

    def test_todays_limits_and_budgets_are_never_read_for_a_past_month(self):
        rpc = FakeRpc(period_spend=self.SPEND, current_limits={1: 999.0})
        #
        _call({"period": LAST}, rpc)
        #
        self.assertNotIn("elitea_core_get_effective_project_limits", rpc.called)
        self.assertNotIn("usage_get_projects_spend", rpc.called)
        self.assertIn("usage_get_projects_period_spend", rpc.called)

    def test_genuine_zero_spend_is_zero_and_available(self):
        row = _rows(_call({"period": LAST}, FakeRpc(period_spend=self.SPEND))[0])[2]
        #
        self.assertEqual(row["spend"], 0.0)
        self.assertTrue(row["spend_available"])

    def test_the_listing_still_pages_and_counts(self):
        payload, _ = _call({"period": LAST}, FakeRpc(period_spend=self.SPEND))
        #
        self.assertEqual(payload["total"], 2)
        self.assertEqual(payload["counts"], {"team": 2, "personal": 0})
        self.assertEqual([row["name"] for row in payload["rows"]], ["alpha", "project_user_9"])


class TestIdentityIsTheSameInBothListings(unittest.TestCase):
    """Both listings label a project through one helper, so they cannot drift apart."""

    IDENTITY = ("project_id", "name", "display_name", "owner_name", "owner_email", "is_personal")

    def test_a_current_row_and_a_past_row_carry_identical_identity_fields(self):
        past = _rows(_call({"period": LAST}, FakeRpc(period_spend=TestPastMonthIsSpendOnly.SPEND))[0])
        current = _rows(_call({}, FakeRpc(current_limits={1: 10.0, 2: 10.0}))[0])
        #
        for project_id in (1, 2):
            self.assertEqual(
                {field: past[project_id][field] for field in self.IDENTITY},
                {field: current[project_id][field] for field in self.IDENTITY},
            )

    def test_a_personal_project_is_labelled_by_its_owner(self):
        row = _rows(_call({"period": LAST}, FakeRpc(period_spend=TestPastMonthIsSpendOnly.SPEND))[0])[2]
        #
        self.assertTrue(row["is_personal"])
        self.assertEqual(row["display_name"], "ada@example.com")
        self.assertEqual(row["owner_name"], "Ada Lovelace")

    def test_a_team_project_keeps_its_own_name(self):
        row = _rows(_call({"period": LAST}, FakeRpc(period_spend=TestPastMonthIsSpendOnly.SPEND))[0])[1]
        #
        self.assertFalse(row["is_personal"])
        self.assertEqual(row["display_name"], "alpha")


class TestPastMonthCurrency(unittest.TestCase):
    def test_spend_is_reported_in_usd_whatever_the_stored_budget_says(self):
        # The stored currency labels a budget; spend itself is metered in USD
        class Eur(FakeRpc):
            def elitea_core_list_project_budgets(self):
                return {1: {"monthly_limit": 5.0, "enabled": True, "currency": "EUR"}}
        #
        row = _rows(_call({"period": LAST}, Eur(period_spend=TestPastMonthIsSpendOnly.SPEND))[0])[1]
        #
        self.assertEqual(row["currency"], "USD")


class TestSpendAvailability(unittest.TestCase):
    def test_unreadable_spend_is_null_not_zero(self):
        rpc = FakeRpc(period_spend={"available": False, "spend": {}, "earliest_period": None})
        row = _rows(_call({"period": LAST}, rpc)[0])[1]
        #
        self.assertFalse(row["spend_available"])
        self.assertIsNone(row["spend"])

    def test_a_failing_spend_call_is_treated_as_unavailable(self):
        class Broken(FakeRpc):
            def usage_get_projects_period_spend(self, **kwargs):
                raise RuntimeError("usage is down")
        #
        row = _rows(_call({"period": LAST}, Broken())[0])[1]
        self.assertFalse(row["spend_available"])
        self.assertIsNone(row["spend"])


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
