"""Unit tests for the canonical effective-limit ladder (rpc/budget_limits.py).

This ladder is the single place both inference planes resolve money limits through, so the
cases here are the contract: an explicit row beats a default, a disabled row means
"deliberately exempt", a project's member default sits between a member's own row and the
platform default, and a personal project has no member limit at all.

The bulk resolvers are exercised over the same fakes as the single-project one, so a change
to one that is not mirrored in the other shows up as a disagreement rather than passing quietly.

Run standalone: python3 tests/unit/test_6573_budget_limits_ladder.py
"""

import os
import sys
import types
import unittest


def _load_module():
    """Load rpc/budget_limits.py with the pylon/tools imports stubbed out."""
    plugin_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    #
    web_stub = types.SimpleNamespace(
        rpc=lambda *a, **kw: (lambda func: func),
        method=lambda *a, **kw: (lambda func: func),
    )
    log_stub = types.SimpleNamespace(
        exception=lambda *a, **kw: None,
        warning=lambda *a, **kw: None,
        info=lambda *a, **kw: None,
    )
    #
    pylon_pkg = types.ModuleType("pylon")
    pylon_core = types.ModuleType("pylon.core")
    pylon_tools = types.ModuleType("pylon.core.tools")
    pylon_tools.web = web_stub
    pylon_tools.log = log_stub
    #
    tools_stub = types.ModuleType("tools")
    tools_stub.context = types.SimpleNamespace(rpc_manager=None)
    #
    saved = {
        name: sys.modules.get(name)
        for name in ("pylon", "pylon.core", "pylon.core.tools", "tools")
    }
    sys.modules["pylon"] = pylon_pkg
    sys.modules["pylon.core"] = pylon_core
    sys.modules["pylon.core.tools"] = pylon_tools
    sys.modules["tools"] = tools_stub
    #
    try:
        import importlib.util  # pylint: disable=C0415
        #
        spec = importlib.util.spec_from_file_location(
            "budget_limits_under_test",
            os.path.join(plugin_root, "rpc", "budget_limits.py"),
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        #
        return module
    finally:
        for name, value in saved.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value


limits_mod = _load_module()
RPC = limits_mod.RPC
UNSET = limits_mod._UNSET  # pylint: disable=W0212

PLATFORM_DEFAULTS = {"enabled": True, "user_monthly_limit": 100.0}


class FakeLadder:
    """Binds the real resolvers over fake stored rows and fake plugin config.

    Production code, not a restatement of it: the tiers all have to be consulted in the
    right order for a member to end up capped by what an admin actually set.
    """

    def __init__(  # pylint: disable=R0913,R0917
            self, project_row=None, member_row=None, defaults=None,
            personal=False, all_projects=None, list_fails=False,
    ):
        self.project_row = project_row
        self.member_row = member_row
        self.personal = personal
        self.all_projects = all_projects or {}
        self.list_fails = list_fails
        self.get_project_calls = 0
        self.get_user_calls = 0
        self.list_calls = 0
        self.list_user_calls = 0
        self.descriptor = types.SimpleNamespace(
            config={"cost_budgets": {"defaults": defaults or {}}},
        )

    # Storage proxies the ladder reads through
    def get_project_budget(self, project_id):  # pylint: disable=W0613
        self.get_project_calls += 1
        return self.project_row

    def get_user_budget(self, project_id, user_id):  # pylint: disable=W0613
        self.get_user_calls += 1
        return self.member_row

    def list_project_budgets(self):
        self.list_calls += 1
        #
        if self.list_fails:
            raise RuntimeError("storage unavailable")
        #
        return dict(self.all_projects)

    def list_user_budgets(self, project_id=None):  # pylint: disable=W0613
        self.list_user_calls += 1
        #
        if self.member_row is None:
            return []
        #
        return [dict(self.member_row, user_id=2)]

    def is_personal_project(self, project_id, **kwargs):  # pylint: disable=W0613
        return self.personal

    get_budget_default_limit = RPC.get_budget_default_limit
    get_effective_project_limit = RPC.get_effective_project_limit
    get_effective_member_default = RPC.get_effective_member_default
    get_effective_member_limit = RPC.get_effective_member_limit
    get_effective_budget_limits = RPC.get_effective_budget_limits
    get_effective_project_limits = RPC.get_effective_project_limits
    get_effective_member_limits = RPC.get_effective_member_limits
    _read_project_budget = RPC._read_project_budget  # pylint: disable=W0212
    _read_user_budget = RPC._read_user_budget  # pylint: disable=W0212
    _read_user_budgets = RPC._read_user_budgets  # pylint: disable=W0212


class TestDefaultLimits(unittest.TestCase):
    """Defaults apply only where nothing was set explicitly, and only when enabled."""

    def test_defaults_off_means_no_default(self):
        ladder = FakeLadder(defaults={"enabled": False, "project_monthly_limit": 50.0})
        self.assertIsNone(ladder.get_budget_default_limit("project", 1))

    def test_project_default_used_for_team_projects(self):
        ladder = FakeLadder(defaults={"enabled": True, "project_monthly_limit": 50.0})
        self.assertEqual(ladder.get_budget_default_limit("project", 1), 50.0)

    def test_personal_projects_have_their_own_default(self):
        ladder = FakeLadder(
            defaults={
                "enabled": True,
                "project_monthly_limit": 50.0,
                "personal_project_monthly_limit": 10.0,
            },
            personal=True,
        )
        self.assertEqual(ladder.get_budget_default_limit("project", 1), 10.0)

    def test_user_scope_short_circuits_the_personal_lookup(self):
        ladder = FakeLadder(defaults=PLATFORM_DEFAULTS, personal=True)
        self.assertEqual(ladder.get_budget_default_limit("user", 1), 100.0)

    def test_missing_key_means_unlimited_not_zero(self):
        ladder = FakeLadder(defaults={"enabled": True})
        self.assertIsNone(ladder.get_budget_default_limit("project", 1))


class TestProjectLimitResolution(unittest.TestCase):
    DEFAULTS = {"enabled": True, "project_monthly_limit": 100.0}

    def test_explicit_row_wins_over_default(self):
        ladder = FakeLadder({"monthly_limit": 7.0, "enabled": True}, defaults=self.DEFAULTS)
        self.assertEqual(ladder.get_effective_project_limit(1), 7.0)

    def test_no_row_falls_back_to_default(self):
        self.assertEqual(FakeLadder(None, defaults=self.DEFAULTS).get_effective_project_limit(1), 100.0)

    def test_disabled_row_is_deliberately_exempt(self):
        # An admin disabling a budget must not be silently re-capped by the default
        ladder = FakeLadder({"monthly_limit": 7.0, "enabled": False}, defaults=self.DEFAULTS)
        self.assertIsNone(ladder.get_effective_project_limit(1))

    def test_row_without_limit_falls_back_to_default(self):
        ladder = FakeLadder({"monthly_limit": None, "enabled": True}, defaults=self.DEFAULTS)
        self.assertEqual(ladder.get_effective_project_limit(1), 100.0)

    def test_zero_explicit_limit_is_respected_not_treated_as_missing(self):
        ladder = FakeLadder({"monthly_limit": 0.0, "enabled": True}, defaults=self.DEFAULTS)
        self.assertEqual(ladder.get_effective_project_limit(1), 0.0)

    def test_an_unreadable_row_does_not_read_as_a_zero_ceiling(self):
        ladder = FakeLadder(defaults=self.DEFAULTS)
        ladder.get_project_budget = lambda project_id: (_ for _ in ()).throw(RuntimeError("db"))
        self.assertEqual(ladder.get_effective_project_limit(1), 100.0)


class TestBulkProjectLimits(unittest.TestCase):
    """The admin pages list whole environments, so limits are read in one query.

    Any divergence from the single-project resolver would change the limit shown for every
    project, so each case asserts the two agree.
    """

    DEFAULTS = {"enabled": True, "project_monthly_limit": 100.0}

    def _both(self, rows, project_id):
        bulk = FakeLadder(all_projects=rows, defaults=self.DEFAULTS)
        single = FakeLadder(rows.get(project_id), defaults=self.DEFAULTS)
        #
        return (
            bulk.get_effective_project_limits([project_id])[project_id],
            single.get_effective_project_limit(project_id),
        )

    def test_explicit_row_agrees_with_the_single_resolver(self):
        bulk, single = self._both({1: {"monthly_limit": 7.0, "enabled": True}}, 1)
        self.assertEqual((bulk, single), (7.0, 7.0))

    def test_missing_row_falls_through_to_the_default(self):
        bulk, single = self._both({}, 1)
        self.assertEqual((bulk, single), (100.0, 100.0))

    def test_disabled_row_agrees_on_unlimited(self):
        bulk, single = self._both({1: {"monthly_limit": 7.0, "enabled": False}}, 1)
        self.assertEqual((bulk, single), (None, None))

    def test_string_keyed_rows_are_still_matched(self):
        ladder = FakeLadder(
            all_projects={"1": {"monthly_limit": 7.0, "enabled": True}}, defaults=self.DEFAULTS,
        )
        self.assertEqual(ladder.get_effective_project_limits([1]), {1: 7.0})

    def test_every_requested_id_is_present_in_the_result(self):
        ladder = FakeLadder(all_projects={}, defaults=self.DEFAULTS)
        self.assertEqual(sorted(ladder.get_effective_project_limits([1, 2, 3])), [1, 2, 3])

    def test_one_query_regardless_of_how_many_projects(self):
        ladder = FakeLadder(all_projects={}, defaults=self.DEFAULTS)
        ladder.get_effective_project_limits([1, 2, 3, 4])
        self.assertEqual(ladder.list_calls, 1)

    def test_a_failed_read_reports_unlimited_rather_than_raising(self):
        # The budgets page must still render; a limit that cannot be read is not enforced
        ladder = FakeLadder(defaults=self.DEFAULTS, list_fails=True)
        self.assertEqual(ladder.get_effective_project_limits([1, 2]), {1: None, 2: None})


class TestMemberDefaultTier(unittest.TestCase):
    """A project's member default sits between a member's own row and the platform default.

    It is what "set a limit for everyone in this project" resolves to, so it must apply to
    members with no row of their own while leaving members who have one untouched.
    """

    def _resolve(self, member_row, project_row, defaults=None, project_budget=UNSET):
        ladder = FakeLadder(project_row, member_row, defaults or PLATFORM_DEFAULTS)
        #
        return ladder, ladder.get_effective_member_limit(1, 2, project_budget=project_budget)

    def test_explicit_member_row_beats_the_project_default(self):
        _, limit = self._resolve(
            {"monthly_limit": 7.0, "enabled": True}, {"member_default_limit": 20.0},
        )
        self.assertEqual(limit, 7.0)

    def test_project_default_beats_the_platform_default(self):
        _, limit = self._resolve(None, {"member_default_limit": 20.0})
        self.assertEqual(limit, 20.0)

    def test_no_project_default_falls_through_to_the_platform_default(self):
        _, limit = self._resolve(None, {"member_default_limit": None})
        self.assertEqual(limit, 100.0)

    def test_no_project_row_at_all_falls_through(self):
        _, limit = self._resolve(None, None)
        self.assertEqual(limit, 100.0)

    def test_member_row_without_a_limit_picks_up_the_project_default(self):
        _, limit = self._resolve(
            {"monthly_limit": None, "enabled": True}, {"member_default_limit": 20.0},
        )
        self.assertEqual(limit, 20.0)

    def test_project_default_overrides_a_member_marked_unlimited(self):
        # A limit an admin set for everyone in the project must not be undone by a member
        # row nobody meant to opt out — that row is often just the dialog's default state
        _, limit = self._resolve(
            {"monthly_limit": 7.0, "enabled": False}, {"member_default_limit": 20.0},
        )
        self.assertEqual(limit, 20.0)

    def test_exempt_member_still_escapes_the_platform_default(self):
        # With no project default there is nothing project-scoped to enforce, so the
        # exemption keeps its original meaning
        _, limit = self._resolve({"monthly_limit": 7.0, "enabled": False}, {})
        self.assertIsNone(limit)

    def test_exempt_member_with_no_project_row_is_unlimited(self):
        _, limit = self._resolve({"monthly_limit": 7.0, "enabled": False}, None)
        self.assertIsNone(limit)

    def test_zero_project_default_blocks_rather_than_falling_through(self):
        _, limit = self._resolve(None, {"member_default_limit": 0.0})
        self.assertEqual(limit, 0.0)

    def test_project_marked_unlimited_still_applies_its_member_default(self):
        # enabled=false exempts the project's OWN limit; the member default is a separate value
        _, limit = self._resolve(
            None, {"enabled": False, "monthly_limit": None, "member_default_limit": 20.0},
        )
        self.assertEqual(limit, 20.0)

    def test_project_default_applies_even_when_platform_defaults_are_off(self):
        _, limit = self._resolve(None, {"member_default_limit": 20.0}, {"enabled": False})
        self.assertEqual(limit, 20.0)

    def test_unlimited_when_neither_tier_has_a_value(self):
        _, limit = self._resolve(None, {}, {"enabled": False})
        self.assertIsNone(limit)

    def test_a_passed_project_row_is_not_re_read(self):
        # The member list loops every member, so the project row must be read once, not per row
        ladder, limit = self._resolve(
            None, None, project_budget={"member_default_limit": 20.0},
        )
        self.assertEqual(limit, 20.0)
        self.assertEqual(ladder.get_project_calls, 0)

    def test_row_is_read_when_the_caller_passes_nothing(self):
        ladder, limit = self._resolve(None, {"member_default_limit": 20.0})
        self.assertEqual(limit, 20.0)
        self.assertEqual(ladder.get_project_calls, 1)

    def test_passing_none_explicitly_means_no_project_row(self):
        ladder, limit = self._resolve(None, {"member_default_limit": 20.0}, project_budget=None)
        self.assertEqual(limit, 100.0)
        self.assertEqual(ladder.get_project_calls, 0)


class TestBulkMemberLimits(unittest.TestCase):
    """The member list can be a whole project, so the project row is read once for all of them."""

    def test_every_member_resolves_and_the_row_is_read_once(self):
        ladder = FakeLadder({"member_default_limit": 20.0}, None, PLATFORM_DEFAULTS)
        self.assertEqual(ladder.get_effective_member_limits(1, [2, 3, 4]), {2: 20.0, 3: 20.0, 4: 20.0})
        self.assertEqual(ladder.get_project_calls, 1)
        # One query for every member row, not one per member
        self.assertEqual(ladder.list_user_calls, 1)
        self.assertEqual(ladder.get_user_calls, 0)

    def test_bulk_agrees_with_the_single_member_resolver(self):
        rows = ({"monthly_limit": 7.0, "enabled": True}, {"member_default_limit": 20.0})
        bulk = FakeLadder(rows[1], rows[0], PLATFORM_DEFAULTS)
        single = FakeLadder(rows[1], rows[0], PLATFORM_DEFAULTS)
        #
        self.assertEqual(
            bulk.get_effective_member_limits(1, [2])[2],
            single.get_effective_member_limit(1, 2),
        )


class TestPersonalProjectHasNoMemberLimit(unittest.TestCase):
    """A personal project's one member is its owner, so its project budget IS their budget.

    A member limit there is a second ceiling on the same person. It also enforced while being
    invisible: the Usage page shows only the project scope for a personal project, so users
    were blocked at a platform default of $20 while the page reported 45% of $300 remaining.
    """

    def _resolve(self, member_row, project_row):
        ladder = FakeLadder(project_row, member_row, PLATFORM_DEFAULTS, personal=True)
        return ladder.get_effective_member_limit(1, 2)

    def test_platform_default_does_not_apply(self):
        # The reported bug: no stored limit anywhere, blocked by the inherited default
        self.assertIsNone(self._resolve(None, None))

    def test_explicit_member_row_does_not_apply(self):
        self.assertIsNone(self._resolve({"monthly_limit": 7.0, "enabled": True}, None))

    def test_project_member_default_does_not_apply(self):
        self.assertIsNone(self._resolve(None, {"member_default_limit": 20.0}))

    def test_a_team_project_still_resolves_its_member_limit(self):
        ladder = FakeLadder(None, {"monthly_limit": 7.0, "enabled": True}, PLATFORM_DEFAULTS)
        self.assertEqual(ladder.get_effective_member_limit(1, 2), 7.0)


class TestCombinedEntryPoint(unittest.TestCase):
    """What the gate calls: both scopes, in micro-USD, from one read of the project row."""

    def test_both_scopes_in_micro_usd(self):
        ladder = FakeLadder(
            {"monthly_limit": 7.5, "enabled": True, "member_default_limit": 2.0},
            None, PLATFORM_DEFAULTS,
        )
        result = ladder.get_effective_budget_limits(1, 2)
        #
        self.assertEqual(result["project_limit_micro"], 7_500_000)
        self.assertEqual(result["member_limit_micro"], 2_000_000)

    def test_unlimited_stays_none_and_never_becomes_zero(self):
        ladder = FakeLadder(None, None, {"enabled": False})
        result = ladder.get_effective_budget_limits(1, 2)
        #
        self.assertIsNone(result["project_limit_micro"])
        self.assertIsNone(result["member_limit_micro"])

    def test_zero_limit_survives_the_micro_conversion(self):
        ladder = FakeLadder({"monthly_limit": 0.0, "enabled": True}, None, {"enabled": False})
        self.assertEqual(ladder.get_effective_budget_limits(1)["project_limit_micro"], 0)

    def test_no_user_means_no_member_limit(self):
        ladder = FakeLadder(None, {"monthly_limit": 7.0, "enabled": True}, PLATFORM_DEFAULTS)
        self.assertIsNone(ladder.get_effective_budget_limits(1)["member_limit_micro"])

    def test_enabled_is_true_when_any_scope_is_limited(self):
        # The gate short-circuits on this flag, so its absence silently disables enforcement
        ladder = FakeLadder({"monthly_limit": 7.5, "enabled": True}, None, {"enabled": False})
        self.assertTrue(ladder.get_effective_budget_limits(1, 2)["enabled"])

    def test_enabled_is_true_when_only_the_member_scope_is_limited(self):
        ladder = FakeLadder(None, {"monthly_limit": 3.0, "enabled": True}, {"enabled": False})
        self.assertTrue(ladder.get_effective_budget_limits(1, 2)["enabled"])

    def test_enabled_is_false_when_nothing_limits_the_caller(self):
        ladder = FakeLadder(None, None, {"enabled": False})
        self.assertFalse(ladder.get_effective_budget_limits(1, 2)["enabled"])

    def test_the_project_row_is_read_once_for_both_scopes(self):
        ladder = FakeLadder({"member_default_limit": 20.0}, None, PLATFORM_DEFAULTS)
        ladder.get_effective_budget_limits(1, 2)
        self.assertEqual(ladder.get_project_calls, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
