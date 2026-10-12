"""#6980: Tool Spend feature limits ladder, its parity with the LLM ladder, and legacy files untouched.

The feature resolver is a separate copy of the LLM resolution order (the LLM files must stay
byte-identical in this story), so a parity test drives both over the same rows and defaults.
"""
import hashlib
import itertools
import types

import pytest

from unit._tool_spend_loader import PLUGIN_ROOT, load, load_legacy_ladder

flim = load("rpc/feature_budget_limits.py")
legacy = load_legacy_ladder()

# Pinned at branch start (#6980): these files must not change in this story
LEGACY_HASHES = {
    "rpc/budget_limits.py": "fcc8d7ccfcbad92a9436f87a6e64fe8be958e467d723b03e5c36aaafb7987486",
    "rpc/project_budgets.py": "d7663c146f55f84c1d3caff2ef1c1edcf3f3c2e41a55c993c72591464bffa377",
    "utils/budget_door.py": "1cd5e26b7909d91074ebb0595f0a86283eb55d451eb3c5b994b45b3cf75348fd",
}


@pytest.mark.parametrize("relative,digest", sorted(LEGACY_HASHES.items()))
def test_legacy_budget_files_are_byte_identical(relative, digest):
    assert hashlib.sha256((PLUGIN_ROOT / relative).read_bytes()).hexdigest() == digest


class FakeFeatureLadder:
    """Binds the real feature RPC over fake rows and config."""

    def __init__(self, project_row=None, member_row=None, defaults=None, personal=False):
        self.project_row, self.member_row, self.personal = project_row, member_row, personal
        self.descriptor = types.SimpleNamespace(
            config={"cost_budgets": {"features": {"web_search": defaults or {}}}},
        )

    def is_personal_project(self, project_id, **kwargs):  # pylint: disable=W0613
        return self.personal

    def get_project_feature_budget(self, *a, **k):  # pylint: disable=W0613
        return self.project_row

    def get_user_feature_budget(self, *a, **k):  # pylint: disable=W0613
        return self.member_row

    def list_project_feature_budgets(self, *a, **k):  # pylint: disable=W0613
        return {} if self.project_row is None else {3: self.project_row}

    def list_user_feature_budgets(self, *a, **k):  # pylint: disable=W0613
        return [] if self.member_row is None else [dict(self.member_row, user_id=2)]

    get_effective_feature_limits = flim.RPC.get_effective_feature_limits
    get_effective_feature_project_limits = flim.RPC.get_effective_feature_project_limits
    get_effective_feature_member_limits = flim.RPC.get_effective_feature_member_limits


def feature_row(limit=None, member_default=None, enabled=True, feature_enabled=None):
    return {"limit": limit, "member_default_limit": member_default, "enabled": enabled,
            "feature_enabled": feature_enabled}


ON = {"enabled": True}


class TestFeatureLadder:
    def test_unlimited_by_default(self):
        out = FakeFeatureLadder(defaults=ON).get_effective_feature_limits(3, 2, "web_search")
        assert out["project_limit_usd"] is None and out["member_limit_usd"] is None
        assert out["project_source"] == "default" and out["member_source"] == "default"

    def test_explicit_rows_beat_defaults(self):
        ladder = FakeFeatureLadder(feature_row(50.0), {"limit": 5.0, "enabled": True},
                                   dict(ON, project_monthly_limit=10.0, user_monthly_limit=1.0))
        out = ladder.get_effective_feature_limits(3, 2, "web_search")
        assert (out["project_limit_usd"], out["member_limit_usd"]) == (50.0, 5.0)
        assert out["project_source"] == "explicit" and out["member_source"] == "explicit"

    def test_member_default_sits_between_row_and_platform(self):
        ladder = FakeFeatureLadder(feature_row(member_default=3.0), None, dict(ON, user_monthly_limit=1.0))
        assert ladder.get_effective_feature_limits(3, 2, "web_search")["member_limit_usd"] == 3.0

    def test_exempt_project_row_is_unlimited(self):
        ladder = FakeFeatureLadder(feature_row(50.0, enabled=False), defaults=dict(ON, project_monthly_limit=10.0))
        assert ladder.get_effective_feature_limits(3, None, "web_search")["project_limit_usd"] is None

    def test_personal_project_has_no_member_limit_and_own_default(self):
        ladder = FakeFeatureLadder(defaults=dict(ON, personal_project_monthly_limit=7.0, user_monthly_limit=1.0),
                                   personal=True)
        out = ladder.get_effective_feature_limits(3, 2, "web_search")
        assert out["project_limit_usd"] == 7.0 and out["member_limit_usd"] is None

    @pytest.mark.parametrize("defaults,row,personal,expected", [
        ({}, None, False, False),                                   # platform switch off
        (ON, None, False, True),                                    # team default on
        (dict(ON, enabled_team=False), None, False, False),
        (dict(ON, enabled_personal=False), None, True, False),
        (dict(ON, enabled_team=False), feature_row(feature_enabled=True), False, True),   # project override
        (ON, feature_row(feature_enabled=False), False, False),
        ({"enabled": False}, feature_row(feature_enabled=True), False, False),            # switch wins
    ])
    def test_feature_enabled_resolution(self, defaults, row, personal, expected):
        ladder = FakeFeatureLadder(row, defaults=defaults, personal=personal)
        assert ladder.get_effective_feature_limits(3, 2, "web_search")["feature_enabled"] is expected

    def test_llm_and_bad_periods_rejected(self):
        ladder = FakeFeatureLadder(defaults=ON)
        with pytest.raises(ValueError):
            ladder.get_effective_feature_limits(3, 2, "llm")
        with pytest.raises(ValueError):
            ladder.get_effective_feature_limits(3, 2, "web_search", period_kind="day")

    def test_bulk_resolvers_agree_with_single(self):
        ladder = FakeFeatureLadder(feature_row(9.0, member_default=2.0), {"limit": None, "enabled": True},
                                   dict(ON, user_monthly_limit=1.0))
        single = ladder.get_effective_feature_limits(3, 2, "web_search")
        assert ladder.get_effective_feature_project_limits([3], "web_search")[3] == single["project_limit_usd"]
        assert ladder.get_effective_feature_member_limits(3, [2], "web_search")[2] == single["member_limit_usd"]


LIMITS = (None, 0.0, 5.0)
ENABLED = (True, False)


def _project_rows():
    yield None
    for limit, member_default, enabled in itertools.product(LIMITS, LIMITS, ENABLED):
        yield {"limit": limit, "member_default": member_default, "enabled": enabled}


def _member_rows():
    yield None
    for limit, enabled in itertools.product(LIMITS, ENABLED):
        yield {"limit": limit, "enabled": enabled}


class TestParityWithLlmLadder:
    """Same rows and defaults through both ladders must give identical limits."""

    @pytest.mark.parametrize("personal", [False, True])
    @pytest.mark.parametrize("default_project,default_user", [(None, None), (20.0, 2.0)])
    def test_every_row_combination(self, personal, default_project, default_user):
        for project, member in itertools.product(_project_rows(), _member_rows()):
            llm_project = None if project is None else {
                "monthly_limit": project["limit"], "member_default_limit": project["member_default"],
                "enabled": project["enabled"], "currency": "USD"}
            llm_member = None if member is None else {
                "monthly_limit": member["limit"], "enabled": member["enabled"], "currency": "USD"}
            llm = legacy.FakeLadder(
                project_row=llm_project, member_row=llm_member, personal=personal,
                defaults={"enabled": True, "project_monthly_limit": default_project,
                          "personal_project_monthly_limit": default_project, "user_monthly_limit": default_user},
            ).get_effective_budget_limits(3, 2)
            feat = FakeFeatureLadder(
                None if project is None else feature_row(project["limit"], project["member_default"], project["enabled"]),
                member, personal=personal,
                defaults={"enabled": True, "project_monthly_limit": default_project,
                          "personal_project_monthly_limit": default_project, "user_monthly_limit": default_user},
            ).get_effective_feature_limits(3, 2, "web_search")
            assert (feat["project_limit_usd"], feat["member_limit_usd"]) == \
                (llm["project_limit"], llm["member_limit"]), (project, member, personal)
