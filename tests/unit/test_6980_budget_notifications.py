"""#6980: feature-aware budget notifications must not break the existing admin lookup or LLM alerts.

Regression: a polish edit once deleted _is_admin_role, so get_project_admin_ids raised NameError
and every budget alert (LLM ones too) was dropped. The live e2e found it, not the unit tests.
"""
import sys
import types

import pytest

from unit._tool_spend_loader import load

# Other suites leave stubs under these names; load the real enum fresh so the order does not matter
for _package in ("plugins.elitea_core.models.enums",):
    sys.modules.setdefault(_package, types.ModuleType(_package)).__path__ = []
load("models/enums/all.py", fresh=True)
notif = load("rpc/budget_notifications.py", fresh=True)


class FakeRpc:
    def __init__(self, roles):
        self.roles, self.fired = roles, []

    def timeout(self, _t):
        return types.SimpleNamespace(
            admin_get_users_roles_in_project=lambda *a, **k: self.roles,
            project_get_by_id=lambda pid: {"name": "Team A", "owner_id": 9},
        )


def module(roles):
    fake = FakeRpc(roles)
    instance = types.SimpleNamespace(
        context=types.SimpleNamespace(
            rpc_manager=fake,
            event_manager=types.SimpleNamespace(fire_event=lambda name, payload: fake.fired.append(payload)),
        ),
    )
    instance.get_project_admin_ids = lambda pid, **k: notif.RPC.get_project_admin_ids(instance, pid)
    instance.notify = lambda *a, **k: notif.RPC.notify_budget_event(instance, *a, **k)
    return instance, fake


def test_admin_lookup_matches_admin_roles_only():
    instance, _ = module({3: ["admin"], 4: ["editor"], 5: ["Project_ADMIN"], 6: []})
    assert sorted(instance.get_project_admin_ids(1)) == [3, 5]


def test_llm_limit_alert_still_reaches_the_admins():
    instance, fake = module({3: ["admin"]})
    assert instance.notify(1, "limit", pct=100) == 1
    assert fake.fired[0]["event_type"] == notif.NotificationEventTypes.budget_limit_reached


def test_feature_limit_alert_uses_feature_type_and_text():
    instance, fake = module({3: ["admin"]})
    assert instance.notify(1, "limit", pct=100, feature="web_search", feature_label="Web search",
                           period_kind="month", limit_usd=0.05) == 1
    sent = fake.fired[0]
    assert sent["event_type"] == notif.NotificationEventTypes.tool_spend_limit_reached
    assert sent["meta"]["feature"] == "web_search"
    assert "$0.05" in sent["meta"]["message"] and "Team A" in sent["meta"]["message"]


def test_member_feature_alert_goes_to_that_member_only():
    instance, fake = module({3: ["admin"]})
    assert instance.notify(1, "threshold", pct=85, user_id=7, feature="web_search") == 1
    assert fake.fired[0]["user_id"] == 7
    assert fake.fired[0]["event_type"] == notif.NotificationEventTypes.member_tool_spend_threshold_reached


@pytest.mark.parametrize("value,text", [(0.05, "$0.05"), (0.005, "$0.005"), (12, "$12.00"), (0.0001, "$0.0001")])
def test_sub_cent_limits_keep_their_digits(value, text):
    assert notif.usd(value) == text
