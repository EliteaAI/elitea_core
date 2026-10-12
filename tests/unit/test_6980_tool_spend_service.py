"""#6980: Tool Spend check/record decisions, permits and the metadata-derived feature list.

The service takes its wiring as `deps`, so every branch here runs the production decision code
against fakes for RPCs (configurations, usage, costs) and module state (toolkit schemas, config).
"""
import datetime
import time

import pytest

from unit._tool_spend_loader import load

features_mod = load("utils/tool_spend_features.py")
permit_mod = load("utils/tool_spend_permit.py")
svc = load("utils/tool_spend.py")

PUBLIC, PROJECT, PERSONAL, OTHER = 1, 3, 30, 99
SHARED_UUID, OWN_UUID, FOREIGN_UUID = "11111111-0000-0000-0000-000000000001", \
    "22222222-0000-0000-0000-000000000002", "33333333-0000-0000-0000-000000000003"
NOW = datetime.datetime(2026, 10, 9, 12, 0, tzinfo=datetime.timezone.utc)


def schema(feature="web_search", backend="tavily", cfg_type="tavily", caps=None, **extra):
    spend = {"feature": feature, "label": "Web search", "backend": backend, "configuration_type": cfg_type,
             "chat_tools": ["web_search", "web_extract"],
             "max_units_per_call": caps or {"search": 2, "extract": 2, "crawl": 12, "map": 10}}
    spend.update(extra)
    return {"title": backend, "metadata": {"label": backend, "tool_spend": spend}}


class Deps:
    def __init__(self, schemas=None, config=None, mode="enforce", limits=None, spend=None,
                 limits_raise=False, configs=None):
        self.schemas = {"tavily": schema()} if schemas is None else schemas
        self.cfg = config if config is not None else {"cost_budgets": {"features": {"web_search": {"enabled": True}}}}
        self._mode, self._limits, self._spend, self.limits_raise = mode, limits, spend, limits_raise
        self.configs = configs if configs is not None else {
            SHARED_UUID: {"uuid": SHARED_UUID, "project_id": PUBLIC, "type": "tavily", "shared": True},
            OWN_UUID: {"uuid": OWN_UUID, "project_id": PROJECT, "type": "tavily", "shared": False},
            FOREIGN_UUID: {"uuid": FOREIGN_UUID, "project_id": OTHER, "type": "tavily", "shared": False},
        }
        self.notified, self.ledgered, self.priced = [], [], []

    def features(self):
        return features_mod.build_feature_map(self.schemas)

    def config(self):
        return self.cfg

    def configuration(self, project_id, uuid):
        cfg = self.configs.get(uuid)
        return cfg if cfg and cfg["project_id"] == project_id else None

    def public_project_id(self):
        return PUBLIC

    def personal_project_id(self, user_id):  # pylint: disable=W0613
        return PERSONAL

    def hmac_key(self):
        return "test-event-node-key"

    def mode(self):
        return self._mode

    def limits(self, project_id, user_id, feature):  # pylint: disable=W0613
        if self.limits_raise:
            raise RuntimeError("db down")
        return self._limits or {"project_limit_usd": None, "member_limit_usd": None, "feature_enabled": True}

    def spend(self, project_id, user_id, feature):  # pylint: disable=W0613
        return self._spend or {"available": True, "project_spent_nano": 0, "member_spent_nano": 0}

    def limit_reached(self, *args):
        self.notified.append(args)

    def price(self, backend, units, unit):
        self.priced.append((backend, units, unit))
        return {"cost": units * 0.008, "cost_source": "estimated:costs-tool-config"}

    def ledger(self, record, attribution):
        self.ledgered.append((record, attribution))
        return {"recorded": True, "duplicate": False}


def body(uuid=SHARED_UUID, owner=PUBLIC, op="search", **extra):
    return dict({"feature": "web_search", "backend": "tavily", "op": op,
                 "configuration_uuid": uuid, "configuration_project_id": owner}, **extra)


def check(deps, payload=None, user_id=7):
    return svc.ToolSpendService(deps).check(PROJECT, user_id, payload or body(), run_id="run-1", now=NOW)


def status_of(callable_):
    with pytest.raises(svc.ToolSpendError) as info:
        callable_()
    return info.value.status


class TestFeatureMap:
    def test_built_from_metadata(self):
        fmap = features_mod.build_feature_map({"tavily": schema(), "github": {"metadata": {"label": "x"}}})
        assert set(fmap) == {"web_search"}
        assert fmap["web_search"]["backends"]["tavily"]["max_units_per_call"]["crawl"] == 12
        assert fmap["web_search"]["backends"]["tavily"]["unit"] == "credit"

    def test_llm_and_malformed_entries_are_dropped(self):
        fmap = features_mod.build_feature_map({
            "a": schema(feature="llm"), "b": schema(backend="b", caps={"search": "x"}),
            "c": {"metadata": {"tool_spend": {"feature": "x"}}},
        })
        assert fmap == {}

    def test_cache_follows_snapshot_identity(self):
        first = {"tavily": schema()}
        assert features_mod.feature_map(first) is features_mod.feature_map(first)
        second = {"tavily": schema(), "brave": schema(backend="brave", cfg_type="brave")}
        assert set(features_mod.feature_map(second)["web_search"]["backends"]) == {"tavily", "brave"}


class TestCheck:
    def test_empty_feature_list_is_503(self):
        assert status_of(lambda: check(Deps(schemas={}))) == 503

    @pytest.mark.parametrize("payload,status", [
        (body(op="research"), 400),
        (dict(body(), feature="web_scrape"), 400),
        (dict(body(), backend="brave"), 400),
        (body(uuid="44444444-0000-0000-0000-000000000004"), 400),
        (dict(body(), configuration_project_id=None), 400),
        (body(uuid=FOREIGN_UUID, owner=OTHER), 403),
    ])
    def test_bad_requests(self, payload, status):
        assert status_of(lambda: check(Deps(), payload)) == status

    def test_wrong_configuration_type_is_400(self):
        deps = Deps()
        deps.configs[SHARED_UUID]["type"] = "github"
        assert status_of(lambda: check(deps)) == 400

    def test_own_credential_is_exempt_with_a_permit_even_when_disabled(self):
        deps = Deps(config={}, limits_raise=True)
        out = check(deps, body(uuid=OWN_UUID, owner=PROJECT))
        assert out["allowed"] and out["exempt"] and out["permit"]

    def test_personal_project_credential_is_own(self):
        deps = Deps()
        deps.configs[OWN_UUID]["project_id"] = PERSONAL
        assert check(deps, body(uuid=OWN_UUID, owner=PERSONAL))["exempt"] is True

    def test_shared_allowed_under_limit(self):
        out = check(Deps(limits={"project_limit_usd": 1.0, "member_limit_usd": 1.0, "feature_enabled": True},
                         spend={"available": True, "project_spent_nano": 10, "member_spent_nano": 10}))
        assert out["allowed"] and out["exempt"] is False

    def test_platform_switch_off_denies_shared(self):
        out = check(Deps(config={}))
        assert out["code"] == svc.CODE_DISABLED and not out["allowed"]

    def test_project_disabled_denies_shared(self):
        out = check(Deps(limits={"feature_enabled": False}))
        assert out["code"] == svc.CODE_DISABLED

    @pytest.mark.parametrize("mode", ["off", "observe"])
    def test_off_and_observe_never_deny_on_limits(self, mode):
        deps = Deps(mode=mode, limits={"project_limit_usd": 0.0, "feature_enabled": True},
                    spend={"available": True, "project_spent_nano": 10**12, "member_spent_nano": 0})
        assert check(deps)["allowed"] is True

    def test_project_denied_before_member(self):
        deps = Deps(limits={"project_limit_usd": 1.0, "member_limit_usd": 0.5, "feature_enabled": True},
                    spend={"available": True, "project_spent_nano": 10**9, "member_spent_nano": 10**9})
        out = check(deps)
        assert out["code"] == svc.CODE_LIMIT and out["scope"] == "project"
        assert out["resets_at"].startswith("2026-11-01")
        assert deps.notified and deps.notified[0][1] is None

    def test_member_limit(self):
        deps = Deps(limits={"project_limit_usd": None, "member_limit_usd": 0.5, "feature_enabled": True},
                    spend={"available": True, "project_spent_nano": 0, "member_spent_nano": 5 * 10**8})
        out = check(deps)
        assert out["scope"] == "member" and deps.notified[0][1] == 7

    def test_unavailable_spend_denies_in_enforce_without_notification(self):
        deps = Deps(spend={"available": False})
        out = check(deps)
        assert out["code"] == svc.CODE_UNAVAILABLE and deps.notified == []

    def test_failed_ladder_denies_in_enforce(self):
        assert check(Deps(limits_raise=True))["code"] == svc.CODE_UNAVAILABLE

    def test_unavailable_accounting_allows_with_warning_in_observe(self):
        out = check(Deps(mode="observe", spend={"available": False}))
        assert out["allowed"] and out["warning"] == svc.CODE_UNAVAILABLE

    def test_no_denial_text_contains_the_run_abort_token(self):
        outs = [
            check(Deps(config={})), check(Deps(spend={"available": False})),
            check(Deps(limits={"project_limit_usd": 0.0, "feature_enabled": True})),
            check(Deps(limits={"feature_enabled": False})),
        ]
        for out in outs:
            assert "budget_exceeded" not in str(out)


def allowed_permit(deps, **over):
    out = check(deps, body(**over))
    assert out["allowed"]
    return out["permit"]


def record(deps, permit, user_id=7, **over):
    payload = dict(body(), permit=permit, units=1, ok=True, tool_name="web_search", duration_ms=10)
    payload.update(over)
    return svc.ToolSpendService(deps).record(PROJECT, user_id, payload, attribution="hdr")


class TestRecordAndPermit:
    def test_row_uses_permit_iat_nonce_and_exempt(self):
        deps = Deps()
        permit = allowed_permit(deps)
        claims = permit_mod.verify_permit(permit_mod.permit_key(deps.hmac_key()), permit, {})
        out = record(deps, permit)
        row, attribution = deps.ledgered[0]
        assert out["recorded"] and attribution == "hdr"
        assert row["idempotency_key"] == f"tool_cost:{claims['nonce']}"
        assert row["ts"] == datetime.datetime.fromtimestamp(claims["iat"], datetime.timezone.utc)
        assert row["budget_exempt"] is False and row["cost_usd"] == pytest.approx(0.008)

    def test_replay_sends_the_same_row_key(self):
        deps = Deps()
        permit = allowed_permit(deps)
        record(deps, permit)
        record(deps, permit)
        assert deps.ledgered[0][0]["idempotency_key"] == deps.ledgered[1][0]["idempotency_key"]
        assert deps.ledgered[0][0]["ts"] == deps.ledgered[1][0]["ts"]

    def test_exempt_comes_from_the_permit(self):
        deps = Deps()
        permit = allowed_permit(deps, uuid=OWN_UUID, owner=PROJECT)
        record(deps, permit, configuration_uuid=OWN_UUID)
        assert deps.ledgered[0][0]["budget_exempt"] is True

    def test_failed_call_records_zero_units(self):
        deps = Deps()
        record(deps, allowed_permit(deps), ok=False, units=0)
        assert deps.ledgered[0][0]["ok"] is False and deps.ledgered[0][0]["units"] == 0

    def test_forged_signature(self):
        deps = Deps()
        payload, _mac = allowed_permit(deps).split(".")
        assert status_of(lambda: record(deps, payload + ".AAAA")) == 403

    def test_missing_permit(self):
        assert status_of(lambda: record(Deps(), None)) == 403

    def test_expired(self):
        deps = Deps()
        permit = allowed_permit(deps)
        later = time.time() + permit_mod.PERMIT_TTL_SECONDS + 1
        with pytest.raises(svc.ToolSpendError) as info:
            svc.ToolSpendService(deps).record(
                PROJECT, 7, dict(body(), permit=permit, units=1), now=later)
        assert info.value.status == 403

    @pytest.mark.parametrize("mismatch", [
        {"op": "extract"}, {"configuration_uuid": OWN_UUID}, {"feature": "web_search2"},
    ])
    def test_mismatched_claims(self, mismatch):
        deps = Deps(schemas={"tavily": schema(), "t2": schema(feature="web_search2")})
        permit = allowed_permit(deps)
        assert status_of(lambda: record(deps, permit, **mismatch)) == 403

    def test_other_user_or_project(self):
        deps = Deps()
        permit = allowed_permit(deps)
        assert status_of(lambda: record(deps, permit, user_id=8)) == 403
        with pytest.raises(svc.ToolSpendError) as info:
            svc.ToolSpendService(deps).record(OTHER, 7, dict(body(), permit=permit, units=1))
        assert info.value.status == 403

    def test_units_over_max_units_is_422_and_nothing_recorded(self):
        deps = Deps()
        assert status_of(lambda: record(deps, allowed_permit(deps), units=3)) == 422
        assert deps.ledgered == []

    def test_max_units_is_the_metadata_ceiling_per_op(self):
        deps = Deps()
        permit = allowed_permit(deps, op="crawl")
        claims = permit_mod.verify_permit(permit_mod.permit_key(deps.hmac_key()), permit, {})
        assert claims["max_units"] == 12 and claims["run_id"] == "run-1"

    def test_negative_units(self):
        deps = Deps()
        assert status_of(lambda: record(deps, allowed_permit(deps), units=-1)) == 400

    def test_permit_key_required(self):
        with pytest.raises(permit_mod.PermitError):
            permit_mod.permit_key("")


class TestSecondBackendByMetadataOnly:
    """AC10: a new backend needs only toolkit metadata (and config), no core code."""

    def test_fake_backend_gets_check_record_and_permit(self):
        deps = Deps(schemas={"tavily": schema(), "fakeprov": schema(
            backend="fakeprov", cfg_type="fakeprov", caps={"query": 1}, unit="request")})
        deps.configs["55555555-0000-0000-0000-000000000005"] = {
            "uuid": "55555555-0000-0000-0000-000000000005", "project_id": PUBLIC, "type": "fakeprov", "shared": True}
        payload = dict(body(uuid="55555555-0000-0000-0000-000000000005", op="query"), backend="fakeprov")
        out = svc.ToolSpendService(deps).check(PROJECT, 7, payload, now=NOW)
        assert out["allowed"]
        svc.ToolSpendService(deps).record(PROJECT, 7, dict(payload, permit=out["permit"], units=1))
        assert deps.priced == [("fakeprov", 1.0, "request")]
        assert status_of(lambda: svc.ToolSpendService(deps).record(
            PROJECT, 7, dict(payload, permit=out["permit"], units=2))) == 422
