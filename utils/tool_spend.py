""" Tool Spend check/record logic (#6980); the API modules are thin wrappers over this """

import datetime

from pylon.core.tools import log  # pylint: disable=E0611,E0401

from pydantic import ValidationError

from ..models.pd.tool_spend import ToolSpendCheckBody, ToolSpendRecordBody
from .tool_spend_features import backend_spec, feature_defaults
from .tool_spend_permit import PermitError, issue_permit, permit_key, verify_permit, verify_record_signature

NANO = 1_000_000_000
MODE_OFF, MODE_OBSERVE, MODE_ENFORCE = "off", "observe", "enforce"
CODE_LIMIT = "tool_spend_limit_reached"
CODE_DISABLED = "tool_spend_feature_disabled"
CODE_UNAVAILABLE = "tool_spend_unavailable"


class ToolSpendError(Exception):
    """An HTTP error answer: (body, status)."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


def _next_period_start(now, period_kind="month"):
    first = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return (first + datetime.timedelta(days=32)).replace(day=1)


def _parse(model, body):
    try:
        return model.model_validate(body).model_dump()
    except ValidationError as exc:
        fields = sorted({".".join(str(part) for part in error["loc"]) for error in exc.errors()})
        raise ToolSpendError(400, f"Invalid request fields: {', '.join(fields)}") from exc


def _deny(code, message, **extra):
    return dict({"allowed": False, "exempt": False, "period_kind": "month", "code": code, "message": message}, **extra)


def _limit_message(label, scope, limit_usd, resets_at):
    who = "this project" if scope == "project" else "you in this project"
    amount = f"${limit_usd:.2f}" if limit_usd >= 0.01 else f"${limit_usd:.4f}".rstrip("0")
    return (f"{label} monthly limit reached for {who} ({amount}). "
            f"Resets {resets_at:%Y-%m-%d %H:%M} UTC. Answer without {label.lower()}.")


class ToolSpendService:
    """Wiring is injected (deps) so the decision logic is unit-testable without pylon."""

    def __init__(self, deps):
        self.deps = deps

    def _spec(self, feature, backend, op):
        features = self.deps.features()
        if not features:
            raise ToolSpendError(503, "Tool spend features are not loaded yet, retry shortly")
        spec = backend_spec(features, feature, backend)
        if spec is None:
            raise ToolSpendError(400, f"Unknown tool spend feature/backend: {feature}/{backend}")
        if op not in spec["operations"]:
            raise ToolSpendError(400, f"Unknown operation {op!r} for {backend}")
        return features[feature], spec

    def _credential(self, project_id, user_id, spec, body):
        cfg = self.deps.configuration(body["configuration_project_id"], body["configuration_uuid"])
        if not cfg or cfg.get("type") != spec["configuration_type"]:
            raise ToolSpendError(400, "Configuration not found for this backend")
        owner = int(cfg["project_id"])
        # Public-project credentials are platform money whether or not they are shared; they reach
        # a caller only through a shared toolkit, so the key itself is never readable here
        if owner == self.deps.public_project_id():
            return cfg, False
        if owner == int(project_id) or owner == self.deps.personal_project_id(user_id):
            return cfg, True
        raise ToolSpendError(403, "Configuration is not accessible from this project")

    def _permit(self, project_id, user_id, feature, backend, op, cfg, exempt, spec, run_id):
        token, _claims = issue_permit(
            permit_key(self.deps.hmac_key()),
            pid=int(project_id), uid=int(user_id), feature=feature, backend=backend, op=op,
            cfg_uuid=str(cfg["uuid"]), exempt=bool(exempt), unit=spec["unit"], run_id=run_id,
        )
        return token

    def check(self, project_id, user_id, body, run_id=None, now=None):
        now = now or datetime.datetime.now(datetime.timezone.utc)
        body = _parse(ToolSpendCheckBody, body)
        feature, backend, op = body["feature"], body["backend"], body["op"]
        info, spec = self._spec(feature, backend, op)
        cfg, own = self._credential(project_id, user_id, spec, body)
        label = info.get("label") or feature
        #
        def allowed(exempt, **extra):
            return dict({"allowed": True, "exempt": exempt, "period_kind": "month",
                         "permit": self._permit(project_id, user_id, feature, backend, op, cfg, exempt, spec, run_id)},
                        **extra)
        #
        if own:
            return allowed(True)
        #
        defaults = feature_defaults(self.deps.config(), feature)
        if not defaults.get("enabled", False):
            return _deny(CODE_DISABLED, f"{label} is disabled by your administrator.")
        mode = self.deps.mode()
        #
        try:
            limits = self.deps.limits(project_id, user_id, feature)
        except Exception:  # pylint: disable=W0703
            log.exception("tool spend: limits unavailable for project %s feature %s", project_id, feature)
            limits = None
        if limits is not None and not limits.get("feature_enabled", True):
            return _deny(CODE_DISABLED, f"{label} is disabled for this project by your administrator.")
        if mode == MODE_OFF:
            return allowed(False)
        #
        spend = self.deps.spend(project_id, user_id, feature) if limits is not None else None
        if limits is None or not (spend or {}).get("available"):
            if mode == MODE_ENFORCE:
                return _deny(CODE_UNAVAILABLE, f"{label} is temporarily unavailable.")
            log.warning("tool spend: accounting unavailable for project %s feature %s, observe allows", project_id, feature)
            return allowed(False, warning=CODE_UNAVAILABLE)
        if mode != MODE_ENFORCE:
            return allowed(False)
        #
        resets_at = _next_period_start(now)
        for scope, limit, spent in (
                ("project", limits.get("project_limit_usd"), spend.get("project_spent_nano", 0)),
                ("member", limits.get("member_limit_usd"), spend.get("member_spent_nano", 0))):
            if limit is not None and int(spent) >= int(round(float(limit) * NANO)):
                self.deps.limit_reached(project_id, None if scope == "project" else user_id, feature, label, limit, now)
                return _deny(
                    CODE_LIMIT, _limit_message(label, scope, float(limit), resets_at), scope=scope,
                    limit_usd=float(limit), spent_usd=int(spent) / NANO, resets_at=resets_at.isoformat(),
                )
        return allowed(False)

    def record(self, project_id, user_id, body, attribution=None, now=None):
        body = _parse(ToolSpendRecordBody, body)
        feature, backend, op = body["feature"], body["backend"], body["op"]
        _info, spec = self._spec(feature, backend, op)
        try:
            claims = verify_permit(permit_key(self.deps.hmac_key()), body.get("permit"), {
                "pid": int(project_id), "uid": int(user_id), "feature": feature, "backend": backend,
                "op": op, "cfg_uuid": str(body.get("configuration_uuid")),
            }, now=now)
            verify_record_signature(self.deps.hmac_key(), project_id, body)
        except PermitError as exc:
            log.warning("tool spend: record rejected for project %s user %s: %s", project_id, user_id, exc)
            raise ToolSpendError(403, str(exc)) from exc
        units, meta = body["units"], {key: value for key, value in body["meta"].items() if value is not None}
        cap = spec["max_units_per_call"]
        if units > cap:
            # Sanity ceiling set in the toolkit code: keep the spend (capped) and leave a trail in the ledger
            log.warning("tool spend: units %s over cap %s for %s/%s, project %s user %s, clamped",
                        units, cap, backend, op, project_id, user_id)
            meta.update(clamped=True, reported_units=units)
            units = cap
        price = self.deps.price(backend, units, claims["unit"]) or {}
        record = {
            "ts": datetime.datetime.fromtimestamp(int(claims["iat"]), datetime.timezone.utc),
            "idempotency_key": f"tool_cost:{claims['nonce']}",
            "project_id": int(project_id), "user_id": int(user_id), "run_id": claims.get("run_id"),
            "feature": feature, "backend": backend, "op": op, "units": units, "unit": claims["unit"],
            "tool_name": body["tool_name"], "ok": body["ok"],
            "duration_ms": body["duration_ms"], "budget_exempt": bool(claims["exempt"]),
            "cost_usd": price.get("cost"), "cost_source": price.get("cost_source"),
            "meta": meta,
        }
        try:
            result = self.deps.ledger(record, attribution) or {}
        except Exception as exc:  # pylint: disable=W0703
            log.warning("tool spend: ledger rejected a record for project %s user %s: %s", project_id, user_id, exc)
            raise ToolSpendError(422, "record rejected by the ledger") from exc
        return {"recorded": bool(result.get("recorded")), "duplicate": bool(result.get("duplicate")),
                "cost_usd": price.get("cost")}


class PylonDeps:
    """Production wiring of ToolSpendService over RPCs and this module's state."""

    def __init__(self, module):
        self.module = module
        self.rpc = module.context.rpc_manager

    def features(self):
        from .tool_spend_features import feature_map  # pylint: disable=C0415
        return feature_map(self.module.toolkit_schemas)

    def config(self):
        return self.module.descriptor.config

    def configuration(self, project_id, uuid):
        return self.rpc.timeout(10).configurations_get_by_uuid(project_id, uuid)

    def public_project_id(self):
        from .utils import get_public_project_id  # pylint: disable=C0415
        return int(get_public_project_id())

    def personal_project_id(self, user_id):
        try:
            value = self.rpc.timeout(5).projects_get_personal_project_id(user_id)
        except Exception:  # pylint: disable=W0703
            log.exception("tool spend: personal project lookup failed for user %s", user_id)
            return None
        return int(value) if value is not None else None

    def hmac_key(self):
        from tools import this  # pylint: disable=E0401,C0415
        return (this.for_module("worker_client").descriptor.config.get("event_node") or {}).get("hmac_key")

    def mode(self):
        return self.rpc.timeout(5).usage_mode()

    def limits(self, project_id, user_id, feature):
        return self.module.get_effective_feature_limits(project_id, user_id, feature, "month")

    def spend(self, project_id, user_id, feature):
        try:
            return self.rpc.timeout(10).usage_get_feature_spend(project_id, user_id, feature)
        except Exception:  # pylint: disable=W0703
            log.exception("tool spend: spend read failed for project %s feature %s", project_id, feature)
            return {"available": False}

    def limit_reached(self, project_id, user_id, feature, label, limit_usd, now):
        if self.module.claim_feature_budget_alert(project_id, feature, f"{now:%Y%m}", 100, user_id=user_id):
            self.module.notify_budget_event(
                project_id, "limit", pct=100, user_id=user_id, feature=feature,
                feature_label=label, period_kind="month", limit_usd=float(limit_usd),
            )

    def price(self, backend, units, unit):
        try:
            return self.rpc.timeout(5).costs_compute_tool_cost(backend, units, unit)
        except Exception:  # pylint: disable=W0703
            log.exception("tool spend: pricing failed for %s, recording unpriced", backend)
            return None

    def ledger(self, record, attribution):
        return self.rpc.timeout(10).usage_record_tool_cost(record, attribution)
