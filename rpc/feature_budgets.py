""" Tool Spend feature budget CRUD (#6980). Separate from project_budgets.py, which stays untouched. """

from pylon.core.tools import web, log

from tools import db

from ..models.feature_budget import FeatureBudget
from ..models.user_feature_budget import UserFeatureBudget
from ..utils.tool_spend_features import validate_feature_period

# Distinguishes "not sent" from an explicit null, so a caller unaware of a field cannot clear it
_UNSET = object()


def _project_row(session, project_id, feature, period_kind):
    return session.query(FeatureBudget).filter(
        FeatureBudget.project_id == project_id,
        FeatureBudget.feature == feature,
        FeatureBudget.period_kind == period_kind,
    ).first()


def _user_row(session, project_id, user_id, feature, period_kind):
    return session.query(UserFeatureBudget).filter(
        UserFeatureBudget.project_id == project_id,
        UserFeatureBudget.user_id == user_id,
        UserFeatureBudget.feature == feature,
        UserFeatureBudget.period_kind == period_kind,
    ).first()


class RPC:
    @web.rpc("elitea_core_get_project_feature_budget", "get_project_feature_budget")
    def get_project_feature_budget(self, project_id, feature, period_kind="month", **kwargs):
        """Stored feature row for a project, or None (= Default)."""
        validate_feature_period(feature, period_kind)
        with db.with_project_schema_session(None) as session:
            row = _project_row(session, project_id, feature, period_kind)
            return row.to_json() if row is not None else None

    @web.rpc("elitea_core_set_project_feature_budget", "set_project_feature_budget")
    def set_project_feature_budget(  # pylint: disable=R0913
            self, project_id, feature, period_kind="month", limit_usd=_UNSET,
            member_default_limit_usd=_UNSET, enabled=_UNSET, feature_enabled=_UNSET, **kwargs
    ):
        """Upsert a project's feature row; omitted fields are left as they are."""
        validate_feature_period(feature, period_kind)
        with db.with_project_schema_session(None) as session:
            row = _project_row(session, project_id, feature, period_kind)
            if row is None:
                row = FeatureBudget(project_id=project_id, feature=feature, period_kind=period_kind, enabled=True)
                session.add(row)
            if limit_usd is not _UNSET:
                row.limit_usd = limit_usd
            if member_default_limit_usd is not _UNSET:
                row.member_default_limit_usd = member_default_limit_usd
            if enabled is not _UNSET:
                row.enabled = bool(enabled)
            if feature_enabled is not _UNSET:
                row.feature_enabled = feature_enabled
            # A new limit restarts the alert cycle
            row.last_alerted_pct = None
            row.last_alerted_period = None
            session.commit()
            return row.to_json()

    @web.rpc("elitea_core_delete_project_feature_budget", "delete_project_feature_budget")
    def delete_project_feature_budget(self, project_id, feature, period_kind="month", **kwargs):
        """Reset to default: the row is removed, so the ladder falls back to the platform default."""
        validate_feature_period(feature, period_kind)
        with db.with_project_schema_session(None) as session:
            row = _project_row(session, project_id, feature, period_kind)
            if row is None:
                return False
            session.delete(row)
            session.commit()
            return True

    @web.rpc("elitea_core_list_project_feature_budgets", "list_project_feature_budgets")
    def list_project_feature_budgets(self, feature, period_kind="month", **kwargs):
        """Every stored project row of a feature, keyed by project id."""
        validate_feature_period(feature, period_kind)
        with db.with_project_schema_session(None) as session:
            return {
                row.project_id: row.to_json()
                for row in session.query(FeatureBudget).filter(
                    FeatureBudget.feature == feature, FeatureBudget.period_kind == period_kind,
                ).all()
            }

    @web.rpc("elitea_core_get_user_feature_budget", "get_user_feature_budget")
    def get_user_feature_budget(self, project_id, user_id, feature, period_kind="month", **kwargs):
        validate_feature_period(feature, period_kind)
        with db.with_project_schema_session(None) as session:
            row = _user_row(session, project_id, user_id, feature, period_kind)
            return row.to_json() if row is not None else None

    @web.rpc("elitea_core_set_user_feature_budget", "set_user_feature_budget")
    def set_user_feature_budget(  # pylint: disable=R0913
            self, project_id, user_id, feature, period_kind="month", limit_usd=_UNSET,
            enabled=_UNSET, **kwargs
    ):
        """Upsert a member's feature row; omitted fields are left as they are."""
        validate_feature_period(feature, period_kind)
        with db.with_project_schema_session(None) as session:
            row = _user_row(session, project_id, user_id, feature, period_kind)
            if row is None:
                row = UserFeatureBudget(
                    project_id=project_id, user_id=user_id, feature=feature, period_kind=period_kind, enabled=True,
                )
                session.add(row)
            if limit_usd is not _UNSET:
                row.limit_usd = limit_usd
            if enabled is not _UNSET:
                row.enabled = bool(enabled)
            row.last_alerted_pct = None
            row.last_alerted_period = None
            session.commit()
            return row.to_json()

    @web.rpc("elitea_core_delete_user_feature_budget", "delete_user_feature_budget")
    def delete_user_feature_budget(self, project_id, user_id, feature, period_kind="month", **kwargs):
        validate_feature_period(feature, period_kind)
        with db.with_project_schema_session(None) as session:
            row = _user_row(session, project_id, user_id, feature, period_kind)
            if row is None:
                return False
            session.delete(row)
            session.commit()
            return True

    @web.rpc("elitea_core_list_user_feature_budgets", "list_user_feature_budgets")
    def list_user_feature_budgets(self, project_id, feature, period_kind="month", **kwargs):
        validate_feature_period(feature, period_kind)
        with db.with_project_schema_session(None) as session:
            return [
                row.to_json() for row in session.query(UserFeatureBudget).filter(
                    UserFeatureBudget.project_id == project_id,
                    UserFeatureBudget.feature == feature,
                    UserFeatureBudget.period_kind == period_kind,
                ).all()
            ]

    @web.rpc("elitea_core_claim_feature_budget_alert", "claim_feature_budget_alert")
    def claim_feature_budget_alert(  # pylint: disable=R0913
            self, project_id, feature, period, pct, user_id=None, period_kind="month", **kwargs
    ):
        """Compare-and-set one feature alert per period and threshold; False when already sent."""
        try:
            validate_feature_period(feature, period_kind)
            with db.with_project_schema_session(None) as session:
                if user_id is None:
                    row = _project_row(session, project_id, feature, period_kind)
                    if row is None:
                        row = FeatureBudget(project_id=project_id, feature=feature, period_kind=period_kind, enabled=True)
                        session.add(row)
                else:
                    row = _user_row(session, project_id, user_id, feature, period_kind)
                    if row is None:
                        row = UserFeatureBudget(
                            project_id=project_id, user_id=user_id, feature=feature,
                            period_kind=period_kind, enabled=True,
                        )
                        session.add(row)
                already_sent = row.last_alerted_pct if row.last_alerted_period == period else None
                if already_sent is not None and already_sent >= pct:
                    return False
                row.last_alerted_pct = pct
                row.last_alerted_period = period
                session.commit()
                return True
        except Exception:  # pylint: disable=W0703
            log.exception("Failed to claim %s alert for project %s user %s", feature, project_id, user_id)
            return False
