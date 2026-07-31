from pylon.core.tools import web, log

from tools import db

from ..models.project_budget import ProjectBudget
from ..models.user_budget import UserBudget


# Distinguishes "not sent" from an explicit null, so a caller unaware of a field cannot clear it
_UNSET = object()


class RPC:
    @web.rpc("elitea_core_get_project_budget_limit", "get_project_budget_limit")
    def get_project_budget_limit(self, project_id: int, **kwargs):
        """Return the effective monthly limit (USD) for a project, or None if unlimited."""
        with db.with_project_schema_session(None) as session:
            budget = session.query(ProjectBudget).filter(
                ProjectBudget.project_id == project_id,
            ).first()
            #
            if budget is None or not budget.enabled:
                return None
            #
            return budget.monthly_limit

    @web.rpc("elitea_core_get_project_budget", "get_project_budget")
    def get_project_budget(self, project_id: int, **kwargs):
        """Return the stored budget row for a project as a dict, or None."""
        with db.with_project_schema_session(None) as session:
            budget = session.query(ProjectBudget).filter(
                ProjectBudget.project_id == project_id,
            ).first()
            #
            if budget is None:
                return None
            #
            return budget.to_json()

    @web.rpc("elitea_core_set_project_budget", "set_project_budget")
    def set_project_budget(  # pylint: disable=R0913
            self, project_id: int, monthly_limit=None, enabled=True, currency='USD',
            member_default_limit=_UNSET, **kwargs
    ):
        """Upsert a project's monthly budget and push it to the LiteLLM tag budget."""
        with db.with_project_schema_session(None) as session:
            budget = session.query(ProjectBudget).filter(
                ProjectBudget.project_id == project_id,
            ).first()
            #
            if budget is None:
                budget = ProjectBudget(project_id=project_id)
                session.add(budget)
            #
            budget.monthly_limit = monthly_limit
            budget.enabled = enabled
            budget.currency = currency
            # Omitted means "leave as is": a caller unaware of the field must not clear
            # the default for every member of the project
            if member_default_limit is not _UNSET:
                budget.member_default_limit = member_default_limit
            # A new limit restarts the alert cycle: clearing the period too, so a stale
            # period cannot suppress the first alert against the new limit
            budget.last_alerted_pct = None
            budget.last_alerted_period = None
            #
            session.commit()
            result = budget.to_json()
        #
        try:
            self.context.rpc_manager.timeout(15).litellm_push_project_budget(
                project_id=project_id,
            )
        except Exception:  # pylint: disable=W0703
            log.exception("Failed to push budget to LiteLLM for project %s", project_id)
        #
        return result

    @web.rpc("elitea_core_list_project_budgets", "list_project_budgets")
    def list_project_budgets(self, **kwargs):
        """Return all stored budget rows, keyed by project id."""
        with db.with_project_schema_session(None) as session:
            return {
                budget.project_id: budget.to_json()
                for budget in session.query(ProjectBudget).all()
            }

    @web.rpc("elitea_core_get_user_budget", "get_user_budget")
    def get_user_budget(self, project_id: int, user_id: int, **kwargs):
        """Return the stored per-user budget row within a project, or None."""
        with db.with_project_schema_session(None) as session:
            budget = session.query(UserBudget).filter(
                UserBudget.project_id == project_id,
                UserBudget.user_id == user_id,
            ).first()
            #
            if budget is None:
                return None
            #
            return budget.to_json()

    @web.rpc("elitea_core_set_user_budget", "set_user_budget")
    def set_user_budget(  # pylint: disable=R0913
            self, project_id: int, user_id: int, monthly_limit=None,
            enabled=True, currency='USD', **kwargs
    ):
        """Upsert a per-user budget within a project and push it to LiteLLM."""
        with db.with_project_schema_session(None) as session:
            budget = session.query(UserBudget).filter(
                UserBudget.project_id == project_id,
                UserBudget.user_id == user_id,
            ).first()
            #
            if budget is None:
                budget = UserBudget(project_id=project_id, user_id=user_id)
                session.add(budget)
            #
            budget.monthly_limit = monthly_limit
            budget.enabled = enabled
            budget.currency = currency
            # A new limit restarts the alert cycle, as for project budgets
            budget.last_alerted_pct = None
            budget.last_alerted_period = None
            #
            session.commit()
            result = budget.to_json()
        #
        try:
            self.context.rpc_manager.timeout(15).litellm_push_user_budget(
                project_id=project_id, user_id=user_id,
            )
        except Exception:  # pylint: disable=W0703
            log.exception(
                "Failed to push user budget to LiteLLM for project %s user %s",
                project_id, user_id,
            )
        #
        return result

    @web.rpc("elitea_core_claim_budget_alert", "claim_budget_alert")
    def claim_budget_alert(  # pylint: disable=R0913
            self, project_id: int, period: str, pct: int, user_id: int = None, **kwargs
    ):
        """Reserve the right to send one budget alert, or return False if already sent.

        Compare-and-set in a single call so two concurrent requests cannot both decide to
        notify. The stored value is the threshold last alerted at, not a flag: raising a
        threshold above what was already sent arms the alert again, while lowering it does
        not re-notify for spend that was already covered.

        A project inheriting a platform default has no stored row, so one is created purely
        to hold this state. monthly_limit stays NULL, which already means "inherit the
        default", so enforcement is unchanged.
        """
        try:
            with db.with_project_schema_session(None) as session:
                if user_id is None:
                    budget = session.query(ProjectBudget).filter(
                        ProjectBudget.project_id == project_id,
                    ).first()
                    #
                    if budget is None:
                        budget = ProjectBudget(project_id=project_id)
                        session.add(budget)
                else:
                    budget = session.query(UserBudget).filter(
                        UserBudget.project_id == project_id,
                        UserBudget.user_id == user_id,
                    ).first()
                    #
                    if budget is None:
                        budget = UserBudget(project_id=project_id, user_id=user_id)
                        session.add(budget)
                #
                # A new period starts a fresh cycle, so anything recorded earlier is stale
                same_period = budget.last_alerted_period == period
                already_sent = budget.last_alerted_pct if same_period else None
                #
                if already_sent is not None and already_sent >= pct:
                    return False
                #
                budget.last_alerted_pct = pct
                budget.last_alerted_period = period
                #
                session.commit()
                #
                return True
        except Exception:  # pylint: disable=W0703
            # Includes the columns not yet existing, when the release lands before the
            # migration is run. Not claiming means not notifying, never a broken request.
            log.exception(
                "Failed to claim budget alert for project %s user %s", project_id, user_id,
            )
            #
            return False

    @web.rpc("elitea_core_list_user_budgets", "list_user_budgets")
    def list_user_budgets(self, project_id: int = None, **kwargs):
        """Return stored per-user budget rows, optionally scoped to one project."""
        with db.with_project_schema_session(None) as session:
            query = session.query(UserBudget)
            #
            if project_id is not None:
                query = query.filter(UserBudget.project_id == project_id)
            #
            return [budget.to_json() for budget in query.all()]
