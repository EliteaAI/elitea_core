from pylon.core.tools import web, log

from tools import db

from ..models.project_budget import ProjectBudget
from ..models.user_budget import UserBudget


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
            self, project_id: int, monthly_limit=None, enabled=True, currency='USD', **kwargs
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
            # A new limit restarts the alert cycle for this period
            budget.last_alerted_pct = None
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
