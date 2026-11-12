"""Monthly history of the effective project budget limit (#7510).

The admin Budgets page can show a past month. Spend is already stored per month, but the
limit and its source are not: project_budgets keeps only today's settings and the platform
defaults live in config. These RPCs record what applied in each month so a later edit cannot
rewrite it.

Two writers feed the current month's row, last write wins:
  - set_project_budget writes through when an admin changes a project, and
  - an hourly job catches what has no database event (a changed platform default, a new
    month, a newly created project) and writes only the rows that differ.
"""

from pylon.core.tools import web, log

from tools import db

from ..utils import budget_periods


class RPC:
    """ RPC """

    @web.rpc("elitea_core_list_project_budget_periods", "list_project_budget_periods")
    def list_project_budget_periods(self, period, project_ids=None, **kwargs):
        """Stored snapshots of one month keyed by project id. A project with none is absent."""
        with db.with_project_schema_session(None) as session:
            return budget_periods.read_snapshots(session, period, project_ids)

    @web.rpc("elitea_core_snapshot_project_budget", "snapshot_project_budget")
    def snapshot_project_budget(self, project_id, **kwargs):
        """Record the current month's effective limit of one project.

        Never raises: the budget change that triggered this has already been saved, and a
        failed history write must not turn it into an error for the admin.
        """
        try:
            budget = self._read_project_budget(project_id)
            effective = self.get_effective_project_limit(project_id, project_budget=budget)
            snapshot = budget_periods.desired_snapshots(
                [project_id], {project_id: budget or {}}, {project_id: effective},
            )
            #
            with db.with_project_schema_session(None) as session:
                budget_periods.upsert_snapshots(
                    session, budget_periods.current_period(), snapshot,
                )
            #
            return True
        except Exception:  # pylint: disable=W0703
            log.exception("Failed to snapshot the budget of project %s", project_id)
            #
            return False

    @web.rpc("elitea_core_snapshot_all_project_budgets", "snapshot_all_project_budgets")
    def snapshot_all_project_budgets(self, **kwargs):
        """Hourly job: bring the current month's snapshots in line with today's limits.

        Resolves every project in bulk and writes only rows that are missing or different, so
        an hour with no changes costs one read and no writes. The first run of a month creates
        that month's row for every project; the previous month's rows are left as last written,
        which makes them that month's final effective limit.
        """
        try:
            projects = self.context.rpc_manager.timeout(60).project_list() or []
            project_ids = [int(project["id"]) for project in projects]
            #
            if not project_ids:
                return {"ok": True, "written": 0}
            #
            budgets = self.list_project_budgets() or {}
            limits = self.get_effective_project_limits(project_ids=project_ids) or {}
            desired = budget_periods.desired_snapshots(project_ids, budgets, limits)
            period = budget_periods.current_period()
            #
            with db.with_project_schema_session(None) as session:
                existing = budget_periods.read_snapshots(session, period)
                changed = budget_periods.changed_snapshots(existing, desired)
                #
                if changed:
                    budget_periods.upsert_snapshots(session, period, changed)
            #
            log.info("Budget period snapshot %s: %s of %s projects written",
                     period, len(changed), len(project_ids))
            #
            return {"ok": True, "written": len(changed)}
        except Exception:  # pylint: disable=W0703
            log.exception("Failed to snapshot project budgets")
            #
            return {"ok": False, "written": 0}
