from pylon.core.tools import web, log

from ..utils.project_utils import get_all_project_ids
from ..utils.skill_scratch import DEFAULT_IDLE_DAYS, sweep_idle_scratch


class RPC:
    @web.rpc("applications_sweep_skill_scratch")
    def sweep_skill_scratch(self, **kwargs):
        """Daily schedule: drop the script scratch of conversations idle for ``idle_days`` in every project."""
        idle_days = int(kwargs.get('idle_days') or DEFAULT_IDLE_DAYS)
        totals = {'removed': 0, 'kept': 0, 'failed_projects': 0}
        for project_id in get_all_project_ids():
            try:
                swept = sweep_idle_scratch(project_id, idle_days=idle_days)
            except Exception:  # noqa: BLE001 -- one project's storage must not stop the sweep of the others
                log.exception('applications_sweep_skill_scratch: project %s not swept', project_id)
                totals['failed_projects'] += 1
                continue
            totals['removed'] += swept['removed']
            totals['kept'] += swept['kept']
        return totals
