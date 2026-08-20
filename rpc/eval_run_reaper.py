from pylon.core.tools import web, log

from ..utils.evaluation_run_reaper import reap_orphaned_runs


class RPC:
    @web.rpc("elitea_core_reap_orphaned_eval_runs", "reap_orphaned_eval_runs")
    def reap_orphaned_eval_runs_rpc(self, **kwargs) -> dict | None:
        """Fail an eval run whose executing thread died with the process (deploy, restart,
        gunicorn worker recycle) and left the row stuck in ``running``. Cron-driven rather
        than startup-driven so it also catches a run orphaned mid-deploy, and so it fires
        once instead of once per API worker."""
        try:
            result = reap_orphaned_runs()
            if result['reaped_runs']:
                log.info('eval run reap: failed %s orphaned run(s)', result['reaped_runs'])
            return result
        except Exception:  # pylint: disable=broad-except
            log.exception('Error during orphaned eval run reap')
            return None
