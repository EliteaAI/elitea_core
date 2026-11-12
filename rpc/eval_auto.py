"""RPCs for the environment-wide scheduled evaluation.

``tick`` and ``drain`` are the scheduler's entry points; the rest serve the ``admin`` plugin's
Super Admin dashboard, matching the existing ``admin`` -> ``elitea_core`` direction.
"""

from pylon.core.tools import web, log

from ..utils import eval_auto_service as service


class RPC:
    @web.rpc("elitea_core_eval_auto_tick", "eval_auto_tick")
    def eval_auto_tick_rpc(self, **kwargs) -> dict | None:
        try:
            return service.tick()
        except Exception:  # pylint: disable=broad-except
            log.exception('Error during scheduled evaluation tick')
            return None

    @web.rpc("elitea_core_eval_auto_drain", "eval_auto_drain")
    def eval_auto_drain_rpc(self, **kwargs) -> dict | None:
        try:
            return service.drain(self.eval_task_node)
        except Exception:  # pylint: disable=broad-except
            log.exception('Error during scheduled evaluation drain')
            return None

    @web.rpc("elitea_core_eval_auto_run_now", "eval_auto_run_now")
    def eval_auto_run_now_rpc(self, user_id: int = None, **kwargs) -> dict:
        return service.run_now(user_id=user_id)

    @web.rpc("elitea_core_eval_auto_get_config", "eval_auto_get_config")
    def eval_auto_get_config_rpc(self, **kwargs) -> dict:
        return service.get_config()

    @web.rpc("elitea_core_eval_auto_set_config", "eval_auto_set_config")
    def eval_auto_set_config_rpc(self, settings: dict, user_id: int = None, **kwargs) -> dict:
        try:
            return {'ok': True, 'settings': service.set_config(settings, user_id=user_id)}
        except ValueError as exc:
            return {'ok': False, 'error': str(exc)}

    @web.rpc("elitea_core_eval_auto_list", "eval_auto_list")
    def eval_auto_list_rpc(self, project_id: int = None, status: str = None, limit: int = 500,
                           offset: int = 0, search: str = None, reason: str = None, **kwargs) -> dict:
        return service.list_latest(project_id=project_id, status=status, limit=limit, offset=offset,
                                   search=search, reason=reason)

    @web.rpc("elitea_core_eval_auto_history", "eval_auto_history")
    def eval_auto_history_rpc(self, project_id: int, application_id: int, limit: int = 20, **kwargs) -> dict:
        return service.list_history(project_id, application_id, limit=limit)

    @web.rpc("elitea_core_eval_auto_get_platform_dataset", "eval_auto_get_platform_dataset")
    def eval_auto_get_platform_dataset_rpc(self, **kwargs) -> dict:
        return service.get_platform_dataset()

    @web.rpc("elitea_core_eval_auto_set_platform_probes", "eval_auto_set_platform_probes")
    def eval_auto_set_platform_probes_rpc(self, probes: list, **kwargs) -> dict:
        try:
            return {'ok': True, **service.set_platform_probes(probes)}
        except ValueError as exc:
            return {'ok': False, 'error': str(exc)}

    @web.rpc("elitea_core_eval_auto_resync_platform_dataset", "eval_auto_resync_platform_dataset")
    def eval_auto_resync_platform_dataset_rpc(self, **kwargs) -> dict:
        try:
            return {'ok': True, **service.resync_platform_dataset()}
        except ValueError as exc:
            return {'ok': False, 'error': str(exc)}
