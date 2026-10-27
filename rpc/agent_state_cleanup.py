from pylon.core.tools import web, log

from ..utils.project_utils import get_all_project_ids
from ..utils.schedule_runs import approval_wait_hours, has_waiting_runs
from ..utils.vectorstore import get_pgvector_connection_string


def waiting_run_min_days() -> int:
    """The cleanup deletes by age only, so a project with a run waiting for approval keeps the whole wait plus a day."""
    return -(-approval_wait_hours() // 24) + 1


class RPC:
    @web.rpc("applications_empty_state")
    def empty_state(self, **kwargs):
        project_ids: list[int] = get_all_project_ids()
        for project_id in project_ids:
            pgvector_connection_string: str = get_pgvector_connection_string(project_id)
            empty_agent_state_input: dict = {
                'pgvector_connstr': pgvector_connection_string,
                **kwargs
            }
            try:
                waiting = has_waiting_runs(project_id)
            except Exception:  # noqa: BLE001 -- a project without the tables is read as waiting, so nothing is lost
                log.exception('applications_empty_state: waiting runs not read: project_id=%s', project_id)
                waiting = True
            if waiting:
                empty_agent_state_input['days_to_retain'] = max(
                    empty_agent_state_input.get('days_to_retain', 1), waiting_run_min_days())
            self.event_node.emit('indexer_empty_agent_state', empty_agent_state_input)