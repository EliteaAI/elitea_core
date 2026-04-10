from pylon.core.tools import web, log

from ..utils.project_utils import get_all_project_ids
from ..utils.vectorstore import get_pgvector_connection_string


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
            self.event_node.emit('indexer_empty_agent_state', empty_agent_state_input)