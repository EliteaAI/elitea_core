from pylon.core.tools import web, log
from tools import rpc_tools

from ..utils.mcp_client import is_client_connected
from ..utils.sio_utils import SioEvents


class RPC:
    @web.rpc('mcp_servers_handler')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def mcp_servers_handler(self):
        for server in self.servers_storage.validate_all(is_client_connected):
            log.debug(f"[MCP_CLIENT] Server {server['name']} stopped answering")
            self.context.sio.emit(
                event=SioEvents.mcp_status,
                data={"connected": False, "project_id": server['project_id'], "type": server['name']},
            )

