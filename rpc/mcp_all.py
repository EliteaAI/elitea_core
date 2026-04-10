from pylon.core.tools import web, log
from tools import rpc_tools

from ..utils.mcp_client import is_client_connected


class RPC:
    @web.rpc('mcp_servers_handler')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def mcp_servers_handler(self):
        self.servers_storage.validate_all(is_client_connected)
