from pylon.core.tools import log
from tools import context

from ..models.mcp import McpConnectSioPayload, McpServer
from ..utils.sio_utils import SioEvents


def mcp_notification_to_sid(sid: str, notification: str):
    context.sio.emit(
        event=SioEvents.mcp_notification,
        data=notification,
        to=sid
    )


def load_server_from_client(server: McpServer):
    sid = server.sio_sid
    mcp_servers = context.sio.call(SioEvents.mcp_tools_list, sid, to=sid, timeout=server.timeout_tools_list)
    payload = McpConnectSioPayload.model_validate(mcp_servers)
    for toolkit_config in payload.toolkit_configs:
        if toolkit_config.name == server.name:
            toolkit_config.project_id = payload.project_id
            toolkit_config.sio_sid = sid
            toolkit_config.timeout_tools_list = payload.timeout_tools_list
            toolkit_config.timeout_tools_call = payload.timeout_tools_call
            #
            return toolkit_config
    return None


def is_client_connected(sid: str):
    try:
        context.sio.call(SioEvents.mcp_ping, sid, to=sid, timeout=5)
        return True
    except Exception as e:
        log.debug(f"[MCP_CLIENT] Connection for client {sid} was evaluated as disconnected: {e}")
        return False
