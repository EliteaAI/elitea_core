from pylon.core.tools import log
from tools import context

from ..utils.sio_utils import SioEvents


def mcp_notification_to_sid(sid: str, notification: str):
    context.sio.emit(
        event=SioEvents.mcp_notification,
        data=notification,
        to=sid
    )


def is_client_connected(sid: str):
    try:
        context.sio.call(SioEvents.mcp_ping, sid, to=sid, timeout=5)
        return True
    except Exception as e:
        log.debug(f"[MCP_CLIENT] Connection for client {sid} was evaluated as disconnected: {e}")
        return False
