""" Guards shared by the socket entry points that start a run """
from pylon.core.tools import log
from tools import auth

from .sio_utils import SioValidationError, SOCKET_UNAUTHENTICATED_CODE, SOCKET_UNAUTHENTICATED_MESSAGE


def ensure_sio_user_in_project(sio, sid, project_id, event, stream_id, message_id) -> None:
    """Refuse a send from a socket whose user is not in the project.

    A socket that reconnected with an expired session, or while the auth service was unreachable, is held by
    the server as an anonymous user. Returning silently left the client's reply placeholder spinning forever
    (#7024), so the sender is told, with a code the client can act on.

    Does nothing without a ``sid``: callers that have no socket are not checked here.
    """
    if sid and not auth.is_sio_user_in_project(sid, project_id):
        log.warning("Sid %s is not in project %s", sid, project_id)
        raise SioValidationError(
            sio=sio,
            sid=sid,
            event=event,
            error=SOCKET_UNAUTHENTICATED_MESSAGE,
            stream_id=stream_id,
            message_id=message_id,
            code=SOCKET_UNAUTHENTICATED_CODE,
        )
