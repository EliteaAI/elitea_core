import threading
from typing import Any, Generator
from typing import Dict
from uuid import UUID
from uuid import uuid4

from pylon.core.tools import log

from ..utils.tags import list_tags
from .mcp_session import SseSession, HttpSession


class CommunicationsHandler:
    def __init__(self) -> None:
        self.sessions: Dict[UUID, SseSession] = {}
        self.lock = threading.Lock()

    def create_session_and_stream(self, project_id: int, return_session=False, one_time=False,
                                  resource_type: str = None, resource_id: int = None) -> tuple[Generator[str, Any, None] | None, tuple[dict, int] | None]:
        agents_tags = list_tags(project_id, {})["rows"]
        mcp_tag = [tag["id"] for tag in agents_tags if tag["name"] == "mcp"][:1]
        sid = uuid4()
        session = SseSession(sid, project_id, mcp_tag, one_time=one_time,
                           resource_type=resource_type, resource_id=resource_id)

        with self.lock:
            self.sessions[sid] = session

        def stream():
            try:
                yield from session.process_event_queue()()
            except Exception as e:
                log.error(f"Error while streaming SSE messages (session_id: {sid}): {e}")
            finally:
                with self.lock:
                    if sid in self.sessions:
                        del self.sessions[sid]
                log.info(f"Client disconnected. Removed session_id: {sid}")

        if return_session:
            return stream(), None, session
        return stream(), None

    def create_http_session(self, project_id: int,
                            resource_type: str = None, resource_id: int = None):
        agents_tags = list_tags(project_id, {})["rows"]
        mcp_tag = [tag["id"] for tag in agents_tags if tag["name"] == "mcp"][:1]
        #
        session = HttpSession(project_id, mcp_tag,
                            resource_type=resource_type, resource_id=resource_id)
        #
        return session, None

    def get_session(self, sid: UUID) -> SseSession | None:
        with self.lock:
            return self.sessions.get(sid)
