import time
import json
import queue

from typing import Generator
from uuid import UUID
from uuid import uuid4

from mcp import types
from pylon.core.tools import log
from tools import context, this

_SSE_EVENTS_LISTENING_INTERVAL_SEC = this.descriptor.config.get("sse_events_listening_interval_sec", 0.1)
_SSE_KEEP_ALIVE_INTERVAL_SEC = this.descriptor.config.get("sse_keep_alive_interval_sec", 25)


class SseSession:
    def __init__(self, sid: UUID, project_id: int, tags: list[int], one_time=False,
                 resource_type: str = None, resource_id: int = None) -> None:
        self.sid = sid
        self.project_id = project_id
        self.tags = tags
        self.event_queue = queue.SimpleQueue()
        self.jrpc_response = None
        self.one_time = one_time
        # Resource scope for filtered MCP endpoints
        self.resource_type = resource_type  # 'toolkit', 'application', etc.
        self.resource_id = resource_id      # ID of the specific resource

    def process_event_queue(self) -> callable:
        if not self.one_time:
            self.dispatch_endpoint()
        #
        def event_processor() -> Generator[str, None, None]:
            last_emit_time = time.time()
            #
            while True:
                try:
                    msg = self.event_queue.get(
                        block=True,
                        timeout=_SSE_EVENTS_LISTENING_INTERVAL_SEC,
                    )
                    #
                    log.info(f"Sending message to SSE (session_id: {self.sid}): {msg}")
                    last_emit_time = time.time()
                    #
                    yield msg
                    #
                    if self.one_time and not msg.startswith("\n"):
                        log.info("Stopping one-time SSE stream")
                        break
                except queue.Empty:
                    if time.time() - last_emit_time >= _SSE_KEEP_ALIVE_INTERVAL_SEC:
                        self.dispatch_keepalive()
        #
        return event_processor

    def dispatch_keepalive(self) -> None:
        self._dispatch_sse_comment("keepalive")

    def dispatch_message(self, data: str) -> None:
        self._dispatch_sse_event(data, "message")

    def dispatch_ping_request(self) -> None:
        self.dispatch_message(
            types.PingRequest(
                method="ping"
            ).model_dump_json()
        )

    def dispatch_empty_response(self, id: str) -> None:
        self.dispatch_message(
            types.JSONRPCResponse(
                jsonrpc="2.0",
                id=id,
                result=types.EmptyResult()
            ).model_dump_json()
        )

    def dispatch_endpoint(self) -> None:
        endpoint_message = f"{context.url_prefix}/app/{self.project_id}/messages?session_id={self.sid}"
        self._dispatch_sse_event(endpoint_message, "endpoint")

    def dispatch_error_response(self, id: str, code: int, message: str) -> None:
        self.dispatch_message(
            types.JSONRPCError(
                jsonrpc="2.0",
                id=id,
                error=types.ErrorData(
                    code=code,
                    message=message
                )
            ).model_dump_json()
        )

    def _dispatch_sse_event(self, data: str, event: str) -> None:
        if self.one_time:
            msg = data
        else:
            msg = f"event: {event}\ndata: {data}\n\n"
        #
        log.info(f"Dispatching message for session_id: {self.sid}: {msg}")
        self.event_queue.put(msg)

    def _dispatch_sse_comment(self, comment: str) -> None:
        if self.one_time:
            msg = "\n"
        else:
            msg = f":{comment}\n\n"
        #
        log.info(f"Dispatching comment for session_id: {self.sid}: {comment}")
        self.event_queue.put(msg)


class HttpSession(SseSession):
    """ HTTP session """

    def __init__(self, project_id: int, tags: list[int],
                 resource_type: str = None, resource_id: int = None) -> None:
        super().__init__(uuid4(), project_id, tags,
                        resource_type=resource_type, resource_id=resource_id)

    def _dispatch_sse_event(self, data: str, event: str) -> None:
        if event != "message":
            return
        #
        jrpc_data = json.loads(data)
        #
        if self.jrpc_response is None:
            self.jrpc_response = jrpc_data
            return
        #
        if isinstance(self.jrpc_response, list):
            self.jrpc_response.append(jrpc_data)
            return
        #
        jrpc_item = self.jrpc_response
        #
        self.jrpc_response = []
        self.jrpc_response.append(jrpc_item)
        self.jrpc_response.append(jrpc_data)

    def _dispatch_sse_comment(self, comment: str) -> None:
        return
