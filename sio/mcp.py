#!/usr/bin/python3
# coding=utf-8

#   Copyright 2025 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" SIO """

import re

from pydantic import ValidationError
from pylon.core.tools import log, web  # pylint: disable=E0611,E0401,W0611
from tools import auth

from ..models.mcp import McpConnectSioPayload
from ..utils.mcp_client import mcp_notification_to_sid
from ..utils.sio_utils import SioEvents, get_event_room


PROTOCOL = 2


class SIO:  # pylint: disable=E1101,R0903
    @web.sio(SioEvents.mcp_connect)
    def mcp_connect(self, sid: str, data: dict):
        if not self.servers_storage.allow_connect(sid):
            log.warning("Sid %s sent mcp_connect too often", sid)
            return {
                "ok": False,
                "error": "rate_limited",
                "message": "Too many registrations from this client; try again in a minute.",
            }
        try:
            payload = McpConnectSioPayload.model_validate(data)
        except ValidationError as error:
            log.warning("Sid %s sent an invalid mcp_connect payload: %s", sid, error)
            return {
                "ok": False,
                "error": "validation_error",
                "message": "The mcp_connect payload is invalid.",
                "details": error.errors(include_url=False, include_context=False, include_input=False),
            }
        #
        if not auth.is_sio_user_in_project(sid, payload.project_id):
            log.warning("Sid %s is not in project %s", sid, payload.project_id)
            return {
                "ok": False,
                "error": "not_in_project",
                "message": f"User has no access to project {payload.project_id} or the project does not exist.",
                "project_id": payload.project_id,
            }
        #
        ack = _apply_snapshot(self, sid, payload) if payload.is_snapshot else _add_servers(self, sid, payload)
        room = get_event_room(SioEvents.mcp_connect, str(payload.project_id))
        self.context.sio.enter_room(sid, room)
        return ack


def _bind_toolkit_configs(sid, payload):
    """Attach the socket and timeouts to each server, keeping a per-server call timeout; return the servers and the invalid names."""
    servers, invalid = [], []
    for toolkit_config in payload.toolkit_configs:
        requested_name = toolkit_config.name
        toolkit_config.project_id = payload.project_id
        toolkit_config.sio_sid = sid
        toolkit_config.timeout_tools_list = payload.timeout_tools_list
        if "timeout_tools_call" not in toolkit_config.model_fields_set:
            toolkit_config.timeout_tools_call = payload.timeout_tools_call
        toolkit_config.name = _sanitize_name(requested_name)
        if toolkit_config.name:
            servers.append(toolkit_config)
        else:
            invalid.append({"name": requested_name, "reason": "invalid_name"})
    return servers, invalid


def _add_servers(module, sid, payload):
    """The path of clients that send no generation: add the new names, never replace or remove."""
    servers, declined = _bind_toolkit_configs(sid, payload)
    for item in declined:
        mcp_notification_to_sid(sid, f"{item['name']} - declined (invalid name).")
    registered = []
    for server in servers:
        if module.servers_storage.add_server(payload.project_id, server):
            status = "registered"
            registered.append(server.name)
            _emit_status(module, payload.project_id, server.name, connected=True)
        else:
            status = "declined"
            declined.append({"name": server.name, "reason": "name_already_registered"})
        #
        mcp_notification_to_sid(sid, f"{server.name} - {status}.")
        log.debug(f"[MCP_CLIENT] Mcp Server {server.name} has been {status} for project {payload.project_id}")
    return {"ok": True, "registered": registered, "declined": declined}


def _apply_snapshot(module, sid, payload):
    """Replace everything this client owns in the project with the snapshot, announcing only what changed."""
    user_id = auth.current_user(auth_data=auth.sio_users[sid]).get("id")
    servers, invalid = _bind_toolkit_configs(sid, payload)
    failed = [item for item in payload.failed_servers if _rename_failed(item)]
    result = module.servers_storage.apply_snapshot(
        payload.project_id, payload.client_id, user_id, sid, payload.epoch, payload.generation, servers, failed,
    )
    if not result.stale:
        for name in result.added + result.updated:
            _emit_status(module, payload.project_id, name, connected=True)
        for name in result.removed:
            _emit_status(module, payload.project_id, name, connected=False)
        log.debug(f"[MCP_CLIENT] Snapshot {payload.epoch}/{payload.generation} of client {payload.client_id} for project "
                  f"{payload.project_id}: added {result.added}, updated {result.updated}, removed {result.removed}")
    return {
        "ok": True,
        "protocol": PROTOCOL,
        "epoch": payload.epoch,
        "generation": payload.generation,
        "stale": result.stale,
        "registered": result.registered,
        "declined": invalid + result.declined,
        "failed": result.failed,
    }


def _rename_failed(item):
    item.name = _sanitize_name(item.name)
    return bool(item.name)


def _emit_status(module, project_id, name, connected):
    """Every connected socket receives this broadcast, so it carries no client-reported text."""
    module.context.sio.emit(event=SioEvents.mcp_status,
                            data={"connected": connected, "project_id": project_id, "type": name})


def _sanitize_name(name: str) -> str:
    # Remove leading non-letters
    result = re.sub(r'^[^a-zA-Z]+', '', name)
    # Remove trailing non-letters/digits/underscores
    result = re.sub(r'[^a-zA-Z0-9_]+$', '', result)
    # Replace invalid middle characters with _
    return re.sub(r'[^a-zA-Z0-9_]', '_', result)
