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

from pylon.core.tools import log, web  # pylint: disable=E0611,E0401,W0611
from tools import auth

from ..models.mcp import McpConnectSioPayload
from ..utils.mcp_client import mcp_notification_to_sid
from ..utils.sio_utils import SioEvents, get_event_room


class SIO:  # pylint: disable=E1101,R0903
    @web.sio(SioEvents.mcp_connect)
    def mcp_connect(self, sid: str, data: dict):
        payload = McpConnectSioPayload.model_validate(data)
        #
        if not auth.is_sio_user_in_project(sid, payload.project_id):
            log.warning("Sid %s is not in project %s", sid, payload.project_id)
            return  # FIXME: need some proper error?
        #
        for toolkit_config in payload.toolkit_configs:
            toolkit_config.project_id = payload.project_id
            toolkit_config.sio_sid = sid
            toolkit_config.timeout_tools_list = payload.timeout_tools_list
            toolkit_config.timeout_tools_call = payload.timeout_tools_call
            toolkit_config.name = _sanitize_name(toolkit_config.name)
            #
            is_registered = self.servers_storage.add_server(payload.project_id, toolkit_config)
            status = "registered" if is_registered else "declined"
            #
            mcp_notification_to_sid(sid, f"{toolkit_config.name} - {status}.")
            log.debug(f"[MCP_CLIENT] Mcp Server {toolkit_config.name} has been {status} for project {payload.project_id}")
            self.context.sio.emit(
                event=SioEvents.mcp_status,
                data={"connected": True, "project_id": payload.project_id, "type": toolkit_config.name},
            )

        room = get_event_room(SioEvents.mcp_connect, str(payload.project_id))
        self.context.sio.enter_room(sid, room)


def _sanitize_name(name: str) -> str:
    # Remove leading non-letters
    result = re.sub(r'^[^a-zA-Z]+', '', name)
    # Remove trailing non-letters/digits/underscores
    result = re.sub(r'[^a-zA-Z0-9_]+$', '', result)
    # Replace invalid middle characters with _
    return re.sub(r'[^a-zA-Z0-9_]', '_', result)
