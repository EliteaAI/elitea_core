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

""" Method """

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def get_provider_api_info(self, user_id, project_id, provider_name):
        """ Method """
        provider = self.lookup_provider(user_id, project_id, provider_name)
        #
        if provider is None:
            log.warning("Provider not found: %s (user=%s, project=%s)", provider_name, user_id, project_id)
            return None
        #
        log.info("get_provider_api_info: provider=%s, toolkits=%s",
                 provider_name, [tk.name for tk in provider.provided_toolkits])
        #
        # Debug: show tools count per toolkit
        for tk in provider.provided_toolkits:
            tools_list = tk.provided_tools
            tools_count = len(tools_list) if tools_list else 0
            log.info("get_provider_api_info: toolkit=%s, provided_tools type=%s, count=%s",
                     tk.name, type(tools_list).__name__, tools_count)
        #
        toolkits = {}
        toolkits_metadata = {}
        #
        for toolkit in provider.provided_toolkits:
            name = toolkit.name
            tools = []
            #
            # Capture toolkit-level metadata (includes required_context, type_override, etc.)
            tk_metadata = getattr(toolkit, "toolkit_metadata", None)
            if tk_metadata:
                toolkits_metadata[name] = dict(tk_metadata) if hasattr(tk_metadata, '__iter__') else {}
            #
            for tool in toolkit.provided_tools:
                try:
                    log.info("get_provider_api_info: converting tool=%s, args_schema type=%s, tool_metadata type=%s",
                             tool.name, type(tool.args_schema).__name__, type(tool.tool_metadata).__name__)
                    tools.append({
                        "name": tool.name,
                        "description": tool.description,
                        "args_schema": self.convert_args_schema(tool.args_schema),
                        "tool_metadata": dict(tool.tool_metadata) if tool.tool_metadata else {},
                        "sync_invocation_supported": tool.sync_invocation_supported,
                        "async_invocation_supported": tool.async_invocation_supported,
                    })
                except Exception as e:
                    log.error("get_provider_api_info: ERROR converting tool=%s: %s", tool.name, e)
                    raise
            #
            toolkits[name] = tools
        #
        return {
            "api_schema_json": self.api_schema_json,
            "service_location_url": str(provider.service_location_url),
            "toolkits": toolkits,
            "toolkits_metadata": toolkits_metadata,
        }
