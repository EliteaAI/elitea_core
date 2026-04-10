#!/usr/bin/python3
# coding=utf-8

#   Copyright 2024 EPAM Systems
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
from copy import deepcopy
from pylon.core.tools import web, log  # pylint: disable=E0611,E0401

from tools import context
from ..utils.toolkit_security import (
    filter_blocked_toolkit_list,
    filter_tools_in_schema,
    get_toolkit_security_config
)


class Method:
    @web.method()
    def toolkits_collected(self, event, payload: list[dict]):
        # Filter out blocked toolkits before registration
        config = get_toolkit_security_config()
        if config['blocked_toolkits']:
            log.info(f"[SECURITY] Blocking toolkits: {config['blocked_toolkits']}")
        if config['blocked_tools']:
            log.info(f"[SECURITY] Blocking tools: {config['blocked_tools']}")

        filtered_payload = filter_blocked_toolkit_list(payload)

        # Also filter blocked tools within each toolkit and pre-compute name_required
        for schema in filtered_payload:
            filtered_schema = filter_tools_in_schema(schema, config=config)
            filtered_schema['name_required'] = not any(
                v.get('toolkit_name') and isinstance(v['toolkit_name'], bool)
                for v in filtered_schema.get('properties', {}).values()
            )
            self.toolkit_schemas[filtered_schema['title']] = filtered_schema

        blocked_count = len(payload) - len(filtered_payload)
        if blocked_count > 0:
            log.info(f"[SECURITY] Blocked {blocked_count} toolkit(s) from registration")

        log.info("Toolkit schemas definitions collected successfully")

    @web.method()
    def toolkit_configurations_collected(self, event, payload: dict):
        self.configuration_schemas = deepcopy(payload)

        # Register configuration schemas directly without model generation
        RPC_CALL_TIMEOUT = 3

        try:
            # Get existing registered configurations to avoid duplicates
            existing_configurations = context.rpc_manager.timeout(RPC_CALL_TIMEOUT).configurations_list_types()
            existing_configuration_names = {config.type for config in existing_configurations}

            for configuration_type, schema in payload.items():
                # TODO: unregister and register new if needed
                if configuration_type in existing_configuration_names:
                    log.warning(f"Configuration {configuration_type} already registered")
                    continue

                if schema.get('metadata', {}).get('check_connection_supported'):
                    check_connection_func = "applications_configuration_check_connection"
                else:
                    check_connection_func = None
                try:
                    context.rpc_manager.timeout(RPC_CALL_TIMEOUT).configurations_register(
                        type_name=configuration_type,
                        section=schema['metadata']['section'],
                        config_schema=schema,
                        validation_func="applications_configuration_validator",
                        check_connection_func=check_connection_func
                    )

                except Exception as ex:
                    log.error(f"Failed to register configuration for {configuration_type}: {ex}")

        except Exception as ex:
            log.error(f"Error during configuration registration: {ex}")

        log.info("Configuration schema definitions collected and registered successfully")
        self.toolkit_configurations_ready_event.set()

    @web.method()
    def index_types_collected(self, event, payload: dict):
        self.index_types = payload
        log.info(f"File type loaders collected: {len(self.index_types)}")
