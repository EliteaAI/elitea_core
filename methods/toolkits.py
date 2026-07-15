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


_INDEXER_CONFIGURATION_VALIDATOR = "applications_configuration_validator"


def _is_indexer_mcp_configuration(entry):
    """Return whether a registry entry is owned by static indexer MCP config."""
    schema = getattr(entry, "config_schema", None) or {}
    metadata = schema.get("metadata", {}) if isinstance(schema, dict) else {}
    return (
        entry.type.startswith("mcp_")
        and entry.section == "toolkits"
        and entry.validation_func == _INDEXER_CONFIGURATION_VALIDATOR
        and bool(metadata.get("mcp_server_name"))
    )


class Method:
    @web.method()
    def toolkits_collected(self, event, payload: list[dict]):
        # Store the full, UNFILTERED toolkit registry. Guardrails (blocked
        # toolkits/tools) are applied live at read time in get_toolkit_schemas,
        # so block/unblock take effect without a pylon restart. Filtering here
        # would be destructive — an unblocked toolkit could never be restored
        # without rebuilding this startup-built registry.
        for schema in payload:
            schema['name_required'] = not any(
                v.get('toolkit_name') and isinstance(v['toolkit_name'], bool)
                for v in schema.get('properties', {}).values()
            )
            self.toolkit_schemas[schema['title']] = schema

        log.info("Toolkit schemas definitions collected successfully")

    @web.method()
    def toolkit_configurations_collected(self, event, payload: dict):
        self.configuration_schemas = deepcopy(payload)

        # Register configuration schemas directly without model generation
        RPC_CALL_TIMEOUT = 3

        try:
            # Get existing registered configurations to avoid duplicates
            existing_configurations = context.rpc_manager.timeout(RPC_CALL_TIMEOUT).configurations_list_types()
            existing_by_name = {config.type: config for config in existing_configurations}
            incoming_mcp_names = {
                name for name in payload if name.startswith("mcp_")
            }

            # Static MCP configuration types are generated from the current
            # indexer definitions. Remove types that disappeared, including
            # after an elitea_core reload where the prior in-memory payload is
            # no longer available.
            for configuration_type, entry in existing_by_name.items():
                if (
                    _is_indexer_mcp_configuration(entry)
                    and configuration_type not in incoming_mcp_names
                ):
                    try:
                        context.rpc_manager.timeout(
                            RPC_CALL_TIMEOUT
                        ).configurations_unregister(type_name=configuration_type)
                    except Exception as ex:
                        log.error(
                            "Failed to unregister configuration %s: %s",
                            configuration_type,
                            ex,
                        )

            for configuration_type, schema in payload.items():
                existing = existing_by_name.get(configuration_type)
                managed_mcp = existing is not None and _is_indexer_mcp_configuration(existing)
                unchanged = (
                    managed_mcp
                    and getattr(existing, "config_schema", None) == schema
                )

                if existing is not None and not managed_mcp:
                    log.warning(f"Configuration {configuration_type} already registered")
                    continue
                if unchanged:
                    continue

                if schema.get('metadata', {}).get('check_connection_supported'):
                    check_connection_func = "applications_configuration_check_connection"
                else:
                    check_connection_func = None
                try:
                    register_kwargs = dict(
                        type_name=configuration_type,
                        section=schema['metadata']['section'],
                        config_schema=schema,
                        validation_func=_INDEXER_CONFIGURATION_VALIDATOR,
                        check_connection_func=check_connection_func
                    )
                    if managed_mcp:
                        register_kwargs["replace"] = True
                    context.rpc_manager.timeout(RPC_CALL_TIMEOUT).configurations_register(
                        **register_kwargs
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
