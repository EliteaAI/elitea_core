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

""" Methods for handling MCP prebuilt server configurations """

from typing import Optional, Dict, Any
from pylon.core.tools import web, log  # pylint: disable=E0611,E0401


def normalize_mcp_toolkit_name(name: str) -> str:
    """
    Normalize MCP toolkit name/type for case-insensitive matching.

    Converts to lowercase, removes/normalizes whitespace, and removes the 'mcp_' prefix
    to handle variations in toolkit naming (e.g., "mcp_epam_presales" -> "epam_presales",
    "Epam Presales" -> "epam_presales").

    Args:
        name: The toolkit name or type to normalize

    Returns:
        Normalized name (lowercase, spaces converted to underscores, 'mcp_' prefix removed)
    """
    if not name:
        return ""
    # Convert to lowercase, replace spaces with underscores, strip
    normalized = name.lower().replace(" ", "_").strip()
    # Remove 'mcp_' prefix if present
    if normalized.startswith("mcp_"):
        normalized = normalized[4:]
    return normalized


class Method:
    @web.method()
    def mcp_prebuilt_config_collected(self, event, payload: dict):
        """
        Store MCP prebuilt server configurations received from indexer_worker.

        Args:
            event: Event name
            payload: Dict of MCP server configurations keyed by normalized name
        """
        self.mcp_prebuilt_configs = payload
        log.info(f"MCP prebuilt configurations collected: {len(self.mcp_prebuilt_configs)} server(s)")

    @web.method()
    def get_mcp_prebuilt_config(self, toolkit_type: str) -> Optional[Dict[str, Any]]:
        """
        Get prebuilt MCP server configuration for a given toolkit type.

        Args:
            toolkit_type: The toolkit type (e.g., "mcp_epam_presales", "mcp_github_copilot")

        Returns:
            Dict with configuration settings if found, None otherwise
        """
        log.info(f'LIST OF MCP PREBUILT CONFIGS: {list(self.mcp_prebuilt_configs.keys()) if hasattr(self, "mcp_prebuilt_configs") else "No configs loaded"}')
        if not hasattr(self, 'mcp_prebuilt_configs') or not self.mcp_prebuilt_configs:
            log.debug("No MCP prebuilt configurations available")
            return None

        # Normalize the search key
        normalized_key = normalize_mcp_toolkit_name(toolkit_type)

        config = self.mcp_prebuilt_configs.get(normalized_key)
        if config:
            log.debug(f"Found MCP prebuilt config for '{toolkit_type}' -> '{normalized_key}'")
            return config

        log.debug(f"No MCP prebuilt config found for '{toolkit_type}' (normalized: '{normalized_key}')")
        return None

    @web.method()
    def resolve_mcp_prebuilt_settings(self, raw_data: dict) -> dict:
        """
        Resolve MCP toolkit settings with fallback to prebuilt configuration.

        This function processes pre-built MCP toolkits (where toolkit_type starts with 'mcp_').
        It merges the prebuilt configuration from pylon config with the incoming request data.

        Priority order for each setting:
        1. If raw_data already has a value - use it (no override)
        2. Otherwise, try to get from prebuilt configuration
        3. If not found, return raw_data unchanged

        Args:
            raw_data: Request data dict with 'toolkit_type' and other settings

        Returns:
            Updated raw_data with settings merged from prebuilt config where missing
        """
        toolkit_type = raw_data.get('toolkit_type', '')

        # Only process pre-built MCP toolkits (type starts with 'mcp_')
        if not toolkit_type or not toolkit_type.startswith('mcp_'):
            log.debug(f"Skipping MCP prebuilt resolution - toolkit type '{toolkit_type}' is not a pre-built MCP toolkit")
            return raw_data

        log.debug(f"Resolving MCP prebuilt settings for toolkit_type={toolkit_type}")

        prebuilt_config = self.get_mcp_prebuilt_config(toolkit_type)
        if not prebuilt_config:
            log.debug(f"No prebuilt config found for toolkit type '{toolkit_type}'")
            return raw_data

        # Create a copy to avoid modifying the original
        result = dict(raw_data)

        # List of fields that can be filled from prebuilt config
        fillable_fields = ['url', 'headers', 'timeout', 'ssl_verify', 'client_id', 'client_secret', 'base_url']

        injected_fields = []
        for field in fillable_fields:
            # Only inject if field is missing or empty in raw_data
            if not result.get(field) and prebuilt_config.get(field):
                result[field] = prebuilt_config[field]
                injected_fields.append(field)

        if injected_fields:
            log.debug(f"Injected {len(injected_fields)} field(s) from prebuilt config for '{toolkit_type}': {', '.join(injected_fields)}")
        else:
            log.debug(f"No fields needed injection for toolkit type '{toolkit_type}'")

        return result

