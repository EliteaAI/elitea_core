"""
Toolkit Security Utilities

Provides functions to check if toolkits or tools are blocked based on
deployment-level configuration. This is a security feature to prevent
destructive or risky operations from being available to users.

Configuration is loaded from elitea_core.yml:
  toolkit_security:
    blocked_toolkits: [list of toolkit types]
    blocked_tools:
      toolkit_type: [list of tool names]
"""

from typing import Optional
from pylon.core.tools import log
from tools import this


def get_toolkit_security_config() -> dict:
    """
    Get the toolkit security configuration from the plugin descriptor.

    Returns:
        dict with 'blocked_toolkits' (list) and 'blocked_tools' (dict)
    """
    try:
        config = this.module.descriptor.config.get('toolkit_security', {})
        return {
            'blocked_toolkits': config.get('blocked_toolkits', []) or [],
            'blocked_tools': config.get('blocked_tools', {}) or {}
        }
    except Exception as e:
        log.warning(f"Failed to load toolkit_security config: {e}")
        return {'blocked_toolkits': [], 'blocked_tools': {}}


def is_toolkit_blocked(toolkit_type: str) -> bool:
    """
    Check if a toolkit type is blocked.

    Args:
        toolkit_type: The type/name of the toolkit (e.g., 'github', 'shell')

    Returns:
        True if the toolkit is blocked, False otherwise
    """
    config = get_toolkit_security_config()
    blocked = toolkit_type.lower() in [t.lower() for t in config['blocked_toolkits']]
    if blocked:
        log.info(f"[SECURITY] Toolkit '{toolkit_type}' is blocked by deployment configuration")
    return blocked


def is_tool_blocked(toolkit_type: str, tool_name: str) -> bool:
    """
    Check if a specific tool within a toolkit is blocked.

    Args:
        toolkit_type: The type/name of the toolkit
        tool_name: The name of the tool within the toolkit

    Returns:
        True if the tool is blocked, False otherwise
    """
    # First check if the entire toolkit is blocked
    if is_toolkit_blocked(toolkit_type):
        return True

    config = get_toolkit_security_config()
    blocked_tools = config['blocked_tools']

    # Check case-insensitive
    toolkit_lower = toolkit_type.lower()
    for tk, tools in blocked_tools.items():
        if tk.lower() == toolkit_lower:
            if tool_name.lower() in [t.lower() for t in (tools or [])]:
                log.info(f"[SECURITY] Tool '{tool_name}' in toolkit '{toolkit_type}' "
                        f"is blocked by deployment configuration")
                return True

    return False


def get_blocked_tools_for_toolkit(toolkit_type: str, config: dict = None) -> list:
    """
    Get the list of blocked tools for a specific toolkit.

    Args:
        toolkit_type: The type/name of the toolkit
        config: Optional pre-fetched security config to avoid re-reading on every call

    Returns:
        List of blocked tool names for this toolkit
    """
    if config is None:
        config = get_toolkit_security_config()
    blocked_tools = config['blocked_tools']

    toolkit_lower = toolkit_type.lower()
    for tk, tools in blocked_tools.items():
        if tk.lower() == toolkit_lower:
            return tools or []

    return []


def filter_blocked_toolkits(toolkit_schemas: dict, config: dict = None) -> dict:
    """
    Filter out blocked toolkits from a dictionary of toolkit schemas.

    Args:
        toolkit_schemas: Dict mapping toolkit type to schema
        config: Optional pre-fetched security config to avoid re-reading on every call

    Returns:
        Filtered dict with blocked toolkits removed
    """
    if config is None:
        config = get_toolkit_security_config()
    blocked = [t.lower() for t in config['blocked_toolkits']]

    filtered = {}
    for toolkit_type, schema in toolkit_schemas.items():
        if toolkit_type.lower() not in blocked:
            filtered[toolkit_type] = schema
        else:
            log.info(f"[SECURITY] Filtering out blocked toolkit: {toolkit_type}")

    return filtered


def filter_blocked_toolkit_list(toolkit_list: list) -> list:
    """
    Filter out blocked toolkits from a list of toolkit schema dicts.
    Used for filtering schemas received from indexer.

    Args:
        toolkit_list: List of toolkit schema dicts (with 'title' key)

    Returns:
        Filtered list with blocked toolkits removed
    """
    config = get_toolkit_security_config()
    blocked = [t.lower() for t in config['blocked_toolkits']]

    filtered = []
    for schema in toolkit_list:
        toolkit_type = schema.get('title', '')
        if toolkit_type.lower() not in blocked:
            filtered.append(schema)
        else:
            log.info(f"[SECURITY] Filtering out blocked toolkit schema: {toolkit_type}")

    return filtered


def filter_tools_in_schema(schema: dict, config: dict = None) -> dict:
    """
    Filter out blocked tools from a toolkit schema.

    This function filters tools from multiple locations in the schema:
    1. Top-level 'available_tools' list (if present)
    2. 'args_schemas' in selected_tools field (SDK pydantic schema format)
    3. 'allOf' items for Literal types in selected_tools

    Args:
        schema: Toolkit schema dict
        config: Optional pre-fetched security config to avoid re-reading on every call

    Returns:
        Schema with blocked tools filtered out
    """
    from copy import deepcopy

    toolkit_type = schema.get('title', '')
    blocked_tools = get_blocked_tools_for_toolkit(toolkit_type, config=config)

    if not blocked_tools:
        return schema

    blocked_lower = set(t.lower() for t in blocked_tools)
    filtered_count = 0

    # Make a deep copy to avoid modifying the original
    schema = deepcopy(schema)

    # 1. Filter available_tools if present (top-level list)
    if 'available_tools' in schema:
        original_tools = schema['available_tools']
        filtered_tools = [
            tool for tool in original_tools
            if tool.get('name', '').lower() not in blocked_lower
        ]
        filtered_count += len(original_tools) - len(filtered_tools)
        schema['available_tools'] = filtered_tools

    # 2. Filter from properties.selected_tools
    # SDK toolkits store available tools in args_schemas dict (tool_name -> schema)
    properties = schema.get('properties', {})
    selected_tools = properties.get('selected_tools', {})
    if selected_tools:
        # Filter args_schemas dict (tool_name -> schema)
        # Can be directly in selected_tools or in json_schema_extra
        args_schemas = selected_tools.get('args_schemas', {})
        if not args_schemas:
            args_schemas = selected_tools.get('json_schema_extra', {}).get('args_schemas', {})

        if args_schemas and isinstance(args_schemas, dict):
            original_count = len(args_schemas)
            filtered_args = {
                name: tool_schema for name, tool_schema in args_schemas.items()
                if name.lower() not in blocked_lower
            }
            if len(filtered_args) != original_count:
                filtered_count += original_count - len(filtered_args)
                # Update in the correct location
                if 'args_schemas' in selected_tools:
                    schema['properties']['selected_tools']['args_schemas'] = filtered_args
                else:
                    schema['properties']['selected_tools']['json_schema_extra']['args_schemas'] = filtered_args
                log.info(f"[SECURITY] Filtered args_schemas for '{toolkit_type}': "
                        f"removed {list(blocked_lower & set(name.lower() for name in args_schemas.keys()))}")

        # Filter allOf Literal values (list of allowed tool names)
        all_of = selected_tools.get('allOf', [])
        if all_of:
            for i, item in enumerate(all_of):
                if 'items' in item and 'enum' in item['items']:
                    original_enum = item['items']['enum']
                    filtered_enum = [
                        name for name in original_enum
                        if name.lower() not in blocked_lower
                    ]
                    if len(filtered_enum) != len(original_enum):
                        schema['properties']['selected_tools']['allOf'][i]['items']['enum'] = filtered_enum

        # Also filter top-level items.enum in selected_tools
        items = selected_tools.get('items', {})
        if items and 'enum' in items:
            original_enum = items['enum']
            filtered_enum = [
                name for name in original_enum
                if name.lower() not in blocked_lower
            ]
            if len(filtered_enum) != len(original_enum):
                filtered_count += len(original_enum) - len(filtered_enum)
                schema['properties']['selected_tools']['items']['enum'] = filtered_enum
                log.info(f"[SECURITY] Filtered items.enum for '{toolkit_type}': "
                        f"removed {list(blocked_lower & set(name.lower() for name in original_enum))}")

    if filtered_count > 0:
        log.info(f"[SECURITY] Filtered {filtered_count} blocked tool(s) from toolkit '{toolkit_type}'")

    return schema
