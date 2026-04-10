from tools import this
from copy import deepcopy

from ..utils.application_tools import find_suggested_toolkit_name_field
from ..utils.toolkit_security import filter_blocked_toolkits, filter_tools_in_schema


def format_tool_call_as_user_input(tool_name: str, tool_params: dict) -> str:
    """
    Format a toolkit tool call as a human-readable string.
    
    Args:
        tool_name: Name of the tool being called
        tool_params: Dictionary of parameters passed to the tool
        
    Returns:
        Human-readable string representation of the tool call
    """
    if tool_params:
        # Format params nicely
        params_str = ", ".join(
            f"{k}={repr(v) if isinstance(v, str) else v}"
            for k, v in tool_params.items()
        )
        return f"Calling tool '{tool_name}' with parameters: {params_str}"
    else:
        return f"Calling tool '{tool_name}' with no parameters"


def get_mcp_schemas(project_id: int, user_id: int) -> dict:
    """
    Get tool schemas from mcp_sse module if available.
    :param project_id:
    :return: list of tool schemas
    """

    return this.module.get_tool_schemas_mcp_sse(project_id, user_id)


def get_toolkit_schemas(project_id: int, user_id: int) -> dict:
    """
    Get toolkits by aggregating schemas of tools and their configurations
    from provided toolkit models and optionally extending them using provider_hub
    and mcp_sse modules. It ensures each tool's name requirement is determined by
    checking for suggested toolkit name fields.

    Blocked toolkits (configured in elitea_core.yml toolkit_security) are filtered out.

    :param toolkit_models: A dictionary containing models for tool validators and
       their configurations. It should include two keys:
       - 'validators': A dictionary of toolkit validators.
       - 'validators_configuration': A dictionary of toolkit configurations.
    :param project_id: An integer representing the project ID used to fetch
       additional tool schemas from optional modules.
    :return: A dictionary with aggregated tool schemas under the key 'tools' and
       their corresponding configurations under 'tools_configuration'.
    :rtype: Dict
    """
    toolkit_schemas = deepcopy(this.module.toolkit_schemas)

    # Provider Hub schemas are always available (migrated to elitea_core)
    toolkit_schemas.update(this.module.get_tool_schemas_provider_hub(project_id, user_id))

    # MCP SSE schemas are always available (migrated to elitea_core)
    toolkit_schemas.update(this.module.get_tool_schemas_mcp_sse(project_id, user_id))

    # Filter out blocked toolkits (security feature)
    toolkit_schemas = filter_blocked_toolkits(toolkit_schemas)

    # Filter blocked tools within each toolkit and set name_required
    for k in list(toolkit_schemas.keys()):
        toolkit_schemas[k] = filter_tools_in_schema(toolkit_schemas[k])
        toolkit_schemas[k]['name_required'] = find_suggested_toolkit_name_field(k) is None

    return toolkit_schemas
