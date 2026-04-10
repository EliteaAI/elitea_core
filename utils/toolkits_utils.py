from tools import this, context

from pylon.core.tools import log

from ..utils.application_tools import find_suggested_toolkit_name_field
from ..utils.toolkit_security import filter_blocked_toolkits, filter_tools_in_schema, get_toolkit_security_config


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
    # Step 1: resolve personal project ID once — reused by both provider_hub and mcp_sse
    personal_project_id = None
    try:
        personal_project_id = context.rpc_manager.timeout(15).projects_get_personal_project_id(user_id)
    except Exception:
        log.warning("Failed to resolve personal project ID for user %s", user_id)

    # Step 2/3: static schemas are already filtered and have name_required pre-computed at
    # startup (in toolkits_collected). Shallow-copy the outer dict — values are not mutated.
    toolkit_schemas = dict(this.module.toolkit_schemas)

    # Step 1: pass the already-resolved personal_project_id to avoid duplicate RPC calls
    provider_hub_schemas = this.module.get_tool_schemas_provider_hub(
        project_id, user_id, personal_project_id=personal_project_id
    )
    mcp_schemas = this.module.get_tool_schemas_mcp_sse(
        project_id, user_id, personal_project_id=personal_project_id
    )

    # Step 4: read security config once for the whole request
    security_config = get_toolkit_security_config()

    # Step 3: apply filtering and set name_required only for dynamic (non-static) schemas
    dynamic_schemas = {**provider_hub_schemas, **mcp_schemas}
    dynamic_schemas = filter_blocked_toolkits(dynamic_schemas, config=security_config)
    for k in list(dynamic_schemas.keys()):
        dynamic_schemas[k] = filter_tools_in_schema(dynamic_schemas[k], config=security_config)
        dynamic_schemas[k]['name_required'] = find_suggested_toolkit_name_field(k) is None

    toolkit_schemas.update(dynamic_schemas)
    return toolkit_schemas
