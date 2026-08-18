import json


def format_tool_call_as_user_input(tool_name: str, tool_params: dict) -> str:
    if tool_params:
        params_json = json.dumps(tool_params, ensure_ascii=False, default=str)
        return f"Calling tool '{tool_name}' with parameters: {params_json}"
    else:
        return f"Calling tool '{tool_name}' with no parameters"
