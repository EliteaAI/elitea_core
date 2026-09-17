from yaml import safe_load, safe_dump, YAMLError

_RESERVED_STATE_VARS = frozenset({'tool_outcomes', 'last_tool_outcome'})


def validate_yaml_from_str(value: str) -> dict:
    try:
        parsed_data = safe_load(value)
        if not isinstance(parsed_data, dict):
            raise ValueError("Pipeline instruction YAML is not valid")
        state = parsed_data.get('state') or {}
        conflicting = set(state.keys()) & _RESERVED_STATE_VARS
        if conflicting:
            raise ValueError(
                f"State variable names {sorted(conflicting)} are reserved for system use "
                f"and cannot be defined by the user."
            )
        return parsed_data
    except YAMLError as e:
        raise ValueError(f"Invalid pipeline YAML data: {e}")


def from_str_to_yaml(value: dict) -> str:
    try:
        return str(safe_dump(value, sort_keys=False))
    except YAMLError as e:
        raise ValueError(f"Error converting to YAML: {e}")
