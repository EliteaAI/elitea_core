from yaml import safe_load, safe_dump, YAMLError


def validate_yaml_from_str(value: str) -> dict:
    try:
        parsed_data = safe_load(value)
        if not isinstance(parsed_data, dict):
            raise ValueError("Pipeline instruction YAML is not valid")
        return parsed_data
    except YAMLError as e:
        raise ValueError(f"Invalid pipeline YAML data: {e}")


def from_str_to_yaml(value: dict) -> str:
    try:
        return str(safe_dump(value, sort_keys=False))
    except YAMLError as e:
        raise ValueError(f"Error converting to YAML: {e}")
