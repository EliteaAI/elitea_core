from yaml import safe_load, safe_dump, YAMLError

from .pipeline_validator import validate_pipeline_yaml


def validate_yaml_from_str(value: str) -> dict:
    """Parse and lightly validate pipeline YAML.

    Wraps :func:`validate_pipeline_yaml` and raises ``ValueError`` on any
    error-severity finding so that existing call sites keep their current
    behaviour.  Incomplete pipelines (empty nodes list, missing entry_point,
    draft state) are accepted — those produce warnings only.
    Use :func:`utils.pipeline_validator.validate_pipeline_yaml` directly when
    structured error details are needed.
    """
    result = validate_pipeline_yaml(value)
    if not result.valid:
        messages = "; ".join(e.message for e in result.errors)
        raise ValueError(f"Invalid pipeline YAML: {messages}")
    try:
        return safe_load(value)
    except YAMLError as e:
        raise ValueError(f"Invalid pipeline YAML data: {e}")


def from_str_to_yaml(value: dict) -> str:
    try:
        return str(safe_dump(value, sort_keys=False))
    except YAMLError as e:
        raise ValueError(f"Error converting to YAML: {e}")
