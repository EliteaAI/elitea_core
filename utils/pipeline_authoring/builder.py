"""What the Agent & Pipeline Builder's two pipeline tools return, and their descriptions.

The platform exposes ``get_pipeline_reference`` and ``validate_pipeline`` as Builder MCP
tools. Their descriptions are sent on every model call while the Builder is enabled, so they
stay short and point at the reference instead of carrying it; the reference itself comes back
only as a tool result, one topic at a time.

Like the rest of this package the module imports nothing else from the SDK, so the platform
can keep a byte-identical copy.
"""

from typing import List, Optional, Union

from .knowledge import get_reference, list_topics
from .validator import ValidationContext, validate_pipeline

# How many reference topics one call may return; overview plus a few node types is the norm.
MAX_TOPICS_PER_CALL = 6

REFERENCE_TOOL_DESCRIPTION = (
    'Pipeline authoring reference: node types, their required fields, wiring and patterns. '
    'Before writing or editing pipeline YAML, call it once with "overview" and the node types '
    'you will use (e.g. ["overview", "node:llm", "node:router"]); load a pattern when the task '
    'matches one. Topics: {topics}.'
)

VALIDATE_TOOL_DESCRIPTION = (
    'Check pipeline YAML before saving it. Returns errors (fix all before saving) and warnings, '
    'each with the node, the field and a suggested fix. Validate again after fixing. Saving '
    'does not depend on calling this, but a pipeline with errors will not build or will '
    'misbehave when it runs.'
)


def reference_tool_description() -> str:
    """The reference tool's description, listing every topic key."""
    return REFERENCE_TOOL_DESCRIPTION.format(topics=', '.join(t['key'] for t in list_topics()))


def builder_reference(topics: Union[str, List[str], None] = None) -> str:
    """Reference pages for the Builder; with no topic, the overview."""
    if isinstance(topics, str):
        topics = [part.strip() for part in topics.split(',') if part.strip()]
    keys = list(topics or ['overview'])
    if len(keys) > MAX_TOPICS_PER_CALL:
        extra = ', '.join(keys[MAX_TOPICS_PER_CALL:])
        return (get_reference(keys[:MAX_TOPICS_PER_CALL]).rstrip()
                + f'\n\n---\n\nNot loaded (at most {MAX_TOPICS_PER_CALL} topics per call): {extra}.\n')
    return get_reference(keys)


def validate_for_builder(pipeline_yaml: str, trigger: Optional[str] = None,
                         tool_schemas: Optional[dict] = None) -> dict:
    """``validate_pipeline`` with a one-line verdict the model can act on first."""
    result = validate_pipeline(pipeline_yaml, ValidationContext(trigger=trigger, tool_schemas=tool_schemas))
    errors, warnings = len(result['errors']), len(result['warnings'])
    if errors:
        summary = f'{errors} error(s) to fix before saving' + (f', {warnings} warning(s)' if warnings else '') + '.'
    elif warnings:
        summary = f'No errors. {warnings} warning(s) to review; saving is fine.'
    else:
        summary = 'Valid. No errors or warnings.'
    return {'valid': result['ok'], 'summary': summary, 'errors': result['errors'], 'warnings': result['warnings']}
