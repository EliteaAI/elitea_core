"""Pipeline authoring for the Elitea Builder (#6388): reference pages and the save check.

The Builder's `reference` tool serves the SDK knowledge pack (utils/pipeline_authoring) for `pipeline:<topic>`, and
`propose` refuses a pipeline change that introduces validator errors, so a broken pipeline never reaches the
Changes card. Kept apart from service.py so it imports only the pure pipeline_authoring package.
"""
from ..pipeline_authoring import ValidationContext, validate_pipeline
from ..pipeline_authoring.builder import MAX_TOPICS_PER_CALL
from ..pipeline_authoring.knowledge import list_topics, render_entry

TOPIC_PREFIX = 'pipeline:'
# `pipeline:overview`, `pipeline:node:llm`, `pipeline:pattern:loop`, ... ; several are comma-separated.


def requested_pages(topic: str) -> list:
    """The page keys named by `pipeline:<page>[,<page>...]`; the overview when none is named."""
    wanted = [t.strip() for t in topic[len(TOPIC_PREFIX):].split(',') if t.strip()]
    keys = [t[len(TOPIC_PREFIX):] if t.startswith(TOPIC_PREFIX) else t for t in wanted]
    return list(dict.fromkeys(keys)) or ['overview']


def reference_pages(topic: str) -> dict:
    """``{"pages": {key: text}, "unknown": [...], "not_loaded": [...]}`` for a `pipeline:` topic.

    One entry per page, so the SDK BuilderMiddleware can keep each page only in its first result in the
    conversation (#6388), the way it keeps each playbook once.
    """
    keys = requested_pages(topic)
    served, not_loaded = keys[:MAX_TOPICS_PER_CALL], keys[MAX_TOPICS_PER_CALL:]
    pages, unknown = {}, []
    for key in served:
        text = render_entry(key)
        if text is None:
            unknown.append(key)
        else:
            pages[key] = text
    return {'pages': pages, 'unknown': unknown, 'not_loaded': not_loaded}


def topics() -> list:
    return [f"{TOPIC_PREFIX}{t['key']}" for t in list_topics()]


# Rules that report several problems on one node and field, told apart only by the message: R36 reports each missing
# required parameter with field `input_mapping`. For them the message is part of the key, so an old missing parameter
# does not hide a new one. Other messages carry details that change with unrelated edits (a trigger name, a count, a
# line number), so keying on them would make an old error look new.
_PER_MESSAGE_RULES = frozenset({'R36'})


def _key(finding):
    key = (finding['rule'], finding['node_id'], finding['field'])
    return key + (finding['message'],) if finding['rule'] in _PER_MESSAGE_RULES else key


DOCUMENT_LEVEL = 'R20'  # "not a pipeline at all"


def context_for(trigger=None, tool_schemas=None):
    """A ValidationContext, or None when there is nothing the validator could use."""
    return ValidationContext(trigger=trigger, tool_schemas=tool_schemas) if trigger or tool_schemas else None


def new_errors(before, after, after_context=None, before_context=None) -> list:
    """Validator errors in `after` that `before` did not have; every error when there is no `before`.

    An edit is refused only for what it breaks, so a pipeline saved before a rule existed can still be edited.
    "Not a pipeline at all" never counts as already present. Each side is checked with its own context: a change that
    also moves the trigger to a schedule must not excuse the HITL node the schedule makes fatal.
    """
    existing = set()
    if before:
        existing = {_key(f) for f in validate_pipeline(before, before_context)['errors']
                    if not (f['rule'] == DOCUMENT_LEVEL and f['node_id'] is None and f['field'] is None)}
    return [f for f in validate_pipeline(after or '', after_context)['errors'] if _key(f) not in existing]


def health(instructions, context=None) -> dict:
    """What the validator finds in a stored pipeline, for `read`: the Builder can fix it while it is there."""
    result = validate_pipeline(instructions or '', context)
    return {'errors': as_errors(result['errors']), 'warnings': as_errors(result['warnings'])}


def trigger_of(state) -> str:
    """How the pipeline is started, from its version's `pipeline_settings.trigger` (chat when unset)."""
    trigger = ((state or {}).get('pipeline_settings') or {}).get('trigger') or {}
    return trigger.get('type') if isinstance(trigger, dict) else None


def as_errors(findings) -> list:
    """Validator findings in the shape `propose` returns ({field, message, suggestion})."""
    return [{'field': f"instructions.nodes.{f['node_id']}.{f['field']}" if f['node_id'] else
             f"instructions.{f['field'] or 'yaml'}",
             'message': f"[{f['rule']}] {f['message']}", 'suggestion': f['suggestion']} for f in findings]


REFUSAL = ('The pipeline YAML has errors that would stop it from building or running; nothing was proposed. '
           'Fix each error and propose again.')
