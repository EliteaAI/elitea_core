"""The run input and the conversation preview of a webhook delivery (#7454); the body is never stored."""
import json

from .input_guardrail import redacted_input

PREVIEW_LIMIT = 500
# Redacted before the cut; wider than the guardrail's longest match (16 KB), so no value is cut in half
REDACT_WINDOW = 32_768


def parse_body(payload: bytes) -> str:
    """The body as indented JSON; `{"raw_payload": text}` when it is not JSON or too deeply nested to write back."""
    text = (payload or b'').decode('utf-8', errors='replace')
    try:
        # The indented (pure Python) writer overflows on nesting json.loads accepts
        return json.dumps(json.loads(text), indent=2)
    except (ValueError, RecursionError):
        return json.dumps({'raw_payload': text}, indent=2)


def shape_input(entity_type: str, prompt: str, body_json: str) -> str:
    """Pipelines get the body; agents get the prompt plus the body as a fenced JSON block, or the body alone."""
    if entity_type == 'pipeline' or not (prompt or '').strip():
        return body_json
    return f'{prompt}\n\n```json\n{body_json}\n```'


def payload_preview(body_json: str) -> str:
    preview = redacted_input(body_json[:REDACT_WINDOW])
    cut = len(preview) > PREVIEW_LIMIT or len(body_json) > REDACT_WINDOW
    return preview[:PREVIEW_LIMIT] + '...' if cut else preview


def trigger_message(webhook_type: str, body_json: str) -> str:
    """The trigger message of the run conversation, as the legacy webhook writes it."""
    return f'[Webhook triggered: {webhook_type}]\n\nPayload:\n```json\n{payload_preview(body_json)}\n```'
