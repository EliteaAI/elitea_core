"""The run input and the conversation preview of a webhook delivery (#7454); the body is never stored."""
import json

from .input_guardrail import redacted_input

PREVIEW_LIMIT = 500


def parse_body(payload: bytes):
    """The JSON body, or `{"raw_payload": text}` when it is not JSON."""
    text = (payload or b'').decode('utf-8', errors='replace')
    try:
        return json.loads(text)
    except ValueError:
        return {'raw_payload': text}


def shape_input(entity_type: str, prompt: str, body) -> str:
    """Pipelines get the body; agents get the prompt plus the body as a fenced JSON block, or the body alone."""
    body_json = json.dumps(body, indent=2)
    if entity_type == 'pipeline' or not (prompt or '').strip():
        return body_json
    return f'{prompt}\n\n```json\n{body_json}\n```'


def payload_preview(body_json: str) -> str:
    # Redacted before the cut, so a cut cannot leave half a token unmatched
    preview = redacted_input(body_json)
    return preview[:PREVIEW_LIMIT] + '...' if len(preview) > PREVIEW_LIMIT else preview


def trigger_message(webhook_type: str, body) -> str:
    """The trigger message of the run conversation, as the legacy webhook writes it."""
    return f'[Webhook triggered: {webhook_type}]\n\nPayload:\n```json\n{payload_preview(json.dumps(body, indent=2))}\n```'
