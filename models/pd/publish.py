"""Pydantic models for publish request validation and AI response parsing."""
import json
import re
from typing import Any

from pydantic import BaseModel, Field, model_validator


VERSION_NAME_PATTERN = r'^[a-zA-Z0-9._-]{1,50}$'


class PublishValidateRequest(BaseModel):
    """Request body for POST /publish_validate."""
    version_name: str = Field(
        ...,
        description="Version name to validate",
        pattern=VERSION_NAME_PATTERN,
    )


class PublishRequest(BaseModel):
    """Request body for POST /publish."""
    version_name: str = Field(
        ...,
        description="Version name for the published agent",
        pattern=VERSION_NAME_PATTERN,
    )
    validation_token: str | None = Field(
        None,
        description="Token from a prior /publish_validate call (skips inline validation)",
    )


class UnpublishRequest(BaseModel):
    """Request body for POST /unpublish."""
    reason: str | None = Field(
        None,
        description="Optional reason for unpublishing the agent version",
    )


_JSON_FENCE_RE = re.compile(
    r'```(?:json)?\s*\n(.*?)\n\s*```', re.DOTALL,
)


def _extract_json(text: str) -> dict | None:
    """Parse JSON from a string, stripping markdown fences."""
    text = text.strip()
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except (json.JSONDecodeError, TypeError):
        pass
    m = _JSON_FENCE_RE.search(text)
    if m:
        try:
            obj = json.loads(m.group(1))
            if isinstance(obj, dict):
                return obj
        except (json.JSONDecodeError, TypeError):
            pass
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end > start:
        try:
            obj = json.loads(text[start:end + 1])
            if isinstance(obj, dict):
                return obj
        except (json.JSONDecodeError, TypeError):
            pass
    return None


class ValidationIssue(BaseModel):
    """Single critical_issue or warning from AI validation."""
    field: str = ''
    issue: str = ''
    fix: str = ''
    context: str | None = None
    source: str = 'ai'


class ValidationRecommendation(BaseModel):
    """Single recommendation from AI validation."""
    field: str = 'Generic'
    suggestion: str = ''
    context: str | None = None
    source: str = 'ai'


class PublishAIResult(BaseModel):
    """Parses raw predict_sio result into validated AI output.

    Accepts the full ``{"result": {"chat_history": [...]}}`` dict
    returned by ``predict_sio`` and extracts + validates the LLM
    JSON response through ``model_validator(mode='before')``.
    """
    summary: str = ''
    critical_issues: list[ValidationIssue] = Field(
        default_factory=list,
    )
    warnings: list[ValidationIssue] = Field(
        default_factory=list,
    )
    recommendations: list[ValidationRecommendation] = Field(
        default_factory=list,
    )

    @model_validator(mode='before')
    @classmethod
    def extract_from_predict_result(cls, data: Any) -> dict:
        """Walk predict_sio envelope → chat_history → JSON."""
        if not isinstance(data, dict):
            raise ValueError('Expected dict from predict_sio')

        # Unwrap {"result": {...}} envelope
        inner = data.get('result', data)
        if not isinstance(inner, dict):
            raise ValueError(
                'predict_sio result is not a dict',
            )

        # Extract last assistant message from chat_history
        text = _extract_chat_response(inner)
        if text is None:
            raise ValueError(
                'No assistant response in chat_history',
            )

        # Parse JSON from LLM text
        parsed = _extract_json(text)
        if parsed is None:
            raise ValueError(
                'Assistant response is not valid JSON',
            )

        return parsed

    @model_validator(mode='after')
    def filter_empty_items(self) -> 'PublishAIResult':
        """Drop items with no meaningful content."""
        self.critical_issues = [
            i for i in self.critical_issues
            if i.field or i.issue
        ]
        self.warnings = [
            i for i in self.warnings
            if i.field or i.issue
        ]
        self.recommendations = [
            r for r in self.recommendations
            if r.field or r.suggestion
        ]
        return self


def _extract_chat_response(result_dict: dict) -> str | None:
    """Pull last assistant message content from chat_history."""
    chat_history = result_dict.get('chat_history')
    if not isinstance(chat_history, list) or not chat_history:
        return None
    for msg in reversed(chat_history):
        if not isinstance(msg, dict):
            continue
        role = msg.get('role', '')
        msg_type = msg.get('type', '')
        if role in ('assistant', 'ai') or msg_type == 'ai':
            content = msg.get('content', '')
            if isinstance(content, str) and content.strip():
                return content
    return None
