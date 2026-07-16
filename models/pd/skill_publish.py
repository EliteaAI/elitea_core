"""Pydantic models for skill-publish validation."""
from typing import Literal

from pydantic import BaseModel, Field

from .publish import PublishAIResult, VERSION_NAME_PATTERN


class PublishSkillValidateRequest(BaseModel):
    version_name: str = Field(
        ...,
        description="Skill version name to validate",
        pattern=VERSION_NAME_PATTERN,
    )
    category: str | None = Field(
        None,
        description="Selected skill category (defaults to 'Other' when omitted)",
    )


class SkillPublishRequest(BaseModel):
    version_name: str = Field(..., pattern=VERSION_NAME_PATTERN)
    category: str | None = None
    validation_token: str | None = None


class SkillUnpublishRequest(BaseModel):
    reason: str | None = None


SkillPublishAIResult = PublishAIResult


class SkillValidationResult(BaseModel):
    status: Literal['PASS', 'WARN', 'FAIL']
    critical_issues: list[dict] = Field(default_factory=list)
    warnings: list[dict] = Field(default_factory=list)
    recommendations: list[dict] = Field(default_factory=list)
    summary: str = ''
    counts: dict = Field(default_factory=dict)
    ai_validation_available: bool = True
    validation_token: str | None = None
