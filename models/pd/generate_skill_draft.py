import re
from typing import Optional

from pydantic import BaseModel, Field, ConfigDict, constr, field_validator, model_validator

from .predict_llm import LLMSettingsRequest
from .skill import RESERVED_NAME_WORDS, validate_skill_name

NAME_MAX_LENGTH = 64
DESCRIPTION_MAX_LENGTH = 2304
INSTRUCTIONS_MAX_LENGTH = 5000


_CONNECTORS = frozenset({"with", "using", "via", "by", "for", "from", "powered"})


def _carries_reserved_word(token: str) -> bool:
    return any(word in token for word in RESERVED_NAME_WORDS)


def _excise_reserved_words(token: str) -> str:
    return "".join(re.split("|".join(RESERVED_NAME_WORDS), token))


def _drop_reserved_words(tokens: list) -> list:
    """The tokens minus the reserved ones, and minus the connectors that only introduced them.

    A connector is orphaned by its *adjacency* to the removal, not by its position in the name:
    'claude powered pr reviewer' strands a leading one and 'pr review with claude for teams'
    strands two in the middle, so both sides of each dropped token are swept.
    """
    kept = []
    follows_drop = False
    for token in tokens:
        if _carries_reserved_word(token):
            while kept and kept[-1] in _CONNECTORS:
                kept.pop()
            follows_drop = True
            continue
        if follows_drop and token in _CONNECTORS:
            continue
        follows_drop = False
        kept.append(token)
    return kept


def _coerce_skill_name(value: str) -> str:
    """Slugify, dropping the words :func:`validate_skill_name` reserves.

    A model asked to name a skill after itself produces 'claude-code-reviewer', which is a rule
    violation rather than a failed generation - the caller gets the whole draft back as a 422 for
    one word. The other two fields already truncate rather than reject; this brings the name in
    line, and a name that is *only* a reserved word still fails.

    Whole words are dropped, not substrings: the rule tests by substring, so 'philanthropic' breaks
    it, but excising the match would leave the fragment 'phil'.

    Excision is the fallback for a token with no word boundary to cut on ('claudebot'), taken only
    when dropping would leave nothing at all, so a salvageable name is never lost to a 422. It reads
    the reserved tokens rather than all of them, so it cannot resurrect a connector just swept.
    """
    tokens = [token for token in re.split(r"[^a-z0-9]+", value.lower()) if token]

    kept = _drop_reserved_words(tokens)
    if not kept:
        carried_reserved = filter(_carries_reserved_word, tokens)
        kept = [excised for excised in map(_excise_reserved_words, carried_reserved) if excised]

    return "-".join(kept)[:NAME_MAX_LENGTH].strip("-")


class GenerateSkillDraftRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "user_description": "A skill that reviews GitHub pull requests for security issues",
                "llm_settings": {
                    "model_name": "gpt-5-mini",
                    "max_tokens": 2048,
                    "temperature": 0,
                },
            }
        }
    )

    user_description: constr(strip_whitespace=True, min_length=1) = Field(
        description="Natural-language description of the desired skill"
    )
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description="LLM model override. If not provided, "
        "uses the project's default model with temperature=0.7 and max_tokens=4096.",
    )
    skill_id: Optional[int] = Field(
        default=None,
        description="Skill ID to edit. When provided with version_id, enables edit mode.",
    )
    version_id: Optional[int] = Field(
        default=None,
        description="Version ID to edit. Required when skill_id is provided.",
    )

    @model_validator(mode="after")
    def validate_edit_params(self):
        """Ensure both skill_id and version_id are provided together."""
        if (self.skill_id is None) != (self.version_id is None):
            raise ValueError("Both skill_id and version_id must be provided for edit mode")
        return self

    @property
    def is_edit_mode(self) -> bool:
        return self.skill_id is not None and self.version_id is not None


class GenerateSkillDraftResponse(BaseModel):
    """AI-generated skill draft, coerced to be creatable.

    The fields are normalized to fit the skill entity's constraints rather than
    rejected when slightly off, so a usable draft always reaches the review form:
    ``name`` is slugified and stripped of reserved words, then checked against
    :func:`validate_skill_name` (the same rule the skill create API enforces —
    single source of truth), and
    ``description``/``instructions`` are truncated to their caps. A 422 is only
    raised when a required field is missing/empty or the name cannot be salvaged
    into a valid slug — i.e. a genuine generation failure the user retries (AC9).
    There are deliberately NO suggested toolkits/agents/pipelines/MCPs for skills.
    """

    name: str = Field(
        min_length=1,
        max_length=NAME_MAX_LENGTH,
        description="Skill name, slugified (lowercase letters/digits/hyphens, no leading/trailing hyphen)",
    )
    description: str = Field(
        min_length=1,
        max_length=DESCRIPTION_MAX_LENGTH,
        description=f"Skill description (truncated to {DESCRIPTION_MAX_LENGTH} characters)",
    )
    instructions: str = Field(
        min_length=1,
        max_length=INSTRUCTIONS_MAX_LENGTH,
        description=f"Skill instructions in Markdown (truncated to {INSTRUCTIONS_MAX_LENGTH} characters)",
    )

    @field_validator("name", mode="before")
    @classmethod
    def _normalize_name(cls, v):
        return validate_skill_name(_coerce_skill_name(v)) if isinstance(v, str) else v

    @field_validator("description", mode="before")
    @classmethod
    def _truncate_description(cls, v):
        return v[:DESCRIPTION_MAX_LENGTH].rstrip() if isinstance(v, str) else v

    @field_validator("instructions", mode="before")
    @classmethod
    def _truncate_instructions(cls, v):
        return v[:INSTRUCTIONS_MAX_LENGTH].rstrip() if isinstance(v, str) else v
