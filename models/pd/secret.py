import re

from pydantic import BaseModel, Field, field_validator


# Vault secret keys are embedded into ``{{secret.<key>}}`` references, so they must
# be limited to a safe identifier charset to avoid breaking reference resolution.
_SECRET_KEY_PATTERN = re.compile(r'^[A-Za-z0-9_.-]+$')

# Coarse per-secret size cap to keep an LLM/MCP caller from pushing large blobs
# into Vault (per-secret quota / DoS guard).
SECRET_MAX_VALUE_LEN = 64 * 1024


class SecretCreateModel(BaseModel):
    """Request payload for creating a project secret in Vault."""
    key: str = Field(..., min_length=1, max_length=128,
                     description="Secret name. Used in the returned {{secret.<key>}} reference.")
    value: str = Field(..., min_length=1, max_length=SECRET_MAX_VALUE_LEN,
                       description="Raw secret value to store in Vault.")
    overwrite: bool = Field(
        False,
        description="Allow replacing an existing secret with the same key. "
                    "When false (default), a collision is rejected with HTTP 409.",
    )

    @field_validator('key')
    @classmethod
    def validate_key(cls, v: str) -> str:
        if not _SECRET_KEY_PATTERN.match(v):
            raise ValueError(
                "Secret key may only contain letters, digits, '_', '.' and '-'"
            )
        return v
