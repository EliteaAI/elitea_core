import re

from pydantic import BaseModel, Field, field_validator


# Vault secret keys are embedded into ``{{secret.<key>}}`` references, so they must
# be limited to a safe identifier charset to avoid breaking reference resolution.
_SECRET_KEY_PATTERN = re.compile(r'^[A-Za-z0-9_.-]+$')


class SecretCreateModel(BaseModel):
    """Request payload for creating (or overwriting) a project secret in Vault."""
    key: str = Field(..., min_length=1, max_length=128,
                     description="Secret name. Used in the returned {{secret.<key>}} reference.")
    value: str = Field(..., min_length=1,
                       description="Raw secret value to store in Vault.")

    @field_validator('key')
    @classmethod
    def validate_key(cls, v: str) -> str:
        if not _SECRET_KEY_PATTERN.match(v):
            raise ValueError(
                "Secret key may only contain letters, digits, '_', '.' and '-'"
            )
        return v
