import json
import re
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


_ELITEA_TITLE_PATTERN = re.compile(r'^[A-Za-z0-9_.\- ]+$')

# Coarse cap on the serialized credential payload to keep an LLM/MCP caller from
# pushing oversized / deeply-nested blobs through the configurations service.
CREDENTIAL_MAX_DATA_LEN = 64 * 1024


class CredentialCreateModel(BaseModel):
    """Request payload for creating a credential (configuration) in the project.

    A credential is stored via the ``configurations`` module. Its ``data`` fields
    may embed ``{{secret.<key>}}`` references produced by the create-secret endpoint
    so that sensitive values live in Vault rather than in the configuration record.
    """
    type: str = Field(..., min_length=1,
                      description="Credential/configuration type (e.g. the integration type it belongs to).")
    label: str = Field(..., min_length=1,
                       description="Human-readable display name for the credential.")
    elitea_title: Optional[str] = Field(
        None,
        description="Unique key for the credential within the project. Derived from label when omitted.",
    )
    data: dict = Field(default_factory=dict,
                       description="Credential fields. May contain {{secret.<key>}} Vault references.")
    section: Optional[str] = Field(None, description="Optional configuration section.")
    source: Optional[str] = Field(None, description="Optional configuration source marker.")

    @field_validator('elitea_title')
    @classmethod
    def validate_elitea_title(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        # Strip before validating so " foo " and "foo" cannot create duplicate
        # credentials that differ only by surrounding whitespace.
        v = v.strip()
        if not v:
            return None
        if not _ELITEA_TITLE_PATTERN.match(v):
            raise ValueError(
                "elitea_title may only contain letters, digits, spaces, '_', '.' and '-'"
            )
        return v

    @field_validator('data')
    @classmethod
    def validate_data_size(cls, v: dict) -> dict:
        if len(json.dumps(v, default=str)) > CREDENTIAL_MAX_DATA_LEN:
            raise ValueError(
                f"Credential data exceeds the maximum size of {CREDENTIAL_MAX_DATA_LEN} bytes"
            )
        return v

    @model_validator(mode='after')
    def default_elitea_title(self) -> 'CredentialCreateModel':
        if not self.elitea_title:
            self.elitea_title = self.label.strip()
        return self

    def to_payload(self, project_id: int) -> dict[str, Any]:
        """Build the payload expected by ``configurations_create_if_not_exists``."""
        payload: dict[str, Any] = {
            'project_id': project_id,
            'elitea_title': self.elitea_title,
            'label': self.label,
            'type': self.type,
            'data': self.data,
        }
        if self.section is not None:
            payload['section'] = self.section
        if self.source is not None:
            payload['source'] = self.source
        return payload
