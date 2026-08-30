from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


PROJECT_CONTEXT_MAX_LEN = 2500
PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN = 300


class ProjectContextUpdate(BaseModel):
    """Request payload for updating project context via PUT endpoint.

    Content and enabled are replaced. Omitted activation_description is preserved
    for compatibility with older clients.
    """
    content: str = Field('', max_length=PROJECT_CONTEXT_MAX_LEN)
    enabled: bool = True
    activation_description: Optional[str] = Field(
        default=None,
        max_length=PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN,
        description="When the model should load the full Project Context. Omit to preserve the existing value.",
    )

    @field_validator('activation_description')
    @classmethod
    def _normalize_activation_description(cls, value):
        if value is None:
            return None
        return ' '.join(value.split()) or None


class ProjectContextDetail(BaseModel):
    """Response model for project context GET/PUT endpoints.

    Represents the current state of project context configuration.
    """
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    content: str = ''
    enabled: bool = True
    activation_description: Optional[str] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_config(cls, config: Optional[dict]) -> 'ProjectContextDetail':
        """Convert Configuration RPC response to ProjectContextDetail.

        Args:
            config: Configuration dict from RPC with structure:
                {
                    'id': int,
                    'data': {
                        'content': str,
                        'enabled': bool,
                        'activation_description': Optional[str],
                    },
                    'updated_at': datetime
                }

        Returns:
            ProjectContextDetail with defaults if config is None or data is missing.
        """
        if config is None:
            return cls()
        data = config.get('data') or {}
        return cls(
            id=config.get('id'),
            content=data.get('content', ''),
            enabled=data.get('enabled', True),
            activation_description=data.get('activation_description'),
            updated_at=config.get('updated_at'),
        )
