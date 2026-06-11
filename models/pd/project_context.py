from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


PROJECT_CONTEXT_MAX_LEN = 2500


class ProjectContextUpdate(BaseModel):
    """Request payload for updating project context via PUT endpoint.

    Full replacement - both fields are always set (not partial update).
    """
    content: str = Field('', max_length=PROJECT_CONTEXT_MAX_LEN)
    enabled: bool = True


class ProjectContextDetail(BaseModel):
    """Response model for project context GET/PUT endpoints.

    Represents the current state of project context configuration.
    """
    model_config = ConfigDict(from_attributes=True)

    id: Optional[int] = None
    content: str = ''
    enabled: bool = True
    updated_at: Optional[datetime] = None

    @classmethod
    def from_config(cls, config: Optional[dict]) -> 'ProjectContextDetail':
        """Convert Configuration RPC response to ProjectContextDetail.

        Args:
            config: Configuration dict from RPC with structure:
                {
                    'id': int,
                    'data': {'content': str, 'enabled': bool},
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
            updated_at=config.get('updated_at'),
        )
