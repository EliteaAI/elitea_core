"""Pydantic models for the platform-dimension registry (§16.1).

Separate from ``pd/evaluation.py``'s dimension models on purpose: the project-facing
``EvalDimensionCreateModel`` rejects ``tier='platform'`` and must keep doing so. These models
are the admin-console boundary and carry no ``tier`` (always platform).

``allowed_engines`` is limited to ai/human: a platform rubric is reusable text, whereas a
code-scored dimension needs a project-local script that cannot be shared through the registry.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict, field_validator, model_validator

from .evaluation import _OPERATORS, _POLARITIES, _SCALE_TYPES
from ..evaluation import EvalEngine, EvalPolarity, EvalScaleType

_PLATFORM_ENGINES = {EvalEngine.ai, EvalEngine.human}


class EvalPlatformDimensionBaseModel(BaseModel):
    description: Optional[str] = None
    allowed_engines: List[str] = Field(default_factory=lambda: [EvalEngine.ai])
    scale_type: str = EvalScaleType.continuous
    scale_min: float = 0.0
    scale_max: float = 100.0
    polarity: str = EvalPolarity.higher_better
    default_weight: float = 1.0
    default_target: Optional[float] = None
    default_target_operator: Optional[str] = None
    meta: dict = Field(default_factory=dict)

    @field_validator('allowed_engines')
    @classmethod
    def _validate_engines(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError('allowed_engines must not be empty')
        bad = [engine for engine in v if engine not in _PLATFORM_ENGINES]
        if bad:
            raise ValueError(
                f'unknown or unsupported engine(s): {bad}; '
                f'platform dimensions allow {sorted(_PLATFORM_ENGINES)}'
            )
        return v

    @field_validator('scale_type')
    @classmethod
    def _validate_scale_type(cls, v: str) -> str:
        if v not in _SCALE_TYPES:
            raise ValueError(f'scale_type must be one of {sorted(_SCALE_TYPES)}')
        return v

    @field_validator('polarity')
    @classmethod
    def _validate_polarity(cls, v: str) -> str:
        if v not in _POLARITIES:
            raise ValueError(f'polarity must be one of {sorted(_POLARITIES)}')
        return v

    @field_validator('default_target_operator')
    @classmethod
    def _validate_operator(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in _OPERATORS:
            raise ValueError(f'default_target_operator must be one of {sorted(_OPERATORS)}')
        return v

    @model_validator(mode='after')
    def _validate_scale_bounds(self):
        if self.scale_min >= self.scale_max:
            raise ValueError('scale_min must be strictly less than scale_max')
        return self


class EvalPlatformDimensionCreateModel(EvalPlatformDimensionBaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    is_active: bool = True


class EvalPlatformDimensionUpdateModel(EvalPlatformDimensionBaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=128)
    is_active: Optional[bool] = None


class EvalPlatformAttachModel(BaseModel):
    """Body of the project-side attach (``POST`` on the catalog): which registry entry to copy."""

    uuid: str = Field(..., min_length=1)

    @field_validator('uuid', mode='before')
    @classmethod
    def _coerce_uuid(cls, v):
        return str(v) if v is not None else v


class EvalPlatformDimensionDetailModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    uuid: str
    name: str
    description: Optional[str] = None
    allowed_engines: List[str] = Field(default_factory=list)
    scale_type: str
    scale_min: float
    scale_max: float
    polarity: str
    default_weight: float
    default_target: Optional[float] = None
    default_target_operator: Optional[str] = None
    is_active: bool
    owner_id: Optional[int] = None
    meta: dict = Field(default_factory=dict)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @field_validator('uuid', mode='before')
    @classmethod
    def _coerce_uuid(cls, v):
        return str(v) if v is not None else v
