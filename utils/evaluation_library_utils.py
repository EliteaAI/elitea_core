"""CRUD for the Agent-Evaluation **Library** — dimension + code-validation definitions
(EVAL-P1-B1, §16 / §3.1 / §19).

Definitions only (bindings are B2). Tables are project-scoped (``p_<project_id>``) and
already provisioned by H6. Errors follow the ``SkillError`` convention: each carries an
``http_status`` the v2 API boundary returns; unexpected errors propagate to a 500.
"""

from contextlib import contextmanager
from typing import List, Optional

from sqlalchemy.exc import IntegrityError

from tools import db

from ..models.evaluation import EvalDimension, EvalCodeValidation, EvalTier
from ..models.pd.evaluation import (
    EvalDimensionCreateModel,
    EvalDimensionUpdateModel,
    EvalCodeValidationCreateModel,
    EvalCodeValidationUpdateModel,
)
from .evaluation_code_screen import screen_validation_code


class EvalLibraryError(Exception):
    """Base for library-domain errors. ``http_status`` is what the API returns."""
    http_status = 400


class EvalDimensionNotFoundError(EvalLibraryError):
    http_status = 404

    def __init__(self, dimension_id: int):
        super().__init__(f'Eval dimension with id {dimension_id} not found')
        self.dimension_id = dimension_id


class EvalCodeValidationNotFoundError(EvalLibraryError):
    http_status = 404

    def __init__(self, code_validation_id: int):
        super().__init__(f'Code validation with id {code_validation_id} not found')
        self.code_validation_id = code_validation_id


class EvalNameConflictError(EvalLibraryError):
    http_status = 409

    def __init__(self, name: str):
        super().__init__(f'A definition named "{name}" already exists')
        self.name = name


class EvalTierImmutableError(EvalLibraryError):
    """Platform-tier definitions are read-only from a project API (managed in admin)."""
    http_status = 403

    def __init__(self, tier: str):
        super().__init__(f"'{tier}'-tier definitions cannot be modified from the project library")
        self.tier = tier


class EvalCodeScreenError(EvalLibraryError):
    """Author-time AST pre-screen (Layer 1) rejected the code body."""
    http_status = 400

    def __init__(self, violations: List[str]):
        super().__init__('Code validation failed the safety pre-screen: ' + '; '.join(violations))
        self.violations = violations


@contextmanager
def _session(session, project_id):
    """Yield a usable session; own commit/rollback/close only when we created it."""
    if session is not None:
        yield session
        session.flush()
        return
    with db.get_session(project_id) as owned:
        try:
            yield owned
            owned.commit()
        except Exception:
            owned.rollback()
            raise


# ----------------------------------------------------------------------------
# Dimension definitions
# ----------------------------------------------------------------------------

def list_dimensions(
    project_id: int,
    include_platform: bool = True,
    session=None,
) -> List[EvalDimension]:
    """List dimension definitions visible to a project: project + agent_adhoc tiers,
    plus (read-only) platform-tier seeds when ``include_platform``."""
    with _session(session, project_id) as s:
        query = s.query(EvalDimension)
        if not include_platform:
            query = query.filter(EvalDimension.tier != EvalTier.platform)
        return query.order_by(EvalDimension.name.asc(), EvalDimension.id.asc()).all()


def get_dimension(project_id: int, dimension_id: int, session=None) -> Optional[EvalDimension]:
    with _session(session, project_id) as s:
        return s.query(EvalDimension).filter(EvalDimension.id == dimension_id).first()


def create_dimension(
    project_id: int,
    data: EvalDimensionCreateModel,
    owner_id: int,
    session=None,
) -> EvalDimension:
    with _session(session, project_id) as s:
        dimension = EvalDimension(
            tier=data.tier,
            name=data.name,
            description=data.description,
            allowed_engines=data.allowed_engines,
            scale_type=data.scale_type,
            scale_min=data.scale_min,
            scale_max=data.scale_max,
            polarity=data.polarity,
            default_weight=data.default_weight,
            default_target=data.default_target,
            default_target_operator=data.default_target_operator,
            owner_id=owner_id,
            meta=data.meta,
        )
        s.add(dimension)
        try:
            s.flush()
        except IntegrityError:
            s.rollback()
            raise EvalNameConflictError(data.name)
        s.refresh(dimension)
        return dimension


def update_dimension(
    project_id: int,
    dimension_id: int,
    data: EvalDimensionUpdateModel,
    session=None,
) -> EvalDimension:
    with _session(session, project_id) as s:
        dimension = s.query(EvalDimension).filter(EvalDimension.id == dimension_id).first()
        if not dimension:
            raise EvalDimensionNotFoundError(dimension_id)
        if dimension.tier == EvalTier.platform:
            raise EvalTierImmutableError(dimension.tier)

        fields = data.model_dump(exclude_unset=True)
        fields.pop('tier', None)  # tier is immutable post-create
        for key, value in fields.items():
            setattr(dimension, key, value)
        try:
            s.flush()
        except IntegrityError:
            s.rollback()
            raise EvalNameConflictError(fields.get('name') or dimension.name)
        s.refresh(dimension)
        return dimension


def delete_dimension(project_id: int, dimension_id: int, session=None) -> None:
    with _session(session, project_id) as s:
        dimension = s.query(EvalDimension).filter(EvalDimension.id == dimension_id).first()
        if not dimension:
            raise EvalDimensionNotFoundError(dimension_id)
        if dimension.tier == EvalTier.platform:
            raise EvalTierImmutableError(dimension.tier)
        s.delete(dimension)


# ----------------------------------------------------------------------------
# Code-validation definitions (project-tier only — §19.6)
# ----------------------------------------------------------------------------

def list_code_validations(project_id: int, session=None) -> List[EvalCodeValidation]:
    with _session(session, project_id) as s:
        return (
            s.query(EvalCodeValidation)
            .order_by(EvalCodeValidation.name.asc(), EvalCodeValidation.id.asc())
            .all()
        )


def get_code_validation(project_id: int, code_validation_id: int, session=None) -> Optional[EvalCodeValidation]:
    with _session(session, project_id) as s:
        return (
            s.query(EvalCodeValidation)
            .filter(EvalCodeValidation.id == code_validation_id)
            .first()
        )


def create_code_validation(
    project_id: int,
    data: EvalCodeValidationCreateModel,
    owner_id: int,
    session=None,
) -> EvalCodeValidation:
    violations = screen_validation_code(data.code)
    if violations:
        raise EvalCodeScreenError(violations)

    with _session(session, project_id) as s:
        code_validation = EvalCodeValidation(
            name=data.name,
            description=data.description,
            code=data.code,
            return_contract=data.return_contract,
            scale_min=data.scale_min,
            scale_max=data.scale_max,
            polarity=data.polarity,
            owner_id=owner_id,
            meta=data.meta,
        )
        s.add(code_validation)
        try:
            s.flush()
        except IntegrityError:
            s.rollback()
            raise EvalNameConflictError(data.name)
        s.refresh(code_validation)
        return code_validation


def update_code_validation(
    project_id: int,
    code_validation_id: int,
    data: EvalCodeValidationUpdateModel,
    session=None,
) -> EvalCodeValidation:
    fields = data.model_dump(exclude_unset=True)
    if 'code' in fields:
        violations = screen_validation_code(fields['code'])
        if violations:
            raise EvalCodeScreenError(violations)

    with _session(session, project_id) as s:
        code_validation = (
            s.query(EvalCodeValidation)
            .filter(EvalCodeValidation.id == code_validation_id)
            .first()
        )
        if not code_validation:
            raise EvalCodeValidationNotFoundError(code_validation_id)
        for key, value in fields.items():
            setattr(code_validation, key, value)
        try:
            s.flush()
        except IntegrityError:
            s.rollback()
            raise EvalNameConflictError(fields.get('name') or code_validation.name)
        s.refresh(code_validation)
        return code_validation


def delete_code_validation(project_id: int, code_validation_id: int, session=None) -> None:
    with _session(session, project_id) as s:
        code_validation = (
            s.query(EvalCodeValidation)
            .filter(EvalCodeValidation.id == code_validation_id)
            .first()
        )
        if not code_validation:
            raise EvalCodeValidationNotFoundError(code_validation_id)
        s.delete(code_validation)
