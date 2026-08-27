"""CRUD for the Agent-Evaluation **Library** — dimension definitions, any engine
(EVAL-P1-B1, §16 / §3.1 / §19 / §2.1).

Definitions only (bindings are B2). Tables are project-scoped (``p_<project_id>``) and
already provisioned by H6. Errors follow the ``SkillError`` convention: each carries an
``http_status`` the v2 API boundary returns; unexpected errors propagate to a 500.
"""

from contextlib import contextmanager
from typing import List, Optional

from sqlalchemy.exc import IntegrityError

from tools import db

from ..models.evaluation import EvalDimension, EvalTier
from ..models.pd.evaluation import EvalDimensionCreateModel, EvalDimensionUpdateModel
from .evaluation_code_screen import screen_validation_code


class EvalLibraryError(Exception):
    """Base for library-domain errors. ``http_status`` is what the API returns."""
    http_status = 400


class EvalDimensionNotFoundError(EvalLibraryError):
    http_status = 404

    def __init__(self, dimension_id: int):
        super().__init__(f'Eval dimension with id {dimension_id} not found')
        self.dimension_id = dimension_id


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
    agent_id: Optional[int] = None,
    session=None,
) -> List[EvalDimension]:
    """List dimension definitions visible to a project: project + agent_adhoc tiers,
    plus (read-only) platform-tier seeds when ``include_platform``.

    When ``agent_id`` is given, ``agent_adhoc`` rows are additionally scoped to that
    agent: only dimensions owned by it, or legacy rows predating the ownership column
    (``agent_id IS NULL``, treated as visible everywhere), are included."""
    with _session(session, project_id) as s:
        query = s.query(EvalDimension)
        if not include_platform:
            query = query.filter(EvalDimension.tier != EvalTier.platform)
        if agent_id is not None:
            query = query.filter(
                (EvalDimension.tier != EvalTier.agent_adhoc)
                | (EvalDimension.agent_id == agent_id)
                | (EvalDimension.agent_id.is_(None))
            )
        return query.order_by(EvalDimension.name.asc(), EvalDimension.id.asc()).all()


def _visible_to_agent(dimension: EvalDimension, agent_id: Optional[int]) -> bool:
    """Mirror ``list_dimensions``' agent scoping for a single already-fetched row: an
    ``agent_adhoc`` dimension owned by a different agent is treated as not found, same as
    it's simply absent from that agent's list."""
    if agent_id is None:
        return True
    if dimension.tier != EvalTier.agent_adhoc:
        return True
    return dimension.agent_id is None or dimension.agent_id == agent_id


def get_dimension(
    project_id: int, dimension_id: int, agent_id: Optional[int] = None, session=None,
) -> Optional[EvalDimension]:
    with _session(session, project_id) as s:
        dimension = s.query(EvalDimension).filter(EvalDimension.id == dimension_id).first()
        if dimension is not None and not _visible_to_agent(dimension, agent_id):
            return None
        return dimension


def create_dimension(
    project_id: int,
    data: EvalDimensionCreateModel,
    owner_id: int,
    session=None,
) -> EvalDimension:
    if data.code:
        violations = screen_validation_code(data.code)
        if violations:
            raise EvalCodeScreenError(violations)

    with _session(session, project_id) as s:
        dimension = EvalDimension(
            tier=data.tier,
            name=data.name,
            description=data.description,
            agent_id=data.agent_id,
            allowed_engines=data.allowed_engines,
            scale_type=data.scale_type,
            scale_min=data.scale_min,
            scale_max=data.scale_max,
            polarity=data.polarity,
            default_weight=data.default_weight,
            default_target=data.default_target,
            default_target_operator=data.default_target_operator,
            code=data.code,
            return_contract=data.return_contract,
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
    agent_id: Optional[int] = None,
    session=None,
) -> EvalDimension:
    with _session(session, project_id) as s:
        dimension = s.query(EvalDimension).filter(EvalDimension.id == dimension_id).first()
        if not dimension or not _visible_to_agent(dimension, agent_id):
            raise EvalDimensionNotFoundError(dimension_id)
        if dimension.tier == EvalTier.platform:
            raise EvalTierImmutableError(dimension.tier)

        fields = data.model_dump(exclude_unset=True)
        fields.pop('tier', None)  # tier is immutable post-create
        fields.pop('agent_id', None)  # ownership is coupled to tier; immutable post-create
        if fields.get('code'):
            violations = screen_validation_code(fields['code'])
            if violations:
                raise EvalCodeScreenError(violations)
        for key, value in fields.items():
            setattr(dimension, key, value)
        try:
            s.flush()
        except IntegrityError:
            s.rollback()
            raise EvalNameConflictError(fields.get('name') or dimension.name)
        s.refresh(dimension)
        return dimension


def delete_dimension(
    project_id: int, dimension_id: int, agent_id: Optional[int] = None, session=None,
) -> None:
    with _session(session, project_id) as s:
        dimension = s.query(EvalDimension).filter(EvalDimension.id == dimension_id).first()
        if not dimension or not _visible_to_agent(dimension, agent_id):
            raise EvalDimensionNotFoundError(dimension_id)
        if dimension.tier == EvalTier.platform:
            raise EvalTierImmutableError(dimension.tier)
        s.delete(dimension)
