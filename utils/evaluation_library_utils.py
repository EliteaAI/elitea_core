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


class EvalDimensionEngineFieldsError(EvalLibraryError):
    """The engine/code pairing invariant (allowed_engines == ['code'] iff code is set) would be
    violated by the *merged* row (existing columns + the fields this request actually sends) —
    not just by the request fragment in isolation. Update requests may omit ``allowed_engines``
    entirely (``exclude_unset``), so a bare ``{"code": "..."}`` PUT on an AI/Human dimension must
    still be caught here even though :class:`EvalDimensionUpdateModel` only validates the pairing
    when the caller explicitly sends ``allowed_engines``."""
    http_status = 400


class EvalDimensionEngineBindingConflictError(EvalLibraryError):
    """Changing ``allowed_engines`` would strand an existing binding whose stored ``engine`` (fixed
    at bind time) would no longer be one of the dimension's allowed engines — the binding would
    still be dispatched by its own stored engine, silently disagreeing with the definition."""
    http_status = 409

    def __init__(self, binding_ids: List[int], engine: str):
        super().__init__(
            f"cannot change allowed_engines: binding(s) {binding_ids} are bound with engine "
            f"'{engine}', which would no longer be allowed; unbind or rebind them first"
        )
        self.binding_ids = binding_ids
        self.engine = engine


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
    (``agent_id IS NULL``, treated as visible everywhere), are included.

    NOTE — this is UX scoping, not an access-control boundary. ``agent_id`` is caller
    supplied (a query param at the API layer) and is not validated against the caller's
    session; a caller can pass any agent id in the project. That is intentionally fine:
    the actual security boundary is the project-level permission check already enforced
    by ``@auth.decorators.check_api`` on every dimension endpoint (viewer/editor of the
    *project*), and every project-tier dimension is visible to every project member
    regardless of ``agent_id`` anyway. ``agent_id`` only decides which *agent_adhoc*
    (per-agent draft) dimensions are hidden from a caller building/testing a different
    agent in the same project — a decluttering nicety, not a privacy guarantee. Do not
    use this parameter to gate access to data a caller shouldn't see at all; that must be
    enforced via the project permission system instead."""
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

        # Validate the *merged* row (existing columns + only the fields this request actually
        # sends), not just the request fragment: EvalDimensionUpdateModel skips the pairing check
        # when allowed_engines is omitted, so e.g. a bare {"code": "..."} PUT on an AI dimension
        # must still be rejected here rather than silently persisting an invalid combination.
        final_engines = fields.get('allowed_engines', dimension.allowed_engines)
        final_code = fields['code'] if 'code' in fields else dimension.code
        final_contract = fields['return_contract'] if 'return_contract' in fields else dimension.return_contract
        is_code = final_engines == ['code']
        if is_code and not final_code:
            raise EvalDimensionEngineFieldsError("code is required when allowed_engines is ['code']")
        if not is_code and (final_code is not None or final_contract is not None):
            raise EvalDimensionEngineFieldsError(
                'code / return_contract are only valid for a code-engine dimension')
        if is_code and final_contract is None:
            fields['return_contract'] = 'bool'

        if 'allowed_engines' in fields and final_engines != dimension.allowed_engines:
            from ..models.evaluation import EvalBinding

            stale = (
                s.query(EvalBinding.id, EvalBinding.engine)
                .filter(EvalBinding.dimension_id == dimension.id,
                        ~EvalBinding.engine.in_(final_engines))
                .all()
            )
            if stale:
                stale_ids = [row[0] for row in stale]
                stale_engine = stale[0][1]
                raise EvalDimensionEngineBindingConflictError(stale_ids, stale_engine)

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
