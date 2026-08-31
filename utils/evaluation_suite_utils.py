"""CRUD for Agent-Evaluation **suites + bindings** (EVAL-P1-B2, §13 / §16.2 / §3).

A *suite* is a named set of *bindings* on an agent (``Application``); each binding applies
one library item (dimension — any engine — or platform key) with per-agent weight/target/
engine/evidence, pinned to a concrete ``ApplicationVersion`` (H6 versioning seam, §16.3).

Definitions live in the library (B1); this module owns the *binding* side. Errors subclass
``EvalLibraryError`` so the v2 API boundary returns ``exc.http_status`` uniformly.
"""

from typing import List, Optional

from sqlalchemy.exc import IntegrityError

from tools import db

from ..models.evaluation import (
    EvalSuite,
    EvalBinding,
    EvalDimension,
    EvalEngine,
    EvalTier,
)
from ..models.all import ApplicationVersion
from ..models.pd.evaluation import (
    EvalSuiteCreateModel,
    EvalSuiteUpdateModel,
    EvalBindingCreateModel,
    EvalBindingUpdateModel,
)
from .evaluation_library_utils import EvalLibraryError, EvalNameConflictError, _session


class EvalSuiteNotFoundError(EvalLibraryError):
    http_status = 404

    def __init__(self, suite_id: int):
        super().__init__(f'Eval suite with id {suite_id} not found')
        self.suite_id = suite_id


class EvalBindingNotFoundError(EvalLibraryError):
    http_status = 404

    def __init__(self, binding_id: int):
        super().__init__(f'Eval binding with id {binding_id} not found')
        self.binding_id = binding_id


class EvalBindingSourceError(EvalLibraryError):
    """A referenced dimension / code-validation / version does not exist or does not belong here."""
    http_status = 400


class EvalBindingDuplicateError(EvalLibraryError):
    """The item is already bound to this suite. Scoring the same criterion twice would silently
    double its weight in the headline, so the second attach is refused."""
    http_status = 409


class EvalBindingEngineError(EvalLibraryError):
    """The binding asks for an engine the bound dimension's definition does not permit."""
    http_status = 400

    def __init__(self, engine: str, allowed: List[str]):
        super().__init__(
            f"engine '{engine}' is not allowed for this dimension; allowed: {sorted(allowed)}"
        )
        self.engine = engine
        self.allowed = allowed


# ----------------------------------------------------------------------------
# Suites
# ----------------------------------------------------------------------------

def list_suites(project_id: int, application_id: Optional[int] = None, session=None) -> List[EvalSuite]:
    with _session(session, project_id) as s:
        query = s.query(EvalSuite)
        if application_id is not None:
            query = query.filter(EvalSuite.application_id == application_id)
        return query.order_by(EvalSuite.application_id.asc(), EvalSuite.name.asc(), EvalSuite.id.asc()).all()


def get_suite(project_id: int, suite_id: int, session=None) -> Optional[EvalSuite]:
    with _session(session, project_id) as s:
        return s.query(EvalSuite).filter(EvalSuite.id == suite_id).first()


def create_suite(project_id: int, data: EvalSuiteCreateModel, owner_id: int, session=None) -> EvalSuite:
    with _session(session, project_id) as s:
        suite = EvalSuite(
            application_id=data.application_id,
            name=data.name,
            description=data.description,
            dataset_id=data.dataset_id,
            judge_model=data.judge_model,
            baseline_run_id=data.baseline_run_id,
            trigger_config=data.trigger_config,
            owner_id=owner_id,
            meta=data.meta,
        )
        s.add(suite)
        try:
            s.flush()
        except IntegrityError:
            s.rollback()
            raise EvalNameConflictError(data.name)
        s.refresh(suite)
        return suite


def bootstrap_default_suite(project_id: int, application_id: int, owner_id: int, session=None) -> EvalSuite:
    """Return the app's 'Default suite', creating it if absent (§13 default-suite bootstrap)."""
    with _session(session, project_id) as s:
        existing = (
            s.query(EvalSuite)
            .filter(EvalSuite.application_id == application_id, EvalSuite.name == 'Default suite')
            .first()
        )
        if existing:
            return existing
        suite = EvalSuite(application_id=application_id, name='Default suite', owner_id=owner_id)
        s.add(suite)
        s.flush()
        s.refresh(suite)
        return suite


def update_suite(project_id: int, suite_id: int, data: EvalSuiteUpdateModel, session=None) -> EvalSuite:
    with _session(session, project_id) as s:
        suite = s.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
        if not suite:
            raise EvalSuiteNotFoundError(suite_id)

        fields = data.model_dump(exclude_unset=True)
        fields.pop('application_id', None)  # a suite cannot be moved to another agent
        for key, value in fields.items():
            setattr(suite, key, value)
        try:
            s.flush()
        except IntegrityError:
            s.rollback()
            raise EvalNameConflictError(fields.get('name') or suite.name)
        s.refresh(suite)
        return suite


def delete_suite(project_id: int, suite_id: int, session=None) -> None:
    with _session(session, project_id) as s:
        suite = s.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
        if not suite:
            raise EvalSuiteNotFoundError(suite_id)
        s.delete(suite)  # bindings cascade (delete-orphan)


# ----------------------------------------------------------------------------
# Bindings
# ----------------------------------------------------------------------------

def _require_suite(s, suite_id: int) -> EvalSuite:
    suite = s.query(EvalSuite).filter(EvalSuite.id == suite_id).first()
    if not suite:
        raise EvalSuiteNotFoundError(suite_id)
    return suite


def _validate_source(s, suite: EvalSuite, data: EvalBindingCreateModel) -> None:
    if data.dimension_id is not None:
        dimension = s.query(EvalDimension).filter(EvalDimension.id == data.dimension_id).first()
        if not dimension:
            raise EvalBindingSourceError(f'dimension {data.dimension_id} not found')
        # agent_adhoc dimensions are only visible/usable from their owning agent's suites; a NULL
        # owner is a legacy row that predates agent-scoping and stays usable everywhere (matches
        # the same convention used by list_dimensions).
        if (
            dimension.tier == EvalTier.agent_adhoc
            and dimension.agent_id is not None
            and dimension.agent_id != suite.application_id
        ):
            raise EvalBindingSourceError(f'dimension {data.dimension_id} not found')
    # platform_key references an external catalog (not a project row) — no DB check here.


def _validate_version_pin(s, suite: EvalSuite, application_version_id: Optional[int]) -> None:
    """A binding may pin only a version that belongs to the suite's application (§16.3)."""
    if application_version_id is None:
        return
    version = (
        s.query(ApplicationVersion)
        .filter(ApplicationVersion.id == application_version_id)
        .first()
    )
    if not version:
        raise EvalBindingSourceError(f'application version {application_version_id} not found')
    if version.application_id != suite.application_id:
        raise EvalBindingSourceError(
            f'application version {application_version_id} does not belong to this suite\'s agent'
        )


def _validate_dimension_engine(s, dimension_id: Optional[int], engine: str) -> None:
    """A dimension binding may only run on an engine its definition permits (§16.2).

    A definition with no ``allowed_engines`` recorded is left alone: those predate the field and
    would otherwise start failing on edit.
    """
    if dimension_id is None:
        return
    dimension = s.query(EvalDimension).filter(EvalDimension.id == dimension_id).first()
    allowed = (dimension.allowed_engines or []) if dimension is not None else []
    if allowed and engine not in allowed:
        raise EvalBindingEngineError(engine, allowed)


def _require_not_already_bound(s, suite_id: int, data: EvalBindingCreateModel) -> None:
    """Pre-check the ``(suite_id, <source>)`` unique constraints so a re-attach returns a 409
    instead of surfacing an IntegrityError from the flush."""
    column, value = (
        (EvalBinding.dimension_id, data.dimension_id) if data.dimension_id is not None
        else (EvalBinding.platform_key, data.platform_key)
    )
    existing = (
        s.query(EvalBinding.id)
        .filter(EvalBinding.suite_id == suite_id, column == value)
        .first()
    )
    if existing:
        raise EvalBindingDuplicateError('this validation is already bound to the suite')


def list_bindings(project_id: int, suite_id: int, session=None) -> List[EvalBinding]:
    with _session(session, project_id) as s:
        _require_suite(s, suite_id)
        return (
            s.query(EvalBinding)
            .filter(EvalBinding.suite_id == suite_id)
            .order_by(EvalBinding.order_index.asc(), EvalBinding.id.asc())
            .all()
        )


def get_binding(project_id: int, suite_id: int, binding_id: int, session=None) -> Optional[EvalBinding]:
    with _session(session, project_id) as s:
        return (
            s.query(EvalBinding)
            .filter(EvalBinding.suite_id == suite_id, EvalBinding.id == binding_id)
            .first()
        )


def add_binding(project_id: int, suite_id: int, data: EvalBindingCreateModel, session=None) -> EvalBinding:
    with _session(session, project_id) as s:
        suite = _require_suite(s, suite_id)
        _validate_source(s, suite, data)
        _validate_version_pin(s, suite, data.application_version_id)
        # Platform bindings always run on the code engine (§12); dimension bindings (any
        # engine, including a code-engine dimension) honor the editable engine column. Normalize
        # at the source so a defaulted 'ai' engine can't be persisted for a platform item.
        engine = data.engine
        if data.platform_key is not None:
            engine = EvalEngine.code
        _validate_dimension_engine(s, data.dimension_id, engine)
        _require_not_already_bound(s, suite_id, data)
        binding = EvalBinding(
            suite_id=suite_id,
            application_version_id=data.application_version_id,
            dimension_id=data.dimension_id,
            platform_key=data.platform_key,
            engine=engine,
            evidence_scope=data.evidence_scope,
            weight=data.weight,
            target=data.target,
            target_operator=data.target_operator,
            order_index=data.order_index,
            meta=data.meta,
        )
        s.add(binding)
        s.flush()
        s.refresh(binding)
        return binding


def update_binding(
    project_id: int, suite_id: int, binding_id: int, data: EvalBindingUpdateModel, session=None,
) -> EvalBinding:
    with _session(session, project_id) as s:
        suite = _require_suite(s, suite_id)
        binding = (
            s.query(EvalBinding)
            .filter(EvalBinding.suite_id == suite_id, EvalBinding.id == binding_id)
            .first()
        )
        if not binding:
            raise EvalBindingNotFoundError(binding_id)

        fields = data.model_dump(exclude_unset=True)
        if 'application_version_id' in fields:
            _validate_version_pin(s, suite, fields['application_version_id'])
        if 'engine' in fields:
            _validate_dimension_engine(s, binding.dimension_id, fields['engine'])
        for key, value in fields.items():
            setattr(binding, key, value)
        s.flush()
        s.refresh(binding)
        return binding


def delete_binding(project_id: int, suite_id: int, binding_id: int, session=None) -> None:
    with _session(session, project_id) as s:
        _require_suite(s, suite_id)
        binding = (
            s.query(EvalBinding)
            .filter(EvalBinding.suite_id == suite_id, EvalBinding.id == binding_id)
            .first()
        )
        if not binding:
            raise EvalBindingNotFoundError(binding_id)
        s.delete(binding)


def reorder_bindings(project_id: int, suite_id: int, binding_ids: List[int], session=None) -> List[EvalBinding]:
    """Set each binding's ``order_index`` to its position in ``binding_ids``. The list must
    be exactly the suite's current binding ids (no partial reorder)."""
    with _session(session, project_id) as s:
        _require_suite(s, suite_id)
        rows = s.query(EvalBinding).filter(EvalBinding.suite_id == suite_id).all()
        current = {r.id for r in rows}
        requested = set(binding_ids)
        if len(binding_ids) != len(requested):
            raise EvalBindingSourceError('binding_ids contains duplicates')
        if requested != current:
            raise EvalBindingSourceError('binding_ids must list exactly the suite\'s bindings')

        by_id = {r.id: r for r in rows}
        for index, bid in enumerate(binding_ids):
            by_id[bid].order_index = index
        s.flush()
        return (
            s.query(EvalBinding)
            .filter(EvalBinding.suite_id == suite_id)
            .order_by(EvalBinding.order_index.asc(), EvalBinding.id.asc())
            .all()
        )
