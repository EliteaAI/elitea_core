"""CRUD + import + promote for Agent-Evaluation **datasets** (EVAL-P1-B3, §17.1 / §17.2).

A *dataset* is a mutable, project-scoped set of golden *cases* (``input`` + optional
``variables`` + optional ``expected_output``). Runs freeze the case set they use (§3.4), so
editing cases later never mutates past runs.

Three case sources feed one dataset (§17.2): ``manual`` (single add / edit), ``import``
(CSV/JSON bulk via :mod:`evaluation_dataset_import`) and ``conversation`` (promote real
traffic via :func:`evaluation_turn_extraction.extract_conversation_turns`, §8.3).

Errors subclass ``EvalLibraryError`` so the v2 API boundary returns ``exc.http_status``.
"""

from typing import List, Optional

from ..models.evaluation import EvalDataset, EvalDatasetCase, EvalCaseSource
from ..models.pd.evaluation import (
    EvalDatasetCreateModel,
    EvalDatasetUpdateModel,
    EvalDatasetCaseCreateModel,
    EvalDatasetCaseUpdateModel,
)
from .evaluation_library_utils import EvalLibraryError, _session
from .evaluation_dataset_import import parse_import
from .evaluation_turn_extraction import extract_conversation_turns


class EvalDatasetNotFoundError(EvalLibraryError):
    http_status = 404

    def __init__(self, dataset_id: int):
        super().__init__(f'Eval dataset with id {dataset_id} not found')
        self.dataset_id = dataset_id


class EvalDatasetCaseNotFoundError(EvalLibraryError):
    http_status = 404

    def __init__(self, case_id: int):
        super().__init__(f'Eval dataset case with id {case_id} not found')
        self.case_id = case_id


# P1 hard cap (#6349) — the single source of truth for the per-dataset case limit. Datasets that
# already exceed this (grandfathered) keep their existing cases but cannot add more.
MAX_CASES_PER_DATASET = 10


class EvalDatasetCaseLimitError(EvalLibraryError):
    http_status = 400

    def __init__(self, current: int, adding: int, limit: int = MAX_CASES_PER_DATASET):
        if adding <= 1:
            message = (
                f'This dataset already has {limit} cases, which is the current maximum. '
                'Remove a case before adding a new one.'
            )
        else:
            message = (
                f'Import contains {adding} cases; only {limit} are allowed per dataset — '
                'reduce the file or split it into multiple datasets.'
            ) if current == 0 else (
                f'This dataset has {current} case(s); adding {adding} more would exceed the '
                f'{limit}-case maximum — reduce the file or split it into multiple datasets.'
            )
        super().__init__(message)
        self.current = current
        self.adding = adding
        self.limit = limit


# ----------------------------------------------------------------------------
# Datasets
# ----------------------------------------------------------------------------

def _attach_counts(dataset: EvalDataset) -> EvalDataset:
    """Attach list-view counters (§17.3) as unmapped attributes for summary serialization."""
    cases = dataset.cases
    dataset.case_count = len(cases)
    dataset.with_expected_count = sum(
        1 for c in cases if c.expected_output is not None and c.expected_output.strip()
    )
    return dataset


def list_datasets(project_id: int, agent_id: Optional[int] = None, session=None) -> List[EvalDataset]:
    """All project datasets, or (§6350) just the ones a given agent's suite config may pick from:
    datasets it owns plus any dataset another agent opted into sharing."""
    with _session(session, project_id) as s:
        query = s.query(EvalDataset)
        if agent_id is not None:
            query = query.filter(
                (EvalDataset.agent_id == agent_id) | (EvalDataset.is_shared.is_(True))
            )
        rows = query.order_by(EvalDataset.name.asc(), EvalDataset.id.asc()).all()
        return [_attach_counts(d) for d in rows]


def get_dataset(project_id: int, dataset_id: int, session=None) -> Optional[EvalDataset]:
    with _session(session, project_id) as s:
        return s.query(EvalDataset).filter(EvalDataset.id == dataset_id).first()


DEFAULT_CASE_LIMIT = 200
MAX_CASE_LIMIT = 1000


def list_cases(
    project_id: int,
    dataset_id: int,
    session=None,
    limit: Optional[int] = None,
    offset: int = 0,
) -> dict:
    """One page of a dataset's cases, ordered by ``order_index``, plus the full ``total``.

    Paginated because a case's ``input``/``expected_output`` text is unbounded — the
    relationship-backed read returned all of them. The dataset itself is capped at
    :data:`MAX_CASES_PER_DATASET` cases (#6349), well under either limit here.
    """
    with _session(session, project_id) as s:
        _require_dataset(s, dataset_id)
        page_size = min(limit or DEFAULT_CASE_LIMIT, MAX_CASE_LIMIT)
        ordered = (
            s.query(EvalDatasetCase)
            .filter(EvalDatasetCase.dataset_id == dataset_id)
            .order_by(EvalDatasetCase.order_index.asc(), EvalDatasetCase.id.asc())
        )
        return {
            'total': ordered.count(),
            'limit': page_size,
            'offset': max(offset, 0),
            'cases': ordered.offset(max(offset, 0)).limit(page_size).all(),
        }


def create_dataset(project_id: int, data: EvalDatasetCreateModel, owner_id: int, session=None) -> EvalDataset:
    with _session(session, project_id) as s:
        dataset = EvalDataset(
            name=data.name,
            description=data.description,
            agent_id=data.agent_id,
            is_shared=data.is_shared,
            owner_id=owner_id,
            meta=data.meta,
        )
        s.add(dataset)
        s.flush()
        s.refresh(dataset)
        return dataset


def update_dataset(project_id: int, dataset_id: int, data: EvalDatasetUpdateModel, session=None) -> EvalDataset:
    with _session(session, project_id) as s:
        dataset = _require_dataset(s, dataset_id)
        for key, value in data.model_dump(exclude_unset=True).items():
            setattr(dataset, key, value)
        s.flush()
        s.refresh(dataset)
        return dataset


def delete_dataset(project_id: int, dataset_id: int, session=None) -> None:
    with _session(session, project_id) as s:
        dataset = _require_dataset(s, dataset_id)
        s.delete(dataset)  # cases cascade (delete-orphan)


# ----------------------------------------------------------------------------
# Cases
# ----------------------------------------------------------------------------

def _require_dataset(s, dataset_id: int) -> EvalDataset:
    dataset = s.query(EvalDataset).filter(EvalDataset.id == dataset_id).first()
    if not dataset:
        raise EvalDatasetNotFoundError(dataset_id)
    return dataset


def _next_order_index(s, dataset_id: int) -> int:
    """The append position: one past the current max ``order_index`` (0 for an empty set)."""
    from sqlalchemy import func as sa_func
    current_max = (
        s.query(sa_func.max(EvalDatasetCase.order_index))
        .filter(EvalDatasetCase.dataset_id == dataset_id)
        .scalar()
    )
    return 0 if current_max is None else current_max + 1


def _case_count(s, dataset_id: int) -> int:
    return s.query(EvalDatasetCase).filter(EvalDatasetCase.dataset_id == dataset_id).count()


def add_case(project_id: int, dataset_id: int, data: EvalDatasetCaseCreateModel, session=None) -> EvalDatasetCase:
    with _session(session, project_id) as s:
        _require_dataset(s, dataset_id)
        current = _case_count(s, dataset_id)
        if current >= MAX_CASES_PER_DATASET:
            raise EvalDatasetCaseLimitError(current, 1)
        case = EvalDatasetCase(
            dataset_id=dataset_id,
            order_index=_next_order_index(s, dataset_id),
            input=data.input,
            variables=data.variables,
            expected_output=data.expected_output,
            source_type=data.source_type,
            source_ref=data.source_ref,
            meta=data.meta,
        )
        s.add(case)
        s.flush()
        s.refresh(case)
        return case


def update_case(
    project_id: int, dataset_id: int, case_id: int, data: EvalDatasetCaseUpdateModel, session=None,
) -> EvalDatasetCase:
    with _session(session, project_id) as s:
        case = (
            s.query(EvalDatasetCase)
            .filter(EvalDatasetCase.dataset_id == dataset_id, EvalDatasetCase.id == case_id)
            .first()
        )
        if not case:
            raise EvalDatasetCaseNotFoundError(case_id)
        fields = data.model_dump(exclude_unset=True)
        fields.pop('source_type', None)  # source is immutable post-create
        for key, value in fields.items():
            setattr(case, key, value)
        s.flush()
        s.refresh(case)
        return case


def delete_case(project_id: int, dataset_id: int, case_id: int, session=None) -> None:
    with _session(session, project_id) as s:
        case = (
            s.query(EvalDatasetCase)
            .filter(EvalDatasetCase.dataset_id == dataset_id, EvalDatasetCase.id == case_id)
            .first()
        )
        if not case:
            raise EvalDatasetCaseNotFoundError(case_id)
        s.delete(case)


# ----------------------------------------------------------------------------
# Bulk sources — import (§17.2 CSV/JSON) and promote (§17.2 conversations)
# ----------------------------------------------------------------------------

def _append_rows(s, dataset_id: int, rows: List[dict], source_type: str) -> List[EvalDatasetCase]:
    """Append validated row dicts as cases with contiguous ``order_index`` at the end.

    All-or-nothing against :data:`MAX_CASES_PER_DATASET` (#6349): a bulk append that would push
    the dataset over the cap is rejected before any row is added, rather than accepting as many
    as fit.
    """
    current = _case_count(s, dataset_id)
    if rows and current + len(rows) > MAX_CASES_PER_DATASET:
        raise EvalDatasetCaseLimitError(current, len(rows))
    start = _next_order_index(s, dataset_id)
    created: List[EvalDatasetCase] = []
    for offset, row in enumerate(rows):
        case = EvalDatasetCase(
            dataset_id=dataset_id,
            order_index=start + offset,
            input=row['input'],
            variables=row.get('variables') or {},
            expected_output=row.get('expected_output'),
            source_type=source_type,
            source_ref=row.get('source_ref'),
            meta={},
        )
        s.add(case)
        created.append(case)
    s.flush()
    if not created:
        return created
    # `created_at` is a server_default, so flush leaves it unloaded and the API's serialization
    # would fault it in one row at a time. Load the whole appended block back in a single
    # statement instead of `refresh`-ing up to MAX_CASES rows sequentially.
    return (
        s.query(EvalDatasetCase)
        .filter(
            EvalDatasetCase.dataset_id == dataset_id,
            EvalDatasetCase.order_index >= start,
            EvalDatasetCase.order_index < start + len(created),
        )
        .order_by(EvalDatasetCase.order_index.asc())
        .populate_existing()
        .all()
    )


def import_cases(project_id: int, dataset_id: int, fmt: str, content: str, session=None) -> dict:
    """Parse ``content`` (§17.2) and append valid rows as ``import`` cases. Returns an
    ``{accepted, rejected, errors, cases}`` report; invalid rows never abort the import."""
    rows, errors = parse_import(fmt, content)
    with _session(session, project_id) as s:
        _require_dataset(s, dataset_id)
        created = _append_rows(s, dataset_id, rows, EvalCaseSource.import_)
        return {
            'accepted': len(created),
            'rejected': len(errors),
            'errors': errors,
            'cases': created,
        }


def promote_from_conversation(
    project_id: int, dataset_id: int, conversation_id: int, include_expected: bool = True, session=None,
) -> dict:
    """Promote a stored conversation into golden cases (§17.2, §8.3). Each user turn → a case
    ``input``; the agent reply → ``expected_output`` when ``include_expected``. ``source_type=
    conversation`` + ``source_ref=<conversation_id>`` link back to the origin. Returns
    ``{accepted, cases}``."""
    with _session(session, project_id) as s:
        _require_dataset(s, dataset_id)
        pairs = extract_conversation_turns(project_id, conversation_id, session=s)
        rows = [
            {
                'input': input_text,
                'expected_output': (output_text if include_expected else None),
                'source_ref': str(conversation_id),
            }
            for input_text, output_text in pairs
            if input_text and input_text.strip()
        ]
        created = _append_rows(s, dataset_id, rows, EvalCaseSource.conversation)
        return {'accepted': len(created), 'cases': created}
