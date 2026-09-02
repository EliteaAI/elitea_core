"""Run + version reads for "Enhance with AI" (ENH-4, §3.2).

Thin DB layer only: it turns a finished run and its pinned agent version into the plain dicts
``enhancement_gap_selection`` and ``enhancement_prompt`` consume. All the judgement lives in those
pure modules; keeping this file free of it is what makes the ranking and the brief unit-testable.
"""

from typing import Optional

from .evaluation_library_utils import _session
from .evaluation_human_score_utils import EvalRunNotFoundError
from .mcp_versioning import instructions_sha256

# The gap payload is capped downstream, but the fold happens over every row, so an unbounded read
# is the one place a large run could hurt. A run has cases × bindings rows; this ceiling is well
# above any realistic P1 suite and exists so a pathological run fails visibly instead of stalling.
MAX_RESULT_ROWS = 20000


class EvalRunNotFinishedError(Exception):
    """The run has not finished, so its results are incomplete.

    Analysing a partial run produces a diagnosis about cases that had not been scored yet — the
    conclusions read as authoritative but describe a subset that changes minute to minute.
    """


def _result_dict(row) -> dict:
    return {
        'dataset_case_id': row.dataset_case_id,
        'dimension_id': row.dimension_id,
        'platform_key': row.platform_key,
        'engine': row.engine,
        'status': row.status,
        'native_score': row.native_score,
        'verdict': row.verdict or {},
    }


def _human_score_dict(row) -> dict:
    return {
        'dataset_case_id': row.dataset_case_id,
        'dimension_id': row.dimension_id,
        'native_score': row.native_score,
        'note': row.note,
        'is_latest': row.is_latest,
    }


def fetch_run_for_enhancement(project_id: int, run_id: int, session=None) -> dict:
    """Everything the analysis needs from one run, as plain dicts.

    Returns ``{run_id, application_id, version_id, status, headline_score, snapshot, results,
    human_scores}``.

    Raises :class:`EvalRunNotFoundError` when the run is absent and
    :class:`EvalRunNotFinishedError` when it is still in flight.
    """
    from ..models.evaluation import EvalRun, EvalResult, EvalHumanScore, EvalRunStatus

    with _session(session, project_id) as s:
        run = s.query(EvalRun).filter(EvalRun.id == run_id).first()
        if not run:
            raise EvalRunNotFoundError(run_id)
        if run.status != EvalRunStatus.finished:
            raise EvalRunNotFinishedError(
                f'Run {run_id} is {run.status}; only a finished run can be analysed'
            )

        results = (
            s.query(EvalResult)
            .filter(EvalResult.run_id == run_id)
            .order_by(EvalResult.dataset_case_id.asc(), EvalResult.id.asc())
            .limit(MAX_RESULT_ROWS)
            .all()
        )
        # Every row, not just is_latest: index_latest_human_scores() picks the current annotation,
        # and it must be the one place that rule is applied so the scorecard and the analysis agree.
        human_scores = (
            s.query(EvalHumanScore)
            .filter(EvalHumanScore.run_id == run_id)
            .order_by(EvalHumanScore.created_at.asc(), EvalHumanScore.id.asc())
            .all()
        )

        return {
            'run_id': run.id,
            'application_id': run.application_id,
            'version_id': run.application_version_id,
            'status': run.status,
            'headline_score': run.headline_score,
            'snapshot': run.snapshot or {},
            'results': [_result_dict(row) for row in results],
            'human_scores': [_human_score_dict(row) for row in human_scores],
        }


def fetch_evaluated_version(
    project_id: int,
    application_id: int,
    version_id: int,
    session=None,
) -> Optional[dict]:
    """The instructions that were actually under test, plus read-only agent context.

    Deliberately reads the run's pinned ``application_version_id`` rather than the agent's current
    default. Diagnosing text that was not the text under test is the easiest way to produce a
    confidently wrong proposal (§7.1), and ``instructions_sha256`` is what lets the apply path
    refuse a patch built against instructions that have since been edited.
    """
    from ..models.all import Application, ApplicationVersion

    with _session(session, project_id) as s:
        version = (
            s.query(ApplicationVersion)
            .filter(
                ApplicationVersion.id == version_id,
                ApplicationVersion.application_id == application_id,
            )
            .first()
        )
        if not version:
            return None
        application = s.query(Application).filter(Application.id == application_id).first()

        instructions = version.instructions or ''
        return {
            'application_id': application_id,
            'application_name': (application.name if application else None) or f'Agent #{application_id}',
            'version_id': version.id,
            'version_name': version.name,
            'version_status': version.status,
            'instructions': instructions,
            'instructions_sha256': instructions_sha256(instructions),
            'agent_context': {
                'model_name': (version.llm_settings or {}).get('model_name'),
                'toolkit_names': [tool.name for tool in (version.tools or []) if tool.name],
            },
        }
