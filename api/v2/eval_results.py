"""Eval **results** — read endpoint for EVAL-P1-B5 (§15.5, §20.10).

GET returns a run's per-item verdicts (native + normalized scores, verdict/evidence envelopes) plus
the latest human annotations and a server-side re-derived weighted headline. The headline is folded
from the same normalized items the response returns, through the one shared aggregation path the
runner (H5) and human re-aggregation (B6) use — so a client recomputing over the returned per-item
scores + the snapshot's binding weights lands on the identical number (EVAL-E2E-09). Read-only and
viewer-visible; no re-aggregation is persisted here (that is B6's write path).
"""

from flask import request  # pylint: disable=E0401

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalRunDetailModel,
    EvalResultDetailModel,
    EvalHumanScoreDetailModel,
    EvalRunResultsModel,
)
from ...utils.evaluation_result_utils import (
    get_run_results,
    DEFAULT_RESULT_LIMIT,
    MAX_RESULT_LIMIT,
)
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Read eval run results",
        description="Returns a run's per-item results (native + normalized scores, verdicts, evidence), the latest human annotations, and a server-side re-derived weighted headline. The headline is aggregated from the returned normalized scores via the shared path, so a client recompute matches it (EVAL-E2E-09). Drives the results screen (#7).",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "run_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "limit", "in": "query", "schema": {"type": "integer"},
             "description": f"Result rows per page (default {DEFAULT_RESULT_LIMIT}, max {MAX_RESULT_LIMIT})."},
            {"name": "offset", "in": "query", "schema": {"type": "integer"},
             "description": "Result rows to skip."},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.run.read"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, run_id: int, **kwargs):
        try:
            limit = int(request.args.get("limit", DEFAULT_RESULT_LIMIT))
            offset = int(request.args.get("offset", 0))
        except ValueError:
            return {"error": "limit and offset must be integers"}, 400
        with db.get_session(project_id) as session:
            try:
                data = get_run_results(
                    project_id, run_id, session=session, limit=limit, offset=offset,
                )
            except EvalLibraryError as exc:
                return {"error": str(exc)}, exc.http_status
            payload = EvalRunResultsModel(
                run=EvalRunDetailModel.model_validate(data['run']),
                results=[EvalResultDetailModel.model_validate(r) for r in data['results']],
                human_scores=[EvalHumanScoreDetailModel.model_validate(h) for h in data['human_scores']],
                headline_score=data['headline_score'],
                total=data['total'],
                limit=data['limit'],
                offset=data['offset'],
            )
            return payload.model_dump(mode='json'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:run_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
