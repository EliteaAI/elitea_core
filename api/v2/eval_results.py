"""Eval **results** — read endpoint for EVAL-P1-B5 (§15.5, §20.10).

GET returns a run's per-item verdicts (native + normalized scores, verdict/evidence envelopes) plus
the latest human annotations and a server-side re-derived weighted headline. The headline is folded
from the same normalized items the response returns, through the one shared aggregation path the
runner (H5) and human re-aggregation (B6) use — so a client recomputing over the returned per-item
scores + the snapshot's binding weights lands on the identical number (EVAL-E2E-09). Read-only and
viewer-visible; no re-aggregation is persisted here (that is B6's write path).
"""

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalRunDetailModel,
    EvalResultDetailModel,
    EvalHumanScoreDetailModel,
    EvalRunResultsModel,
)
from ...utils.evaluation_result_utils import get_run_results
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Read eval run results",
        description="Returns a run's per-item results (native + normalized scores, verdicts, evidence), the latest human annotations, and a server-side re-derived weighted headline. The headline is aggregated from the returned normalized scores via the shared path, so a client recompute matches it (EVAL-E2E-09). Drives the results screen (#7).",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "run_id", "in": "path", "schema": {"type": "integer"}},
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
        with db.get_session(project_id) as session:
            try:
                data = get_run_results(project_id, run_id, session=session)
            except EvalLibraryError as exc:
                return {"error": str(exc)}, exc.http_status
            payload = EvalRunResultsModel(
                run=EvalRunDetailModel.model_validate(data['run']),
                results=[EvalResultDetailModel.model_validate(r) for r in data['results']],
                human_scores=[EvalHumanScoreDetailModel.model_validate(h) for h in data['human_scores']],
                headline_score=data['headline_score'],
            )
            return payload.model_dump(mode='json'), 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:run_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
