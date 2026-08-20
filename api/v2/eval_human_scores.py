"""Eval **human scores** — collection endpoint (list + append) for EVAL-P1-B6.

Human scores are append-only annotations on a run's case x dimension (§15.5, D2): POST appends
a new row (never overwrites) and re-aggregates the run headline; GET returns the audit trail,
optionally filtered to one case/dimension or to the latest annotation per key. Read is
viewer-visible; writing is editor-gated per EVAL-H3 (the EDITOR persona annotates).
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalHumanScoreCreateModel,
    EvalHumanScoreDetailModel,
)
from ...utils.evaluation_human_score_utils import (
    list_human_scores,
    write_human_score,
)
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List human scores on a run",
        description="Returns the run's human-score annotations (newest first). Filter with dataset_case_id / dimension_id, or latest=true for the current annotation per case x dimension.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "run_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dataset_case_id", "in": "query", "schema": {"type": "integer"}, "description": "Filter to one case"},
            {"name": "dimension_id", "in": "query", "schema": {"type": "integer"}, "description": "Filter to one dimension"},
            {"name": "latest", "in": "query", "schema": {"type": "boolean", "default": False}, "description": "Only the latest annotation per case x dimension"},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.human_score.read"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, run_id: int, **kwargs):
        dataset_case_id = request.args.get("dataset_case_id", type=int)
        dimension_id = request.args.get("dimension_id", type=int)
        latest_only = request.args.get("latest", default="false").lower() == "true"
        with db.get_session(project_id) as session:
            try:
                rows = list_human_scores(
                    project_id, run_id,
                    dataset_case_id=dataset_case_id,
                    dimension_id=dimension_id,
                    latest_only=latest_only,
                    session=session,
                )
            except EvalLibraryError as exc:
                return {"error": str(exc)}, exc.http_status
            return [
                EvalHumanScoreDetailModel.model_validate(r).model_dump(mode='json')
                for r in rows
            ], 200

    @register_openapi(
        name="Append a human score to a run",
        description="Appends a human annotation for a case x dimension (append-only: never overwrites). normalized_score is computed server-side; the reviewer is the current user. Triggers run re-aggregation.",
        request_body=EvalHumanScoreCreateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "run_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.human_score.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, run_id: int, **kwargs):
        try:
            data = EvalHumanScoreCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        reviewer_id = auth.current_user().get("id")
        try:
            row = write_human_score(project_id, run_id, data, reviewer_id=reviewer_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalHumanScoreDetailModel.model_validate(row).model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:run_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
