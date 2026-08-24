"""Eval dataset **import** — bulk CSV/JSON case import for EVAL-P1-B3 (§17.2).

Appends valid rows as ``import`` cases and returns an accepted-count + per-row error report;
invalid rows never abort the import. Editor-gated (dataset content mutation).
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.evaluation import (
    EvalDatasetImportModel,
    EvalDatasetCaseDetailModel,
)
from ...utils.evaluation_dataset_utils import import_cases
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Import cases into an eval dataset",
        description="Parses CSV/JSON content and appends valid rows as cases. Returns accepted/rejected counts, a per-row error report, and the created cases (§17.2).",
        request_body=EvalDatasetImportModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dataset_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dataset.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, dataset_id: int, **kwargs):
        try:
            data = EvalDatasetImportModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        agent_id = request.args.get('agent_id', type=int)
        try:
            report = import_cases(project_id, dataset_id, data.format, data.content, agent_id=agent_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return {
            "accepted": report["accepted"],
            "rejected": report["rejected"],
            "errors": report["errors"],
            "cases": [
                EvalDatasetCaseDetailModel.model_validate(c_).model_dump(mode='json')
                for c_ in report["cases"]
            ],
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:dataset_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
