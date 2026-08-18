"""Eval dataset **case** — item endpoint (update + delete) for EVAL-P1-B3 (§17.4).

Editing/removing a single case. ``source_type`` is immutable post-create (§17.2). Mutation is
editor-gated per EVAL-H3.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, auth, register_openapi

from ...models.pd.evaluation import (
    EvalDatasetCaseUpdateModel,
    EvalDatasetCaseDetailModel,
)
from ...utils.evaluation_dataset_utils import update_case, delete_case
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Update an eval dataset case",
        description="Updates a case's input / variables / expected_output / meta. source_type is immutable.",
        request_body=EvalDatasetCaseUpdateModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dataset_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "case_id", "in": "path", "schema": {"type": "integer"}},
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
    def put(self, project_id: int, dataset_id: int, case_id: int, **kwargs):
        try:
            data = EvalDatasetCaseUpdateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            case = update_case(project_id, dataset_id, case_id, data)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalDatasetCaseDetailModel.model_validate(case).model_dump(mode='json'), 200

    @register_openapi(
        name="Delete an eval dataset case",
        description="Removes a case from the dataset. Past runs keep their frozen case set. Irreversible.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dataset_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "case_id", "in": "path", "schema": {"type": "integer"}},
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
    def delete(self, project_id: int, dataset_id: int, case_id: int, **kwargs):
        try:
            delete_case(project_id, dataset_id, case_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status
        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:dataset_id>/<int:case_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
