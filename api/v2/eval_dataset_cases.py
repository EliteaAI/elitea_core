"""Eval dataset **cases** — nested collection endpoint (list + add) for EVAL-P1-B3 (§17.4).

Cases carry ``input`` + optional ``variables`` + optional ``expected_output`` (§17.1). Read is
viewer-visible; adding a case is editor-gated (dataset content mutation). New cases append to
the end of the ordered set.
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalDatasetCaseCreateModel,
    EvalDatasetCaseDetailModel,
)
from ...utils.evaluation_dataset_utils import get_dataset, add_case
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List cases in an eval dataset",
        description="Returns a dataset's cases ordered by order_index.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dataset_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dataset.read"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, dataset_id: int, **kwargs):
        with db.get_session(project_id) as session:
            dataset = get_dataset(project_id, dataset_id, session=session)
            if not dataset:
                return {"error": f"Eval dataset with id {dataset_id} not found"}, 404
            return [
                EvalDatasetCaseDetailModel.model_validate(c_).model_dump(mode='json')
                for c_ in dataset.cases
            ], 200

    @register_openapi(
        name="Add a case to an eval dataset",
        description="Appends a manually authored case to the dataset (§17.1).",
        request_body=EvalDatasetCaseCreateModel,
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
            data = EvalDatasetCaseCreateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            case = add_case(project_id, dataset_id, data)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalDatasetCaseDetailModel.model_validate(case).model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:dataset_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
