"""Eval **dataset** — item endpoint (get + update + delete) for EVAL-P1-B3 (§17.4).

Read is viewer-visible and returns the ordered case set; mutation is editor-gated per
EVAL-H3. Deleting a dataset cascades to its cases (§17.1).
"""

from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.pd.evaluation import (
    EvalDatasetUpdateModel,
    EvalDatasetDetailModel,
)
from ...utils.evaluation_dataset_utils import get_dataset, update_dataset, delete_dataset
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get an eval dataset",
        description="Returns a single eval dataset with its ordered case set.",
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
            return EvalDatasetDetailModel.model_validate(dataset).model_dump(mode='json'), 200

    @register_openapi(
        name="Update an eval dataset",
        description="Updates dataset-level fields (name, description, meta). Cases are managed via the cases API.",
        request_body=EvalDatasetUpdateModel,
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
    def put(self, project_id: int, dataset_id: int, **kwargs):
        try:
            data = EvalDatasetUpdateModel.model_validate(dict(request.json))
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        try:
            dataset = update_dataset(project_id, dataset_id, data)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status

        return EvalDatasetDetailModel.model_validate(dataset).model_dump(mode='json'), 200

    @register_openapi(
        name="Delete an eval dataset",
        description="Deletes a dataset and cascades to its cases. Past runs keep their frozen case set. Irreversible.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "dataset_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.dataset.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, dataset_id: int, **kwargs):
        try:
            delete_dataset(project_id, dataset_id)
        except EvalLibraryError as exc:
            return {"error": str(exc)}, exc.http_status
        return '', 204


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:dataset_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
