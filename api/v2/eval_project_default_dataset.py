"""The project's default evaluation dataset: the scheduled evaluation runs it on agents that have
no dataset of their own, before falling back to the platform health check.

Stored as ``EvalDataset.meta.project_default`` on one shared dataset, so the tenant schema needs no
migration. Set and cleared by project admins, like the other project settings.
"""

from typing import Optional

from flask import request
from pydantic import BaseModel, ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.evaluation_dataset_utils import get_project_default_dataset, set_project_default_dataset
from ...utils.evaluation_library_utils import EvalLibraryError
from ...utils.project_settings_access import require_project_settings_admin


class ProjectDefaultEvalDatasetModel(BaseModel):
    # None clears the default
    dataset_id: Optional[int] = None


def _payload(dataset) -> dict:
    if dataset is None:
        return {"dataset_id": None, "name": None}
    return {"dataset_id": dataset.id, "name": dataset.name}


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get the project's default eval dataset",
        description="Returns the shared dataset the scheduled evaluation runs agents without a "
                    "dataset of their own on, or nulls.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
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
    def get(self, project_id: int, **kwargs):
        with db.get_session(project_id) as session:
            return _payload(get_project_default_dataset(project_id, session=session)), 200

    @register_openapi(
        name="Set the project's default eval dataset",
        description="Sets (or, with dataset_id null, clears) the project's default dataset for the "
                    "scheduled evaluation. The dataset must be shared. Project admins only.",
        request_body=ProjectDefaultEvalDatasetModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.project_context.edit"],
        "recommended_roles": {
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    @require_project_settings_admin
    def put(self, project_id: int, **kwargs):
        try:
            data = ProjectDefaultEvalDatasetModel.model_validate(request.json or {})
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        with db.get_session(project_id) as session:
            try:
                dataset = set_project_default_dataset(project_id, data.dataset_id, session=session)
            except EvalLibraryError as exc:
                return {"error": str(exc)}, exc.http_status
            result = _payload(dataset)
            session.commit()
        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
