"""An agent's default evaluation dataset: the one the scheduled evaluation runs it on first.

Stored as ``Application.meta.default_eval_dataset_id``, the same shape as the default version, so
the tenant schema needs no migration. Set and cleared by the agent's editors; deleting the dataset
clears it (``evaluation_dataset_utils.delete_dataset``).
"""

from typing import Optional

from flask import request
from pydantic import BaseModel, ValidationError

from tools import api_tools, config as c, db, auth, register_openapi

from ...models.all import Application
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.evaluation_dataset_utils import get_dataset
from ...utils.folder_access import require_folder_access, APPLICATION_ENTITY_TYPES

DEFAULT_EVAL_DATASET_KEY = 'default_eval_dataset_id'


class DefaultEvalDatasetModel(BaseModel):
    # None clears the default
    dataset_id: Optional[int] = None


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get an agent's default eval dataset",
        description="Returns the dataset the scheduled evaluation runs this agent on first, or null.",
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "application_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.suite.read"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access(APPLICATION_ENTITY_TYPES, 'application_id')
    def get(self, project_id: int, application_id: int, **kwargs):
        with db.get_session(project_id) as session:
            application = session.query(Application).filter(Application.id == application_id).first()
            if application is None:
                return {"error": f"Application {application_id} not found"}, 404
            return {"dataset_id": (application.meta or {}).get(DEFAULT_EVAL_DATASET_KEY)}, 200

    @register_openapi(
        name="Set an agent's default eval dataset",
        description="Sets (or, with dataset_id null, clears) the dataset the scheduled evaluation "
                    "runs this agent on first. The dataset must be the agent's own or shared.",
        request_body=DefaultEvalDatasetModel,
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "application_id", "in": "path", "schema": {"type": "integer"}},
        ],
        tags=["elitea_core/evaluation"],
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.evaluation.suite.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access(APPLICATION_ENTITY_TYPES, 'application_id', write=True)
    def put(self, project_id: int, application_id: int, **kwargs):
        try:
            data = DefaultEvalDatasetModel.model_validate(request.json or {})
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400

        with db.get_session(project_id) as session:
            application = session.query(Application).filter(Application.id == application_id).first()
            if application is None:
                return {"error": f"Application {application_id} not found"}, 404
            if data.dataset_id is not None and get_dataset(
                    project_id, data.dataset_id, agent_id=application_id, session=session) is None:
                return {"error": f"Eval dataset with id {data.dataset_id} not found"}, 404
            meta = dict(application.meta or {})
            if data.dataset_id is None:
                meta.pop(DEFAULT_EVAL_DATASET_KEY, None)
            else:
                meta[DEFAULT_EVAL_DATASET_KEY] = data.dataset_id
            application.meta = meta
            session.commit()
            return {"dataset_id": data.dataset_id}, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>/<int:application_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
