"""The project's consent to the scheduled evaluation scoring its agents from their stored chats.

Off by default. Read by anyone who may read evaluation datasets, so the settings page can show it;
set by project admins only.
"""

from flask import request
from pydantic import BaseModel, StrictBool, ValidationError
from pylon.core.tools import log  # pylint: disable=E0611,E0401

from tools import api_tools, config as c, auth, register_openapi

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.eval_history_consent import read_history_consent, save_history_consent
from ...utils.project_settings_access import require_project_settings_admin


class EvalHistoryConsentModel(BaseModel):
    history_evaluation_consent: StrictBool


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Get the project's history evaluation consent",
        description="Whether the scheduled evaluation may score the project's agents from their "
                    "stored chats.",
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
        try:
            consent = read_history_consent(project_id)
        except Exception:  # pylint: disable=broad-except
            log.exception("Could not read the history evaluation consent of project %s", project_id)
            return {"error": "The setting could not be read"}, 503
        return {"history_evaluation_consent": consent}, 200

    @register_openapi(
        name="Set the project's history evaluation consent",
        description="Turns on or off scoring the project's agents from their stored chats, private "
                    "ones included. Project admins only.",
        request_body=EvalHistoryConsentModel,
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
            data = EvalHistoryConsentModel.model_validate(request.json or {})
        except ValidationError as e:
            return e.errors(include_url=False, include_context=False, include_input=False), 400
        try:
            saved = save_history_consent(project_id, data.history_evaluation_consent)
        except Exception:  # pylint: disable=broad-except
            log.exception("Could not save the history evaluation consent of project %s", project_id)
            return {"error": "The setting could not be saved"}, 503
        return saved, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        "<int:project_id>",
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
