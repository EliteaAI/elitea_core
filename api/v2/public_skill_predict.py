from flask import request  # pylint: disable=E0401

from tools import api_tools, auth, config as c, register_openapi  # pylint: disable=E0401

from ...models.pd.skill_predict import SkillPredictRequest  # pylint: disable=E0402
from ...utils.constants import PROMPT_LIB_MODE  # pylint: disable=E0402
from ...utils.skill_run_utils import execute_skill_predict, is_async_query  # pylint: disable=E0402
from ...utils.utils import get_public_project_id  # pylint: disable=E0402


class PromptLibAPI(api_tools.APIModeHandler):  # pylint: disable=R0903
    @register_openapi(
        name="Run a published Catalog skill with user input, without an agent",
        description=(
            "Runs a published Catalog skill on behalf of project_id, which is billed for the run and "
            "supplies the model and project context. public_skill_id is the Catalog id, not a skill id "
            "of project_id. Runs the newest published version, or the published version given as a "
            "trailing path segment (/{project_id}/{public_skill_id}/{version_id}); a version that is not "
            "published is rejected. Model: request llm_settings, then the version's saved settings, then "
            "the project's default model. Sync by default; async_mode or callback_url returns a task_id."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "public_skill_id", "in": "path", "schema": {"type": "integer"}},
        ],
        path_suffix_override='<string:mode>/<int:project_id>/<int:public_skill_id>',
        tags=["elitea_core/skills"],
        request_body=SkillPredictRequest,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.predict.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @auth.decorators.check_api({
        "permissions": ["models.applications.public_application.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, public_skill_id: int, version_id: int | None = None, **kwargs):
        try:
            public_project_id = get_public_project_id()
        except Exception as e:  # pylint: disable=W0718
            return {"error": f"'ai_project_id' not set: {e}"}, 400
        return execute_skill_predict(
            self.module,
            request.get_json(silent=True),
            caller_project_id=project_id,
            skill_project_id=public_project_id,
            skill_id=public_skill_id,
            version_id=version_id,
            published_only=True,
            user_id=auth.current_user()["id"],
            async_requested=is_async_query(request.args),
        )


class API(api_tools.APIBase):  # pylint: disable=R0903
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:public_skill_id>',
        '<int:project_id>/<int:public_skill_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
