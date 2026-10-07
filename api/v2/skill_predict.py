from flask import request  # pylint: disable=E0401

from tools import api_tools, auth, config as c, register_openapi  # pylint: disable=E0401

from .skill import resolve_version_id  # pylint: disable=E0402
from ...models.pd.skill_predict import SkillPredictMcpRequest, SkillPredictRequest  # pylint: disable=E0402
from ...utils.constants import PROMPT_LIB_MODE  # pylint: disable=E0402
from ...utils.folder_access import require_folder_access  # pylint: disable=E0402
from ...utils.skill_run_utils import execute_skill_predict, is_async_query, request_model_for  # pylint: disable=E0402
from ...utils.utils import get_public_project_id  # pylint: disable=E0402


class PromptLibAPI(api_tools.APIModeHandler):  # pylint: disable=R0903
    @register_openapi(
        name="Run a skill of this project with user input, without an agent",
        description=(
            "Runs the skill's default version, or the version given as a trailing path segment or version_id query parameter "
            "(/{project_id}/{skill_id}/{version_id}). The version's instructions are the system "
            "prompt and no tools are bound. Model: request llm_settings, then the version's saved "
            "settings, then the project's default model; model_fallback is true when the requested "
            "model was unavailable and the default ran instead. Sync by default; async_mode or "
            "callback_url returns a task_id immediately; with sid the run streams to that socket and the call "
            "returns task_id and stream_id. In the public project only published versions run."
        ),
        parameters=[
            {"name": "project_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "skill_id", "in": "path", "schema": {"type": "integer"}},
            {"name": "version_id", "in": "query", "required": False, "schema": {"type": "integer"}, "description": "Version to run; same as the trailing path segment"},
        ],
        path_suffix_override='<string:mode>/<int:project_id>/<int:skill_id>',
        mcp_description="""
        USE to run one of this project's skills on its own and get its answer. The skill's saved instructions are the system prompt; no agent is needed.
        DO NOT USE when:
        - Running an agent or pipeline → use the agent predict tool
        - Running a published Catalog skill → use the Catalog skill run tool
        - Reading a skill's content → use the skill details tool

        skill_id and the optional version_id are numeric. Without version_id the skill's default version runs.
        Body: { 'user_input': 'Review this paragraph for tone: ...' }
        → Waits and returns the answer with run_id, skill_version_id and the model used.""",
        tags=["elitea_core/skills"],
        request_body=SkillPredictRequest,
        mcp_request_body=SkillPredictMcpRequest,
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.predict.post"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @auth.decorators.check_api({
        "permissions": ["models.applications.skills.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access('skill', 'skill_id')
    def post(self, project_id: int, skill_id: int, version_id: int | None = None, **kwargs):
        version_id, error = resolve_version_id(version_id)
        if error:
            return error
        return execute_skill_predict(
            self.module,
            request.get_json(silent=True),
            caller_project_id=project_id,
            skill_project_id=project_id,
            skill_id=skill_id,
            version_id=version_id,
            published_only=project_id == get_public_project_id(),
            user_id=auth.current_user()["id"],
            async_requested=is_async_query(request.args),
            request_model=request_model_for(request.environ),
        )


class API(api_tools.APIBase):  # pylint: disable=R0903
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:skill_id>',
        '<int:project_id>/<int:skill_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
