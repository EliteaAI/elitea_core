from flask import request
from pydantic import ValidationError

from ...models.pd.application import ApplicationUpdateModel
from ...utils.application_utils import (
    get_application_details,
    application_update,
    ApplicationVersionNonFoundError,
    VersionMismatchError,
    VersionNotUpdatableError
)
from ...utils.constants import PROMPT_LIB_MODE

from tools import api_tools, auth, config as c, db, register_openapi

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Retrieve full metadata and active-version configuration of a specific agent or pipeline by name-based version lookup",
        description="Returns the full details of the specified agent or pipeline. If version_name is provided, that specific version is returned. If omitted, the active default version is returned.",
        mcp_description="""
        USE when you know the application_id and want full metadata + configuration. Primary 'read one' tool for
        agents and pipelines.

        DO NOT USE when:
        - You only have a numeric version_id → use get_version_details
        - You need a list of agents → use list_agents
        - You want to run an agent → use execute_agent

        Agent vs. Pipeline in response:
        Check version_details.agent_type.
        'pipeline' → parse instructions as YAML to see graph.
        Other values → instructions is a system prompt.

        Examples:
        1. Get default version of agent 7: { project_id: 42, application_id: 7 } → returns default version config.
        2. Get named version of pipeline 15: { project_id: 42, application_id: 15, version_name: 'v2' } → returns version 'v2' with YAML graph.
        3. Detect type: if response.version_details.agent_type == 'pipeline' → parse instructions as YAML.
        """,
        parameters=[
            {"name": "version_name", "in": "query", "required": False,
             "schema": {"type": "string"},
             "description": "Version name to retrieve. If omitted, the active default version is returned."},
        ],
        path_suffix_override='<string:mode>/<int:project_id>/<int:application_id>',
        mcp_tool=True,
        tags=["elitea_core/applications"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.application.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, application_id: int, version_name: str | None = None, **kwargs):
        try:
            result = get_application_details(project_id, application_id, version_name)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        if not result['ok']:
            return {'error': result['msg']}, 400
        return result['data'], 200

    @register_openapi(
        name="Delete an agent or pipeline",
        description=(
            "Deletes a specific agent or pipeline version when version_name is provided, "
            "or permanently deletes the entire agent/pipeline with all versions when version_name is omitted. "
            "Full deletion is irreversible."
        ),
        parameters=[
            {"name": "version_name", "in": "query", "required": False,
             "schema": {"type": "string"},
             "description": "Version name to delete. When omitted, the entire application and all its versions are deleted."},
        ],
        path_suffix_override='<string:mode>/<int:project_id>/<int:application_id>',
        tags=["elitea_core/applications"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.application.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id, application_id, version_name: str = None):
        if version_name:
            with db.get_session(project_id) as session:
                from ...models.all import ApplicationVersion
                version = session.query(ApplicationVersion).filter(
                    ApplicationVersion.application_id == application_id,
                    ApplicationVersion.name == version_name,
                ).first()
                if not version:
                    return {"ok": False, "error": f"Version '{version_name}' not found"}, 404
                version_id = version.id
            result = self.module.delete_application_version(project_id, version_id)
            if isinstance(result, dict) and 'error' in result:
                return {"ok": False, "error": result['error']}, 400
            return '', 204
        result = self.module.delete_application(project_id, application_id)
        if isinstance(result, dict) and 'error' in result:
            return {"ok": False, "error": result['error']}, 400
        if result:
            return '', 204
        return {"ok": False, "error": "Application is not found"}, 400

    @register_openapi(
        name="Update an agent's or pipeline's top-level metadata and optionally its active version configuration in a single atomic operation",
        description="Updates the agent or pipeline metadata and its active version in a single request. The version referenced in the request body must match the agent or pipeline in the path.",
        mcp_description="""
        USE when you want to rename an agent/pipeline, change description, or update application + version
        together in one call.

        DO NOT USE when:
        - Only updating version config → use update_version
        - Target version is published or embedded → will fail
        - Creating a new version → use create_version

        Agent update example:
        { 'version': { 'id': 101, 'application_id': 7, 'instructions': 'New system prompt...', 'llm_settings': { 'model_name': 'gpt-4o', 'temperature': 0.2 } } }

        Pipeline update example:
        { 'version': { 'id': 202, 'application_id': 15, 'agent_type': 'pipeline', 'instructions': 'nodes:\n  - id: start\n    type: llm\n...' } }
        Omit pipeline_settings entirely if not changing the trigger.

        Rename only:
        { 'name': 'My Renamed Agent' } (no version field needed)

        Error: HTTP 400 'Version is published' → unpublish first, then update.
        """,
        request_body=ApplicationUpdateModel,
        tags=["elitea_core/applications"],
        available_to_users=True,
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.application.update"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def put(self, project_id: int, application_id: int):
        payload = dict(request.json)

        payload['project_id'] = project_id
        payload['user_id'] = auth.current_user()['id']

        try:
            update_data = ApplicationUpdateModel.model_validate(payload)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        if update_data.version.application_id != application_id:
            return {'error': f"{application_id=} mismatch with version payload value"}, 400

        if update_data.version and not update_data.version.author_id:
            update_data.version.author_id = auth.current_user().get("id")
        with db.get_session(project_id) as session:
            try:
                result = application_update(project_id, application_id, update_data, session=session)
            except (ApplicationVersionNonFoundError, VersionMismatchError, VersionNotUpdatableError) as ex:
                return {'error': str(ex)}, 400
            except Exception as e:
                log.exception(str(e))
                return {'error': f"Can not update {application_id=}"}, 400
            session.commit()
            return result, 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:application_id>',
        '<int:project_id>/<int:application_id>/<string:version_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
