from queue import Empty

import json
import copy
from flask import request
from pydantic import ValidationError
from typing import Optional

from pylon.core.tools import log
from sqlalchemy.orm import selectinload

from tools import api_tools, auth, config as c, db, VaultClient, register_openapi

from ...models.all import Application, ApplicationVersion
from ...models.pd.version import (
    ApplicationVersionDetailModel,
    ApplicationVersionMcpUpdateModel,
    ApplicationVersionUpdateModel
)
from ...utils.application_utils import (
    applications_update_version,
    VersionNotUpdatableError
)
from ...utils.create_utils import clone_persisted_application_version
from ...utils.mcp_versioning import (
    INTERNAL_MCP_ENVIRON_KEY,
    InstructionsPatchConflictError,
    build_mcp_backup_version_name,
    instructions_sha256,
    sanitize_mcp_settings_update,
)
from ...utils.skill_utils import apply_runtime_skills
from ...utils.utils import mask_secret
from ....configurations.utils import expand_configuration
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.secrets import check_secret_header
from ...utils.folder_access import require_folder_access, APPLICATION_ENTITY_TYPES


def extract_user_id(received_auth_session: Optional[str]) -> int:
    user_id = None
    if received_auth_session and received_auth_session != '-':
        session_context = auth.get_referenced_auth_context(received_auth_session)
        if session_context:
            user_id = session_context.get('user_id')
    else:
        user_id = auth.current_user().get('id')
    return user_id


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Retrieve the complete configuration of a specific agent or pipeline version by numeric version ID — includes resolved tool metadata, LLM settings, and pipeline YAML graph",
        description="Returns the full configuration of a specific agent or pipeline version, including toolkits, tools, tool mappings, and variables.",
        mcp_description="""
        USE when you have a numeric version_id and need full tool, configuration, or instruction details.
        DO NOT USE when:
        - You only know the version name → use get_agent_details with version_name
        - You need application metadata (name, description) → use get_agent_details
        - You need a list of all versions → use list_versions

        Reading the response by type:
        Agent: response.instructions = system prompt text; response.llm_settings = model config.
        Pipeline: response.instructions = YAML string → parse to understand graph nodes and edges.

        Examples:
        1. Read agent system prompt: GET .../version/prompt_lib/42/7/101
        → response.instructions = 'You are a code review expert...'

        2. Inspect pipeline graph: GET .../15/202
        → response.agent_type = 'pipeline' → parse response.instructions as YAML.

        3. Check available tools: response.tools[].settings.selected_tools = restricted tool list for this version.""",
        tags=["elitea_core/applications"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access(APPLICATION_ENTITY_TYPES, 'application_id')
    def get(self, project_id: int, application_id: int, version_id: int, **kwargs):
        with db.with_project_schema_session(project_id) as session:
            application_version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == version_id,
                ApplicationVersion.application_id == application_id
            ).options(
                selectinload(ApplicationVersion.tools),
                selectinload(ApplicationVersion.tool_mappings),
                selectinload(ApplicationVersion.variables),
                selectinload(ApplicationVersion.tags)
            ).first()
            if not application_version:
                return {'error': f'Application[{application_id}] version[{version_id}] not found'}, 400
            version_details = ApplicationVersionDetailModel.from_orm(application_version)
            for tool in version_details.tools:
                tool.set_agent_type(project_id)
                tool.fix_name(project_id)
                tool.set_agent_meta_and_fields(project_id)
                tool.set_online(project_id)

            result = version_details.model_dump(mode='json')
            result['instructions_sha256'] = instructions_sha256(
                application_version.instructions
            )

            log.debug(f"{result=}")
            return result, 200

    @auth.decorators.check_api({
        "permissions": ["models.applications.version.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access(APPLICATION_ENTITY_TYPES, 'application_id', write=True)
    def patch(self, project_id: int, application_id: int, version_id: int, **kwargs):
        received_secret = request.headers.get('X-SECRET')
        received_auth_session = request.headers.get('X-USERSESSION')
        user_id = extract_user_id(received_auth_session)
        if user_id is None:
            log.debug(f"Invalid auth session: {received_auth_session}, {mask_secret(received_secret, 6)}")
            return {'error': 'Invalid auth session'}, 400
        unsecret = check_secret_header(received_secret, project_id=project_id)
        if not unsecret:
            return {'error': 'Invalid secret header'}, 400

        version_details = self.module.get_application_version_details_expanded(
            project_id=project_id,
            application_id=application_id,
            version_id=version_id,
            user_id=user_id
        )
        if 'error' in version_details:
            return {'error': version_details['error']}, 404

        # #5267: MCP tools are computed at runtime, not stored in the DB. The direct-chat
        # path injects them in generate_toolkit_payload(); the SDK sub-agent path fetches
        # version details through this endpoint and never received them. Inject here, scoped
        # to the resolved end-user (own private project + own token) — see
        # inject_mcp_toolkits(). Guarded and non-fatal: agents without 'internal_mcp' return
        # before any RPC/token work.
        # project_id is the SDK client's project, which is the conversation's
        # (predict_utils.py sets llm.kwargs.project_id = parsed.project_id), and the SDK only
        # reaches this endpoint for a sub-agent in that same project — a cross-project one is
        # served by get_public_app_details instead. So it is the right scope: without it, a
        # sub-agent carrying builder tools would be an unclamped route out of a clamped
        # conversation.
        try:
            from ...utils.internal_tools import inject_mcp_toolkits
            agent_internal_tools = (version_details.get('meta') or {}).get('internal_tools', [])
            mcp_tools = inject_mcp_toolkits(
                user_id=user_id,
                current_project_id=project_id,
                internal_tools=agent_internal_tools,
                existing_tools=version_details.get('tools'),
                scope_project_id=project_id,
            )
            if mcp_tools:
                version_details.setdefault('tools', [])
                version_details['tools'].extend(mcp_tools)
        except Exception as e:
            log.warning(f"[#5267] Failed to inject MCP toolkits into version details: {e}")

        try:
            from ...utils.internal_tools import dedupe_internal_mcp_tools, resolve_internal_mcp_tools
            dedupe_internal_mcp_tools(version_details.get('tools'))
            resolve_internal_mcp_tools(version_details.get('tools'), user_id, project_id)
        except Exception as e:
            log.warning(f"Failed to resolve internal MCP toolkits in version details: {e}")

        apply_runtime_skills(version_details)

        return version_details, 200

    @register_openapi(
        name="Update non-instruction settings of an existing draft agent or pipeline version — internal MCP prompt and YAML edits use the safe patch tool",
        description="Updates the configuration of an existing agent or pipeline version. Only versions that are not published state can be updated.",
        request_body=ApplicationVersionUpdateModel,
        mcp_request_body=ApplicationVersionMcpUpdateModel,
        mcp_description="""
        USE to modify non-instruction fields of an existing draft agent or pipeline version.
        DO NOT USE when:
        - Renaming application or changing description → use update_agent
        - Changing agent instructions or pipeline YAML → use the safe instructions patch tool
        - Version is published or embedded → will fail; unpublish first or use create_version
        - Creating a new version → use create_version

        REQUIRED path params: project_id, application_id, version_id (the numeric version ID).
        Only pass fields you want to change — unset fields are NOT overwritten. Identity,
        author, and the current version name are resolved by the server.

        IMPORTANT: This tool cannot change `instructions`. For an instruction change only,
        first read the version, then use the safe instructions patch tool with the returned
        instructions_sha256.
        This tool automatically creates a backup version before other internal-MCP edits.

        Agent settings example:
        { 'llm_settings': { 'model_name': 'gpt-5-mini', 'temperature': 0.1 } }

        Pipeline metadata example:
        { 'welcome_message': 'Pipeline ready.' }
        → Omit instructions and pipeline_settings to preserve the graph and trigger.

        Error: HTTP 400 'Version is published' → unpublish first, then update.""",
        tags=["elitea_core/applications"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access(APPLICATION_ENTITY_TYPES, 'application_id', write=True)
    def put(self, project_id: int, application_id: int, version_id: int = None, **kwargs):
        version_data = dict(request.json)
        internal_mcp_request = bool(request.environ.get(INTERNAL_MCP_ENVIRON_KEY))
        version_data['author_id'] = auth.current_user().get("id")
        version_data['application_id'] = application_id
        version_data['id'] = version_id
        version_data['project_id'] = project_id
        try:
            with db.with_project_schema_session(project_id) as session:
                backup_version = None
                if internal_mcp_request:
                    source_version = session.query(ApplicationVersion).filter(
                        ApplicationVersion.id == version_id,
                        ApplicationVersion.application_id == application_id,
                    ).one_or_none()
                    application = session.query(Application).filter(
                        Application.id == application_id,
                    ).one_or_none()
                    if not source_version or not application:
                        return {'error': 'Application version not found'}, 404
                    # The schema no longer exposes instructions, but cached and older MCP
                    # clients can keep sending its former empty-string default after deploy.
                    version_data = sanitize_mcp_settings_update(
                        version_data, source_version.instructions
                    )
                    version_data.setdefault('name', source_version.name)

                version_data = ApplicationVersionUpdateModel.model_validate(version_data)
                if internal_mcp_request:
                    backup_version = clone_persisted_application_version(
                        source_version=source_version,
                        application=application,
                        new_version_name=build_mcp_backup_version_name(version_id),
                        author_id=version_data.author_id,
                        session=session,
                    )

                res = applications_update_version(version_data, session, commit=False)
                if not res['updated']:
                    session.rollback()
                    return res['msg'], 400
                session.commit()
                if backup_version is not None:
                    res['data']['mcp_backup_version'] = {
                        'id': backup_version.id,
                        'name': backup_version.name,
                    }
        except VersionNotUpdatableError as e:
            return {'error': str(e)}, 400
        except InstructionsPatchConflictError as e:
            return {'error': str(e)}, 409
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        return res['data'], 201

    @register_openapi(
        name="Delete a specific agent or pipeline version by numeric version ID — optionally reassign dependents to a replacement version",
        description="Deletes a specific agent or pipeline version. If the version is referenced by other entities, provide a replacement_version_id to reassign dependents before deletion.",
        mcp_description="""
        USE to permanently delete a draft or unpublished agent or pipeline version.
        DO NOT USE when:
        - You want to archive or unpublish a version → use update_version instead
        - The version is currently published or embedded and referenced — provide replacement_version_id to safely reassign dependents

        REQUIRED path params: project_id, application_id, version_id (the numeric version ID).
        OPTIONAL query param: replacement_version_id — numeric ID of the version to reassign dependents to before deleting.

        Example: DELETE .../prompt_lib/42/7/101?replacement_version_id=99
        → Reassigns all references from version 101 to version 99, then deletes version 101.

        Error: HTTP 400 with 'error' field — e.g. version not found or deletion not allowed.""",
        tags=["elitea_core/applications"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access(APPLICATION_ENTITY_TYPES, 'application_id', write=True)
    def delete(self, project_id: int, application_id: int, version_id: int = None):
        # Get optional replacement_version_id from query params
        replacement_version_id = request.args.get('replacement_version_id')
        if replacement_version_id:
            try:
                replacement_version_id = int(replacement_version_id)
            except (ValueError, TypeError):
                return {"ok": False, "error": "Invalid replacement_version_id"}, 400

        result = self.module.delete_application_version(
            project_id, version_id, replacement_version_id=replacement_version_id
        )
        if 'error' in result:
            return {"ok": False, "error": result['error']}, 400
        return result, 200

class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:application_id>',
        '<int:project_id>/<int:application_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
