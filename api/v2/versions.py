import json
from flask import request

from tools import api_tools, auth, config as c, db, register_openapi
from sqlalchemy.orm import joinedload, selectinload
from pydantic import ValidationError


from ...models.pd.version import (
    ApplicationVersionDetailModel,
    ApplicationVersionListModel,
    ApplicationVersionCreateModel
)
from ...models.all import Application, ApplicationVersion
from ...utils.create_utils import create_version
from ...utils.constants import PROMPT_LIB_MODE


class ProjectAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List all versions of a specific agent or pipeline — returns summary with numeric IDs, names, and statuses; use this to find version IDs before executing or updating",
        description="Returns all versions for the specified agent or pipeline, ordered by creation date.",
        mcp_description="""
        USE to enumerate all versions of an agent or pipeline, find their numeric IDs, and check publish statuses.
        DO NOT USE when:
        - You need full version config (instructions, tools) → use get_version_details
        - You need application metadata → use get_agent_details
        
        Typical workflow: call list_versions first → find version.id by name → pass that integer to execute_agent or get_version_details.
        
        Examples:
        1. Find version_id of 'base' version:
        GET .../versions/prompt_lib/42/7
        → [{ 'id': 101, 'name': 'base', 'status': 'draft' }]
        → Use id=101 in POST /predict/101.
        
        2. Find published version: filter where status == 'published'.
        3. Find draft version to update: filter where status == 'draft' → pass its id to update_version.""",
        tags=["elitea_core/applications"],
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.versions.get"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, application_id: int, **kwargs):
        with db.with_project_schema_session(project_id) as session:
            application = session.query(Application).options(
                selectinload(Application.versions)
            ).get(application_id)
            if not application:
                return {
                    "ok": False,
                    "error": f"Application with id '{application_id}' doesn't exist"
                }, 400
            
            return [ApplicationVersionListModel.from_orm(version).model_dump(mode='json') for version in application.versions]
            
    @register_openapi(
        name="Add a new named draft version to an existing agent or pipeline — version name must not be 'base', agent type must match the parent application",
        description=(
            "Create a new version for an existing agent or pipeline. Optional body field "
            "copy_skills_from_version_id (int): copies the skills of that source version "
            "(same application only; invalid/foreign ids ignored) onto the new version."
        ),
        request_body=ApplicationVersionCreateModel,
        mcp_description="""
        USE to add a new draft version to an existing agent or pipeline for iteration, testing, or staging variants.

        Optionally pass copy_skills_from_version_id (source version's numeric id, same application) to copy that
        version's attached skills onto the new one — used by "Save As Version"; invalid/foreign ids are ignored.

        DO NOT USE when:
        - Application does not exist yet → use create_agent
        - Modifying an existing version → use update_version
        - Name is 'base' → reserved, will be rejected
        
        New agent version example:
        { 'name': 'v2-strict', 'agent_type': 'openai', 'llm_settings': { 'model_name': 'gpt-4o' }, 'instructions': 'Strict mode: bullet points only.' }
        
        New pipeline version example:
        { 'name': 'v2-parallel', 'agent_type': 'pipeline', 'llm_settings': { 'model_name': 'gpt-4o' }, 'instructions': 'nodes:\n  - id: fetch\n    type: llm\n  - id: analyze\n    type: llm\nedges:\n  - from: fetch\n    to: analyze' }
        
        Key errors:
        - name = 'base' → HTTP 400: use any other name.
        - Invalid YAML in pipeline instructions → HTTP 400.""",
        tags=["elitea_core/applications"],
        mcp_tool=True,
        available_to_users=True,
    )
    @auth.decorators.check_api({
        "permissions": ["models.applications.versions.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def post(self, project_id: int, application_id: int, **kwargs):
        payload = request.get_json()
        with db.get_session(project_id) as session:
            application = session.query(Application).get(application_id)
            if not application:
                return {
                    "ok": False,
                    "error": f"Application with id '{application_id}' doesn't exist"
                }
            
            # Unlike tools, attached skills are NOT carried as content in the payload.
            # Tools live in the client's form state and ride into the body as data
            # (version_details.tools), so they are re-materialized from the request.
            # Skills are version-scoped junction rows (EntitySkillMapping) that the form
            # never holds, so instead of trusting a client-supplied skill list we take a
            # pointer to the source version and copy its already-valid rows server-side.
            # The id is popped off the RAW payload BEFORE model_validate (rather than
            # living on ApplicationVersionCreateModel) so a malformed value can never
            # raise ValidationError and 400 the entire version create; copy_skill_mappings
            # int-coerces/guards it to a harmless no-op.
            copy_id = payload.pop('copy_skills_from_version_id', None)
            try:
                payload['user_id'] = payload['author_id'] = auth.current_user().get("id")
                payload['project_id'] = project_id

                version_data = ApplicationVersionCreateModel.model_validate(payload)
            except ValidationError as e:
                return e.errors(
                    include_url=False,
                    include_context=False,
                    include_input=False,
                ), 400
            version = create_version(
                version_data, application, session,
                copy_skills_from_version_id=copy_id,
            )
            # session.add(version)
            session.commit()

            # Explicitly load relationships since they are now lazy
            version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == version.id
            ).options(
                selectinload(ApplicationVersion.tools),
                selectinload(ApplicationVersion.tool_mappings),
                selectinload(ApplicationVersion.variables),
                selectinload(ApplicationVersion.tags)
            ).first()

            result = ApplicationVersionDetailModel.from_orm(version)
        return result.model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:application_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: ProjectAPI,
    }
