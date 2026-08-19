from traceback import format_exc

import json

from flask import request
from pydantic import ValidationError
from queue import Empty

from sqlalchemy.orm import selectinload
from tools import api_tools, config as c, db, auth, serialize, register_openapi, rpc_tools

from ...models.pd.application import (
    ApplicationCreateModel,
    ApplicationDetailModel,
    ApplicationVersionDetailModel,
    MultipleApplicationListModel,
    MultiplePublishedApplicationListModel,
)

from ...models.all import ApplicationVersion
from ...utils.create_utils import create_application
from ...utils.application_utils import list_applications_api
from ...utils.utils import get_public_project_id
from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log


def get_enabled_agent_internal_tools(personalization) -> list:
    """
    Extract enabled agent internal tool names from user personalization settings.
    Maps field names like 'default_agent_image_generation_enabled' to tool names like 'image_generation'.
    
    Args:
        personalization: dict or PersonalizationModel object
    
    Returns:
        list of enabled internal tool names
    """
    if not personalization:
        return []
    
    # Convert Pydantic model to dict if needed
    if hasattr(personalization, 'model_dump'):
        personalization_dict = personalization.model_dump(exclude_none=True)
    elif hasattr(personalization, 'dict'):
        personalization_dict = personalization.dict(exclude_none=True)
    else:
        personalization_dict = personalization
    
    # Map personalization field names to internal tool names
    field_to_tool_map = {
        'default_agent_internal_mcp_enabled': 'internal_mcp',
        'default_agent_skill_builder_enabled': 'skill_builder',
        'default_agent_project_context_builder_enabled': 'project_context_builder',
        'default_agent_ask_user_enabled': 'ask_user',
        'default_agent_image_generation_enabled': 'image_generation',
        'default_agent_data_analysis_enabled': 'data_analysis',
        'default_agent_planner_enabled': 'planner',
        'default_agent_pyodide_enabled': 'pyodide',
        'default_agent_swarm_enabled': 'swarm',
        'default_agent_lazy_tools_mode_enabled': 'lazy_tools_mode',
    }
    
    enabled_tools = []
    for field_name, tool_name in field_to_tool_map.items():
        if personalization_dict.get(field_name):
            enabled_tools.append(tool_name)
    
    return enabled_tools



class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="List and search agents and pipelines in a project — paginated, filterable by type (agent vs. pipeline), tags, author, status, and free text",
        description="Returns a paginated list of agents and pipelines in the project. Supports filtering by tags, author, status, type, and free-text search.",
        mcp_description="""
        USE to discover, search, or browse agents and pipelines, and to find application_id before calling other tools.
        DO NOT USE when:
        - Already have application_id and need full config → use get_agent_details
        - Need tool or instruction details → use get_version_details
        - Want to execute an agent → use execute_agent
        
        Filter guidance:
        - All agents + pipelines: omit agents_type
        - Only classic agents: agents_type=classic
        - Only pipelines: agents_type=pipeline
        - Only pipelines with interrupts: agents_type=pipeline then filter rows where has_interrupt == true
        
        Examples:
        1. List all: GET .../applications/prompt_lib/42
        2. Pipelines only: GET ...?agents_type=pipeline
        3. Search by name: GET ...?query=code+review
        4. Published agents only: GET ...?statuses=published
        5. Page 2, 20 per page: GET ...?limit=20&offset=20""",
        mcp_tool=True,
        tags=["elitea_core/applications"],
        parameters=[
            {"name": "query", "in": "query", "required": False, "schema": {"type": "string"},
             "description": "Free-text search filter on agent/pipeline name."},
            {"name": "limit", "in": "query", "required": False, "schema": {"type": "integer", "default": 10},
             "description": "Maximum number of results to return."},
            {"name": "offset", "in": "query", "required": False, "schema": {"type": "integer", "default": 0},
             "description": "Pagination offset."},
            {"name": "sort_by", "in": "query", "required": False, "schema": {"type": "string", "default": "created_at"},
             "description": "Field to sort by."},
            {"name": "sort_order", "in": "query", "required": False, "schema": {"type": "string", "default": "desc"},
             "description": "Sort order (asc or desc)."},
            {"name": "tags", "in": "query", "required": False, "schema": {"type": "string"},
             "description": "Filter by tag name(s)."},
            {"name": "author_id", "in": "query", "required": False, "schema": {"type": "integer"},
             "description": "Filter by author user ID."},
            {"name": "statuses", "in": "query", "required": False, "schema": {"type": "string"},
             "description": "Filter by publication status (e.g. 'published', 'draft')."},
            {"name": "agents_type", "in": "query", "required": False, "schema": {"type": "string"},
             "description": "Filter by agent type: 'pipeline' for pipelines, 'classic' for classic agents. Omit for all."},
            {"name": "my_liked", "in": "query", "required": False, "schema": {"type": "boolean", "default": False},
             "description": "If true, return only agents/pipelines liked by the current user."},
        ],
        available_to_users=True,
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.applications.list"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int | None = None, **kwargs):
        with db.get_session(project_id) as session:
            some_result = list_applications_api(
                project_id=project_id,
                tags=request.args.get('tags'),
                author_id=request.args.get('author_id'),
                q=request.args.get('query'),
                limit=request.args.get("limit", default=10, type=int),
                offset=request.args.get("offset", default=0, type=int),
                sort_by=request.args.get("sort_by", default="created_at"),
                sort_order=request.args.get("sort_order", default='desc'),
                my_liked=request.args.get('my_liked', False),
                trend_start_period=request.args.get('trend_start_period'),
                trend_end_period=request.args.get('trend_end_period'),
                statuses=request.args.get('statuses'),
                agents_type=request.args.get('agents_type'),
                without_tags=request.args.get('without_tags', False),
                session=session
            )
        list_model = (
            MultiplePublishedApplicationListModel
            if project_id == get_public_project_id()
            else MultipleApplicationListModel
        )
        try:
            parsed = list_model(applications=some_result['applications'])
            return {
                'total': some_result['total'],
                'rows': [
                    serialize(i)
                    for i in parsed.applications
                ]
            }, 200
        except Exception as e:
            log.error(f'application list exc\n{format_exc()}')
            return {
                "ok": False,
                "error": str(e)
            }, 400

    @register_openapi(
        name="Create a new agent or pipeline with a mandatory initial 'base' version — agent type is set by agent_type inside the version definition",
        description="Creates a new agent or pipeline with an initial (base) version. The request must include agent or pipeline metadata and at least one version definition.",
        request_body=ApplicationCreateModel,
        mcp_description=f"""
        USE to create a brand-new agent or pipeline from scratch.
        DO NOT USE when:
        - Adding a version to existing app → use create_version
        - Forking an existing agent → use the fork endpoint
        - Importing from JSON → use the import endpoint
        
        Classic agent payload:
        {{"name": "Code Reviewer", "owner_id": 42, "versions": [{{"name": "base", "agent_type": "openai", "llm_settings": {{"model_name": "gpt-5-mini"}}, "instructions": "You are a senior engineer..."}}]}}
        
        Pipeline payload:
        {{"name": "CI Pipeline", "owner_id": 42, "versions": [{{"name": "base", "agent_type": "pipeline", "llm_settings": {{"model_name": "gpt-5-mini"}}, "instructions": "nodes:\\n  - id: start\\n    type: llm\\nedges:\\n  ..."}}]}}
        
        Params and default values :
        - owner_id is current project ID
        - project_id is current project ID
        - user_id is current user ID
        
        Key errors:
        - versions[0].name != 'base' → HTTP 400
        - len(versions) > 1 → HTTP 400
        - Invalid YAML in pipeline instructions → HTTP 400
        """,
        mcp_tool=True,
        tags=["elitea_core/applications"],
        available_to_users=True,
    )
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.applications.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int | None = None, **kwargs):
        raw = dict(request.json)
        raw["owner_id"] = project_id
        author_id = auth.current_user().get("id")
        raw['project_id'] = project_id
        raw['user_id'] = author_id
        for version in raw.get("versions", []):
            version["author_id"] = author_id
         
        # Apply default internal tools from user personalization if not explicitly set
        try:
            user_personalization = rpc_tools.RpcMixin().rpc.timeout(2).social_get_user(author_id)
            if user_personalization and 'personalization' in user_personalization:
                default_internal_tools = get_enabled_agent_internal_tools(
                    user_personalization['personalization']
                )
                # Apply to all versions that don't already have internal_tools set in meta
                for version in raw.get("versions", []):
                    meta = version.get('meta') or {}
                    if 'internal_tools' not in meta and default_internal_tools:
                        meta['internal_tools'] = default_internal_tools
                        version['meta'] = meta
        except Empty:
            log.debug(f"social_get_user RPC not available, skipping default internal tools")
        except Exception as e:
            log.warning(f"Failed to fetch user personalization for default internal tools: {e}")
         
        try:
            application_data = ApplicationCreateModel.model_validate(raw)
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        with db.get_session(project_id) as session:
            application = create_application(application_data, session, project_id)
            session.commit()

            # Explicitly load relationships for the first version since they are now lazy
            first_version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == application.versions[0].id
            ).options(
                selectinload(ApplicationVersion.tools),
                selectinload(ApplicationVersion.tool_mappings),
                selectinload(ApplicationVersion.variables),
                selectinload(ApplicationVersion.tags)
            ).first()

            result = ApplicationDetailModel.from_orm(application)
            result.version_details = ApplicationVersionDetailModel.from_orm(first_version)

            return result.model_dump(mode='json'), 201


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(
        [
            "",
            "<int:project_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
