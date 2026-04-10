from flask import request
from pydantic import ValidationError

from tools import api_tools, config as c, db, auth, serialize, store_secrets
from pylon.core.tools import log

from ...models.all import EliteATool
from ...models.pd.tool import ToolDetails, ToolCreateModel
from ...utils.toolkits_utils import get_mcp_schemas
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.application_tools import toolkits_listing


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.tools.list"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        query = request.args.get('query')
        limit = request.args.get('limit', default=10, type=int)
        offset = request.args.get('offset', default=0, type=int)
        sort_by = request.args.get("sort_by", default="created_at")
        sort_order = request.args.get("sort_order", default='desc')
        toolkit_type = request.args.getlist("toolkit_type")
        filter_mcp = request.args.get('mcp', 'false').lower() == 'true'
        filter_application = request.args.get('application', 'false').lower() == 'true'
        author_id = request.args.get('author_id', type=int)

        try:
            result = toolkits_listing(
                project_id=project_id,
                query=query,
                toolkit_type=toolkit_type,
                limit=limit,
                offset=offset,
                sort_by=sort_by,
                sort_order=sort_order,
                filter_mcp=filter_mcp,
                filter_application=filter_application,
                author_id=author_id,
            )
            return result, 200
        except Exception as e:
            log.error(str(e))
            return {
                "ok": False,
                "error": "Failed to list toolkits"
            }, 400

    @auth.decorators.check_api(
        {
            "permissions": ["models.applications.tools.create"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
            },
        }
    )
    @api_tools.endpoint_metrics
    def post(self, project_id: int, **kwargs):
        data = dict(request.json)
        data['user_id'] = data['author_id'] = auth.current_user()['id']
        data['project_id'] = project_id

        try:
            tool_data = ToolCreateModel.model_validate(data)
        except ValidationError as e:
            return {"ok": False, "error": e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            )}, 400

        # if tool_data.type == 'application':
        #     with db.get_session(project_id) as session:
        #         application_tool: EliteATool = session.query(EliteATool).where(
        #             EliteATool.type == 'application',
        #             EliteATool.settings['application_version_id'].astext.cast(Integer) == tool_data.settings[
        #                 'application_version_id']
        #         ).first()
        #         if application_tool:
        #             result = ToolDetails.from_orm(application_tool)
        #             result.fix_name(project_id)
        #             result.set_online(project_id)
        #             result.set_agent_type(project_id)
        #             result.set_icon_meta(project_id)
        #             return serialize(result), 200

        tool_data.fix_name(project_id)

        user_id = auth.current_user()['id']
        if tool_data.type in get_mcp_schemas(project_id, user_id):
            tool_data.meta['mcp'] = True

        try:
            with db.get_session(project_id) as session:
                store_secrets(tool_data.dict(), project_id)

                application_tool = EliteATool(
                    **serialize(tool_data),
                )
                session.add(application_tool)
                session.commit()
                result = ToolDetails.from_orm(application_tool)
                result.fix_name(project_id)
                result.set_online(project_id)
                result.set_agent_type(project_id)
                result.set_agent_meta_and_fields(project_id)
                return serialize(result), 201
        except Exception as e:
            return {"ok": False, "error": str(e)}, 400


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes(
        [
            "<int:project_id>",
        ]
    )

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }