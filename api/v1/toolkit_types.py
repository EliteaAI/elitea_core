from flask import request
from sqlalchemy import Boolean

from pylon.core.tools import log
from tools import api_tools, config as c, db, auth, serialize, store_secrets

from ...models.elitea_tools import EliteATool
from ...utils.constants import PROMPT_LIB_MODE


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
        filter_mcp = request.args.get('mcp', 'false').lower() == 'true'
        filter_application = request.args.get('application', 'false').lower() == 'true'

        try:
            with db.get_session(project_id) as session:
                q = session.query(EliteATool.type).distinct()

                if filter_mcp:
                    # Filter for MCP toolkits: either meta['mcp'] is True OR type is 'mcp'
                    q = q.filter(
                        (EliteATool.meta['mcp'].astext.cast(Boolean) == True) |
                        (EliteATool.type == 'mcp')
                    )
                else:
                    # Filter out MCP toolkits: meta['mcp'] must be False/None AND type must not be 'mcp'
                    q = q.filter(
                        (EliteATool.meta['mcp'].astext.cast(Boolean) == False) |
                        (EliteATool.meta['mcp'].astext.is_(None))
                    ).filter(
                        EliteATool.type != 'mcp'
                    )

                if filter_application:
                    # Filter for application toolkits: either meta['application'] is True OR type is 'application'
                    q = q.filter(
                        (EliteATool.meta['application'].astext.cast(Boolean) == True) |
                        (EliteATool.type == 'application')
                    )
                else:
                    # Filter out application toolkits: meta['application'] must be False/None AND type must not be 'application'
                    q = q.filter(
                        (EliteATool.meta['application'].astext.cast(Boolean) == False) |
                        (EliteATool.meta['application'].astext.is_(None))
                    ).filter(
                        EliteATool.type != 'application'
                    )

                tool_types = {tool_type[0] for tool_type in q.all()}
                return {"rows": serialize(tool_types), "total": len(tool_types)}
        except Exception as e:
            log.error(str(e))
            return {
                "ok": False,
                "error": "Failed to list toolkit types"
            }, 400

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