import json

from pydantic import ValidationError

from flask import request
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.application_utils import (
    validate_toolkit_details,
    ToolkitConnectionError,
)
from pylon.core.tools import log

from tools import api_tools, auth, config as c


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.toolkit_validator.check"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, toolkit_id: int, **kwargs):
        """
        GET endpoint for toolkit validation with connection check.
        Accepts mcp_tokens via X-Toolkit-Tokens header (JSON-encoded) for OAuth-enabled configurations.
        """
        user_id = auth.current_user().get('id')

        # Read OAuth tokens from header — avoids a request body on GET while still
        # supporting MCP and any other OAuth-enabled toolkit types.
        raw_tokens = request.headers.get('X-Toolkit-Tokens', '{}')
        try:
            mcp_tokens = json.loads(raw_tokens)
        except (ValueError, TypeError):
            mcp_tokens = {}
        log.info(f"Received mcp_tokens via header for toolkit {toolkit_id}")

        result = {'error': [], 'settings_errors': [], 'connection_errors': []}
        try:
            validate_toolkit_details(
                project_id=project_id,
                toolkit_id=toolkit_id,
                user_id=user_id,
                mcp_tokens=mcp_tokens,
                check_connection=True,
            )
        except ValidationError as e:
            result['settings_errors'] = e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            )
            return result, 400
        except ToolkitConnectionError as e:
            result['connection_errors'] = e.connection_errors
            return result, 400
        except Exception as e:
            result['error'] = str(e)
            return result, 400

        return result, 200

class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:toolkit_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
