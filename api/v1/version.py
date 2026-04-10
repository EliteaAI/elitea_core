from queue import Empty

import json
import copy
from flask import request
from pydantic import ValidationError
from typing import Optional

from pylon.core.tools import log
from sqlalchemy.orm import selectinload

from tools import api_tools, auth, config as c, db, VaultClient

from ...models.all import ApplicationVersion
from ...models.pd.version import (
    ApplicationVersionDetailModel,
    ApplicationVersionUpdateModel
)
from ...utils.application_utils import (
    applications_update_version,
    VersionNotUpdatableError
)
from ...utils.utils import mask_secret
from ....configurations.utils import expand_configuration
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.secrets import check_secret_header


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
    @auth.decorators.check_api({
        "permissions": ["models.applications.version.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, application_id: int, version_id: int, **kwargs):
        with db.with_project_schema_session(project_id) as session:
            application_version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == version_id,
                ApplicationVersion.application_id == application_id
            ).options(
                selectinload(ApplicationVersion.tools),
                selectinload(ApplicationVersion.tool_mappings),
                selectinload(ApplicationVersion.variables)
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

            log.debug(f"{result=}")
            return result, 200

    @auth.decorators.check_api({
        "permissions": ["models.applications.version.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
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

        return version_details, 200

    @auth.decorators.check_api({
        "permissions": ["models.applications.version.update"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def put(self, project_id: int, application_id: int, version_id: int = None, **kwargs):
        version_data = dict(request.json)
        version_data['author_id'] = auth.current_user().get("id")
        version_data['application_id'] = application_id
        version_data['id'] = version_id
        version_data['project_id'] = project_id
        try:
            version_data = ApplicationVersionUpdateModel.model_validate(version_data)
            with db.with_project_schema_session(project_id) as session:
                res = applications_update_version(version_data, session)
            if not res['updated']:
                return res['msg'], 400
        except VersionNotUpdatableError as e:
            return {'error': str(e)}, 400
        except ValidationError as e:
            return e.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            ), 400

        return res['data'], 201

    @auth.decorators.check_api({
        "permissions": ["models.applications.version.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
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
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:application_id>',
        '<int:project_id>/<int:application_id>/<int:version_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
