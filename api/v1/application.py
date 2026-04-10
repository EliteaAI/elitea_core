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

from tools import api_tools, auth, config as c, db

from pylon.core.tools import log


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.application.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, application_id: int, version_name: str = None, **kwargs):
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

    @auth.decorators.check_api({
        "permissions": ["models.applications.application.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id, application_id):
        result = self.module.delete_application(project_id, application_id)
        if isinstance(result, dict) and 'error' in result:
            return {"ok": False, "error": result['error']}, 400
        if result:
            return '', 204
        return {"ok": False, "error": "Application is not found"}, 400

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
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>',
        '<int:project_id>/<int:application_id>',
        '<int:project_id>/<int:application_id>/<string:version_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
