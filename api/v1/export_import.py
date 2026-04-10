import json
from io import BytesIO
from datetime import date

from flask import request, send_file
from pydantic.v1 import ValidationError
from tools import api_tools, auth, config as c

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.export_import import export_application


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.export_import.export"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, application_ids: str, **kwargs):
        application_ids = [int(id_str) for id_str in application_ids.split(",")]
        forked = 'fork' in request.args
        follow_version_ids = request.args.get('follow_version_ids', type=str)
        if follow_version_ids:
            follow_version_ids = [int(id_str) for id_str in follow_version_ids.split(",")]

        try:
            result = export_application(
                project_id=project_id,
                user_id=auth.current_user()['id'],
                application_ids=application_ids,
                forked=forked,
                follow_version_ids=follow_version_ids
            )
        except ValidationError as e:
            return e.errors(), 400

        if not result['ok']:
            return {'errors': {'applications': result['msg']}}, 400

        result.pop('ok')

        if 'as_file' in request.args:
            file = BytesIO()
            data = json.dumps(result, ensure_ascii=False, indent=4)
            file.write(data.encode('utf-8'))
            file.seek(0)
            return send_file(file, download_name=f'elitea_agents_{date.today()}.json', as_attachment=False)
        return result, 200


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<int:project_id>/<string:application_ids>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
