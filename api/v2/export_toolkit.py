import json
from io import BytesIO
from datetime import date

from flask import request, send_file
from pydantic.v1 import ValidationError
from tools import api_tools, auth, config as c

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.export_import import export_toolkits


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.export_toolkit.export"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, toolkit_ids: str, **kwargs):
        toolkit_ids = [int(id_str) for id_str in toolkit_ids.split(",")]
        forked = 'fork' in request.args

        try:
            result = export_toolkits(
                project_id=project_id,
                user_id=auth.current_user()['id'],
                toolkit_ids=toolkit_ids,
                forked=forked
            )
        except ValidationError as e:
            return e.errors(), 400

        if not result['ok']:
            return {'errors': {'toolkits': result['msg']}}, 400

        result.pop('ok')

        if 'as_file' in request.args:
            file = BytesIO()
            data = json.dumps(result, ensure_ascii=False, indent=4)
            file.write(data.encode('utf-8'))
            file.seek(0)
            return send_file(file, download_name=f'elitea_toolkits_{date.today()}.json', as_attachment=False)
        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<string:toolkit_ids>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
