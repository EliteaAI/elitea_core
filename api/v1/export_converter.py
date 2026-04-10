from flask import request
# from pydantic.v1 import ValidationError
from tools import api_tools, auth, config as c
from pylon.core.tools import log

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.export_import import generate_repeatable_uuid


class PromptLibAPI(api_tools.APIModeHandler):
#    @auth.decorators.check_api({
#        "permissions": ["models.applications.export_converter.transform"],
#        "recommended_roles": {
#            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
#            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
#        }})
#    @api_tools.endpoint_metrics
    def post(self, **kwargs):
        export_data = request.json
        toolkits = {}

        try:
            for application in export_data.get('applications', []):
                for version in application.get('versions', []):
                    toolkits_old = version.get('tools', [])
                    for i, toolkit in enumerate(toolkits_old):
                        if 'import_uuid' in toolkit:
                            continue
                        toolkit_import_uuid = generate_repeatable_uuid(
                            prefix='ToolExportBase',
                            values=toolkit.get('settings', {}),
                            suffix=toolkit.get('name', '')
                        )
                        toolkits_old[i] = {"import_uuid": toolkit_import_uuid}
                        toolkit['import_uuid'] = toolkit_import_uuid
                        toolkit.pop('author_id', None)
                        toolkits[toolkit_import_uuid] = toolkit
        except Exception as ex:
            log.error(ex)
            return {"error": "Can not transform export data"}, 400

        if toolkits:
            export_data['toolkits'] = list(toolkits.values())

        return export_data, 200


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([
        '<string:mode>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
