from flask import request
from tools import api_tools, auth, config as c, register_openapi
from pylon.core.tools import log

from ...utils.constants import PROMPT_LIB_MODE
from ...utils.export_import import generate_repeatable_uuid


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Convert Export Format",
        description="Transform legacy export data by generating stable import UUIDs for toolkits.",
        tags=["elitea_core/import_export"],
        parameters=[
            {"name": "mode", "in": "path", "required": True, "schema": {"type": "string"}, "description": "API mode (e.g. prompt_lib)."},
        ],
    )
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
    url_params = api_tools.with_modes([
        '<string:mode>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
