from traceback import format_exc

from queue import Empty
from flask import request

from ...utils.constants import PROMPT_LIB_MODE

from pylon.core.tools import log
from tools import api_tools, auth, config as c


def _merge_search_options_results(search_results):
    assert len(search_results) != 0

    res = {
        "collection": {
            "total": 0,
            "rows": []
        },
        "tag": {
            "total": 0,
            "rows": []
        }
    }
    for entity_name, entity_result in search_results.items():
        if entity_name in ('collection', 'tag'):
            for item in entity_result['rows']:
                if item not in res[entity_name]['rows']:
                    res[entity_name]['rows'].append(item)
            res[entity_name]['total'] = len(res[entity_name]['rows'])
        elif entity_name not in res:
            res[entity_name] = entity_result
        else:
            continue

    return res


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api(
        {
            "permissions": ["models.promptlib_shared.search"],
            "recommended_roles": {
                c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
                c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
            },
        }
    )
    @api_tools.endpoint_metrics
    def get(self, project_id: int):
        results = {}
        entities = set(request.args.getlist('entities[]'))

        for entity in ('application', 'pipeline', 'toolkit', 'credential'):
            results[entity] = {"total": 0, "rows": []}

        try:
            if 'toolkit' in entities:
                try:
                    res = self.module.applications_get_toolkit_search_options(
                        project_id,
                        **request.args.to_dict()
                    )
                except Empty:
                    log.warning("Application plugin is not available, skipping toolkits for search_options")
                else:
                    results['toolkit'] = res

            if "application" in entities:
                try:
                    res = self.module.applications_get_search_options(project_id)
                except Empty:
                    log.warning("Application plugin is not available, skipping for search_options")
                else:
                    results.update(res)

            if "pipeline" in entities:
                try:
                    res = self.module.applications_get_search_options(
                       project_id,
                       pipeline=True
                    )
                except Empty:
                    log.warning("Application plugin is not available, skipping for search_options")
                else:
                    results.update(res)

            if "credential" in entities:
                try:
                    res = self.module.context.rpc_manager.timeout(2).configurations_get_search_options(
                        project_id,
                        **request.args.to_dict()
                    )
                except Empty:
                    log.warning("Configurations plugin is not available, skipping for search_options")
                else:
                    results.update(res)

        except AttributeError as ex:
            log.error(ex)
            return {"error": f"One of the search conditions has invalid value: {ex.name}"}, 400
        except Exception as ex:
            log.error(format_exc())
            return {"error": str(ex)}, 400

        result = _merge_search_options_results(results)

        return result, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }