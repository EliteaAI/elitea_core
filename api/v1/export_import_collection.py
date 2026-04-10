from datetime import date
import json
from io import BytesIO

from flask import request, send_file

from ...models.all import Collection
from ...utils.collections import group_by_project_id, deep_merge_collection_export_results
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.collection_registry import ENTITY_REG
from ...utils.exceptions import EntityNotAvailableCollectionError

from pylon.core.tools import log
from tools import api_tools, db, auth, config as c


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.promptlib_shared.export_import.export"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": True},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, collection_id: int = None, **kwargs):
        forked = 'fork' in request.args
        result = {}
        with db.with_project_schema_session(project_id) as session:
            collection = session.query(Collection).get(collection_id)
            if not collection:
                return {"error": f"Collection with id '{collection_id}' not found"}, 400

        try:
            for ent in ENTITY_REG:
                entities = ent.get_entities_field(collection)
                if not entities:
                    continue

                exported_data = ent.entity_export(
                    group_by_project_id(entities),
                    user_id=auth.current_user().get('id'),
                    forked=forked
                )

                for entity in exported_data[ent.entities_name]:
                    if 'original_exported' not in entity:
                        entity['original_exported'] = True

                result = deep_merge_collection_export_results(result, exported_data)

            if request.args.get('as_file', False):
                file = BytesIO()
                data = json.dumps(result, ensure_ascii=False, indent=4)
                file.write(data.encode('utf-8'))
                file.seek(0)
                return send_file(file, download_name=f'elitea_collection_{date.today()}.json', as_attachment=False)
        except EntityNotAvailableCollectionError as ex:
            return {"error": str(ex)}, 400
        except Exception as ex:
            log.error(ex)
            return {"error": f"Can not export {collection_id=}"}
        return result, 200


class API(api_tools.APIBase):
    module_name_override = "promptlib_shared"

    url_params = api_tools.with_modes([
        '<int:project_id>/<int:collection_id>',
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
