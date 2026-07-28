from sqlalchemy.orm.attributes import flag_modified
from pylon.core.tools import log

from tools import api_tools, auth, config as c, serialize, db

from ...models.elitea_tools import EliteATool
from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.index_meta.edit"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, toolkit_id: int, index_name: str):
        current_user_id = str(auth.current_user().get("id"))

        try:
            with db.get_session(project_id) as session:
                toolkit = session.query(EliteATool).filter(
                    EliteATool.id == toolkit_id
                ).first()
                if not toolkit:
                    return {"ok": False, "error": "Toolkit not found"}, 404

                meta = toolkit.meta or {}
                indexes_meta = meta.get("indexes_meta", {})
                index_entry = indexes_meta.get(index_name)

                if not index_entry:
                    return {"ok": False, "error": f"No schedule found for index '{index_name}'"}, 404

                schedules = index_entry.get("schedules", {})
                if current_user_id not in schedules:
                    return {"ok": False, "error": "No schedule found for current user"}, 404

                del schedules[current_user_id]
                index_entry["schedules"] = schedules
                indexes_meta[index_name] = index_entry
                toolkit.meta["indexes_meta"] = indexes_meta

                flag_modified(toolkit, "meta")
                session.commit()

                return serialize(indexes_meta), 200
        except Exception as e:
            log.error(f"Error deleting schedule for index '{index_name}' in toolkit {toolkit_id}: {e}")
            return {"ok": False, "error": "Error occurred while deleting index schedule"}, 400


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>/<string:index_name>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
