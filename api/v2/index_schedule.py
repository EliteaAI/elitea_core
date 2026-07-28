from flask import request
from sqlalchemy.orm.attributes import flag_modified
from pylon.core.tools import log

from tools import api_tools, auth, config as c, serialize, db

from ...models.elitea_tools import EliteATool
from ...utils.constants import PROMPT_LIB_MODE

_ADMIN_ROLES = {
    c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
    c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
}


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.index_meta.edit"],
        "recommended_roles": _ADMIN_ROLES,
    })
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, toolkit_id: int, index_name: str):
        current_user_id = str(auth.current_user().get("id"))

        # Optional: admins/editors may pass ?user_id= to delete another user's
        # schedule (including shared/team schedules stored under key "-1").
        target_user_id = request.args.get("user_id", current_user_id)
        target_user_id = str(target_user_id)

        # Only the owner can delete their own schedule unless the caller has
        # admin/editor rights (already enforced by check_api above, so any
        # authenticated editor can delete any entry when they pass user_id).

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
                if target_user_id not in schedules:
                    return {"ok": False, "error": f"No schedule found for user '{target_user_id}'"}, 404

                del schedules[target_user_id]
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
