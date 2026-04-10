import json
from datetime import datetime, UTC

from flask import request
from pydantic import ValidationError
from pylon.core.tools import log
from sqlalchemy import cast, create_engine, inspect, Numeric, nullslast
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm.attributes import flag_modified

from tools import api_tools, auth, config as c, serialize, db, VaultClient
from ...models.elitea_tools import EliteATool
from ...models.indexer import EmbeddingStore
from ...models.pd.index import UpdateIndexingSchedule, ToolkitIndexingSchedule
from ...utils.application_tools import (
    load_and_validate_toolkit_for_index,
    get_session_for_schema,
    is_index_stale,
    clean_up_schedule_in_toolkit,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.predict_utils import get_toolkit_config


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.index_meta.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, toolkit_id: int, **kwargs):
        toolkit_config = get_toolkit_config(project_id, auth.current_user()['id'], toolkit_id)
        toolkit_name_id, connection_string, validation_error = load_and_validate_toolkit_for_index(toolkit_config)
        if validation_error:
            return validation_error
        #
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        table_exists = inspector.has_table("langchain_pg_embedding", schema=toolkit_name_id)
        if not table_exists:
            log.warning(f"Table {toolkit_name_id}.langchain_pg_embedding does not exist. Probably no data has been indexed yet.")
            return [], 200
        #
        try:
            with get_session_for_schema(connection_string, toolkit_name_id) as session:
                meta = session.query(
                    EmbeddingStore.id,
                    EmbeddingStore.cmetadata
                ).filter(
                    EmbeddingStore.cmetadata['type'].astext == 'index_meta'
                ).order_by(
                    nullslast(
                        cast(EmbeddingStore.cmetadata['updated_on'].astext, Numeric).desc()
                    )
                ).all()
                result = []
                # Get task disconnect timeout from vault secrets
                vault_client = VaultClient(project_id)
                secrets = vault_client.get_secrets()
                task_disconnected_timeout = int(secrets.get('task_disconnected_timeout_sec', 7200))
                
                for id, cmetadata in meta:
                    for key in ['index_configuration', 'history']:
                        if cmetadata and key in cmetadata:
                            try:
                                cmetadata[key] = json.loads(cmetadata[key])
                            except (TypeError, json.JSONDecodeError):
                                log.warning(f"Failed to decode {key} for index_meta {id}")
                    # highlight the fist history item as 'created' if it is completed successfully
                    if 'history' in cmetadata and len(cmetadata['history']) > 0 and cmetadata['history'][0]['state'] == 'completed':
                        cmetadata['history'][0]['state'] = 'created'
                    #
                    # Determine if task is stale (in_progress but not updated recently)
                    updated_on = cmetadata.get('updated_on', 0)
                    index_data_state = cmetadata.get('state', '')
                    stale = is_index_stale(updated_on, index_data_state, task_disconnected_timeout)
                    #
                    result.append({
                        "id": id,
                        "metadata": cmetadata,
                        "stale": stale
                    })
                return serialize(result), 200
        except Exception as e:
            session.rollback()
            log.error(f"Error occurred while fetching index_meta: {e}")
            return {"ok": False, "error": "Error occurred while fetching index_meta"}, 400

    @auth.decorators.check_api({
        "permissions": ["models.applications.index_meta.delete"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def delete(self, project_id: int, toolkit_id: int, index_meta_id: str):
        toolkit_config = get_toolkit_config(project_id, auth.current_user()['id'], toolkit_id)
        toolkit_name_id, connection_string, validation_error = load_and_validate_toolkit_for_index(toolkit_config)
        if validation_error:
            return validation_error
        #
        index_name = None
        #
        try:
            with get_session_for_schema(connection_string, toolkit_name_id) as session:
                obj = session.query(EmbeddingStore).filter(EmbeddingStore.id == index_meta_id).one()
                index_name = obj.cmetadata["collection"]
                #
                session.query(EmbeddingStore).filter(
                    EmbeddingStore.cmetadata["collection"].astext == index_name
                ).delete(synchronize_session=False)
                session.commit()
                log.debug(f"Deleted all index_meta with collection '{index_name}' from toolkit {toolkit_id}")
        except NoResultFound:
            return {"ok": False, "error": f"index_meta {index_meta_id} not found"}, 404
        except Exception as e:
            session.rollback()
            log.error(f"Error occurred while deleting index_meta {index_meta_id}: {e}")
            return {"ok": False, "error": "Error occurred while deleting index_meta"}, 400

        clean_result, code = clean_up_schedule_in_toolkit(project_id, toolkit_id, index_name)
        if not clean_result.get("ok", True):
            log.error(f"Error occurred while cleaning up schedules for index_meta {index_meta_id}: {clean_result.get('error')}")
            return clean_result, code

        return {"ok": True}, 200

    @auth.decorators.check_api({
        "permissions": ["models.applications.index_meta.edit"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": False},
        }})
    @api_tools.endpoint_metrics
    def patch(self, project_id: int, toolkit_id: int, index_meta_id: str):
        payload = dict(request.json)

        try:
            update_data = UpdateIndexingSchedule.parse_obj(payload)
        except ValidationError as e:
            log.error(f"Validation error on index schedule update: {e.errors()}")
            return {"ok": False, "error": f"Validation error on index schedule update: {e.errors()}"}, 400

        try:
            with db.get_session(project_id) as session:
                toolkit = session.query(EliteATool).filter(
                    EliteATool.id == toolkit_id
                ).first()
                if not toolkit:
                    return {"ok": False, "error": "Toolkit not found"}, 404

                private_configuration = toolkit.settings['pgvector_configuration']['private']

                if update_data.user_id == -1 and private_configuration:
                    update_data.user_id = auth.current_user().get("id")

                meta = toolkit.meta or {}
                indexes_meta = meta.get("indexes_meta", {})

                # Structure: indexes_meta[index_meta_id]["schedules"][user_id] = {cron, enabled}
                index_entry = indexes_meta.get(index_meta_id, {})
                if "schedules" not in index_entry:
                    index_entry["schedules"] = {}

                current_user_id = auth.current_user().get("id")

                # Build and validate toolkit schedule model separately so field errors (e.g. cron) surface as 400
                schedule_model = ToolkitIndexingSchedule(
                    cron=update_data.cron,
                    enabled=update_data.enabled,
                    credentials=update_data.credentials,
                    created_by=current_user_id,
                    timezone=update_data.timezone,
                    last_run=datetime.now(UTC),
                )

                # Update or add user-specific scheduling using validated data
                index_entry["schedules"][update_data.user_id] = schedule_model.dict()
                indexes_meta[index_meta_id] = index_entry
                toolkit.meta["indexes_meta"] = indexes_meta

                flag_modified(toolkit, "meta")
                session.commit()

                return serialize(indexes_meta), 200
        except Exception as e:
            session.rollback()
            log.error(f"Error occurred while updating index_meta {index_meta_id}: {e}")
            return {"ok": False, "error": "Error occurred while updating index_meta"}, 400


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>',
        '<int:project_id>/<int:toolkit_id>/<string:index_meta_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
