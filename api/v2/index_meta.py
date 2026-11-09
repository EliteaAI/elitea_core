import json

from flask import request
from pylon.core.tools import log
from sqlalchemy import cast, inspect, Numeric, nullslast, select, text
from sqlalchemy.exc import NoResultFound, OperationalError, ProgrammingError

from tools import api_tools, auth, config as c, serialize, db, VaultClient
from ...models.elitea_tools import EliteATool
from ...models.entity_schedule import EntitySchedule
from ...models.indexer import EmbeddingStore, IndexRun
from ...utils.application_tools import (
    INDEX_SCHEDULE_MANAGED_MESSAGE,
    load_and_validate_toolkit_for_index,
    get_session_for_schema,
    get_pending_index_run_heartbeats,
    resolve_index_staleness,
    clean_up_schedule_in_toolkit,
    find_last_successful_run,
    IndexMetaLockTimeoutError,
    INDEX_META_LOCK_TIMEOUT,
    DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC,
    HEARTBEAT_STALE_HORIZON_SEC,
    _get_pgvector_engine,
    _is_undefined_table_error,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.predict_utils import get_toolkit_config
from ...utils.folder_access import require_folder_access, TOOLKIT_ENTITY_TYPES


def _scheduled_index_names(project_id: int, toolkit_id: int) -> set:
    """Names of the indexes of the toolkit that a schedule row or the old settings key already schedules.

    A project whose schema has no schedule columns yet answers from the settings key alone.
    """
    with db.get_session(project_id) as session:
        meta = session.scalar(select(EliteATool.meta).where(EliteATool.id == toolkit_id)) or {}
        names = {
            name for name, entry in (meta.get("indexes_meta") or {}).items()
            if isinstance(entry, dict) and entry.get("schedules")
        }
        try:
            names.update(session.scalars(
                select(EntitySchedule.index_name).where(EntitySchedule.toolkit_id == toolkit_id)
            ).all())
        except Exception as error:  # noqa: BLE001 -- a schema older than the schedule columns
            session.rollback()
            log.warning("Scheduled indexes of toolkit %s read from the settings only: %s", toolkit_id, type(error).__name__)
    return names


def _has_saved_settings(configuration) -> bool:
    """True when the index row holds settings a run can send; the column may hold JSON text or a decoded object."""
    if isinstance(configuration, str):
        try:
            configuration = json.loads(configuration)
        except (TypeError, json.JSONDecodeError):
            return False
    return bool(configuration) and isinstance(configuration, dict)


def _index_summary(project_id: int, toolkit_id: int, connection_string: str, schema: str):
    """The picker list: per index only its name, state, update time, and whether it is scheduled or schedulable.

    Reads three small keys of the row; the full list also carries history and report, which a picker never shows.
    """
    scheduled = _scheduled_index_names(project_id, toolkit_id)
    with get_session_for_schema(connection_string, schema) as session:
        rows = session.query(
            EmbeddingStore.cmetadata['collection'].astext,
            EmbeddingStore.cmetadata['state'].astext,
            EmbeddingStore.cmetadata['updated_on'],
            EmbeddingStore.cmetadata['index_configuration'],
        ).filter(
            EmbeddingStore.cmetadata['type'].astext == 'index_meta'
        ).order_by(
            nullslast(cast(EmbeddingStore.cmetadata['updated_on'].astext, Numeric).desc())
        ).all()
    return [
        {
            "name": name, "state": state, "updated_on": updated_on,
            "scheduled": name in scheduled, "schedulable": _has_saved_settings(configuration),
        }
        for name, state, updated_on, configuration in rows
    ]


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.index_meta.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    @require_folder_access(TOOLKIT_ENTITY_TYPES, 'toolkit_id')
    def get(self, project_id: int, toolkit_id: int, **kwargs):
        toolkit_config = get_toolkit_config(project_id, auth.current_user()['id'], toolkit_id)
        toolkit_name_id, connection_string, validation_error = load_and_validate_toolkit_for_index(toolkit_config)
        if validation_error:
            return validation_error
        #
        engine = _get_pgvector_engine(connection_string)
        inspector = inspect(engine)
        table_exists = inspector.has_table("langchain_pg_embedding", schema=toolkit_name_id)
        if not table_exists:
            log.warning(f"Table {toolkit_name_id}.langchain_pg_embedding does not exist. Probably no data has been indexed yet.")
            return [], 200
        #
        if request.args.get("summary", "false").lower() == "true":
            try:
                return serialize(_index_summary(project_id, toolkit_id, connection_string, toolkit_name_id)), 200
            except Exception as e:
                log.error(f"Error occurred while fetching the index summary: {e}")
                return {"ok": False, "error": "Error occurred while fetching index_meta"}, 400
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
                task_disconnected_timeout = int(secrets.get(
                    'task_disconnected_timeout_sec', DEFAULT_TASK_DISCONNECTED_TIMEOUT_SEC))
                # One query for every row's run heartbeat; per-row would be one
                # query per index on a list endpoint.
                pending_heartbeats = get_pending_index_run_heartbeats(session)
                
                for id, cmetadata in meta:
                    for key in ['index_configuration', 'history', 'report']:
                        # The isinstance guard matters now that the SDK patches keys
                        # instead of routing them through a writer that stringified every
                        # dict: an already-decoded object is not an error.
                        if cmetadata and isinstance(cmetadata.get(key), str) and cmetadata[key]:
                            try:
                                cmetadata[key] = json.loads(cmetadata[key])
                            except (TypeError, json.JSONDecodeError):
                                log.warning(f"Failed to decode {key} for index_meta {id}")
                    # Display-only, never a search or retention gate (those key on
                    # metadata.indexed_chunks). Computed from the RAW history before the
                    # completed→created relabel below, or the flagship "first index
                    # succeeded, first reindex failed" case would report null.
                    history_entries = cmetadata.get('history')
                    last_successful = find_last_successful_run(
                        history_entries if isinstance(history_entries, list) else []
                    )
                    last_successful_run = {
                        "updated_on": last_successful.get('updated_on'),
                        "indexed": last_successful.get('indexed'),
                    } if last_successful else None
                    # highlight the fist history item as 'created' if it is completed successfully
                    if 'history' in cmetadata and len(cmetadata['history']) > 0 and cmetadata['history'][0]['state'] == 'completed':
                        cmetadata['history'][0]['state'] = 'created'
                    #
                    # Determine if task is stale (in_progress but not updated recently)
                    updated_on = cmetadata.get('updated_on', 0)
                    index_data_state = cmetadata.get('state', '')
                    pending_heartbeat = pending_heartbeats.get(cmetadata.get('collection'))
                    # Two flags on purpose: `stale` is chrome and may be wrong for one
                    # poll, `reclaimable` authorizes destructive affordances and stays on
                    # the disconnect rule the dispatch guard uses.
                    stale = resolve_index_staleness(
                        index_data_state, updated_on, task_disconnected_timeout,
                        pending_heartbeat, heartbeat_horizon=HEARTBEAT_STALE_HORIZON_SEC,
                    )
                    reclaimable = resolve_index_staleness(
                        index_data_state, updated_on, task_disconnected_timeout,
                        pending_heartbeat,
                    )
                    #
                    result.append({
                        "id": id,
                        "metadata": cmetadata,
                        "stale": stale,
                        "reclaimable": reclaimable,
                        "last_successful_run": last_successful_run
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
    @require_folder_access(TOOLKIT_ENTITY_TYPES, 'toolkit_id', write=True)
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
                # Universal lock order shared with cancel and the SDK's promote/discard:
                # meta row → run rows → chunk rows. Deleting chunk rows first while a
                # Stop holds the meta and run rows is a textbook AB-BA deadlock, and the
                # promote path can hold the meta lock for minutes — hence the bounded
                # wait surfaced as a retryable error instead of a hang.
                session.execute(text(f"SET LOCAL lock_timeout = '{INDEX_META_LOCK_TIMEOUT}'"))
                obj = session.query(EmbeddingStore).filter(
                    EmbeddingStore.id == index_meta_id
                ).with_for_update().one()
                index_name = obj.cmetadata["collection"]
                # Run rows of every status go with the index: a surviving pending row
                # would refuse re-creating a same-named index for the full staleness
                # timeout. Savepoint-guarded — the runs table may not be provisioned yet.
                savepoint = session.begin_nested()
                try:
                    session.query(IndexRun).filter(
                        IndexRun.collection == index_name
                    ).delete(synchronize_session=False)
                    savepoint.commit()
                except ProgrammingError as error:
                    savepoint.rollback()
                    if not _is_undefined_table_error(error):
                        raise
                session.query(EmbeddingStore).filter(
                    EmbeddingStore.cmetadata["collection"].astext == index_name
                ).delete(synchronize_session=False)
                session.commit()
                log.debug(f"Deleted all index_meta with collection '{index_name}' from toolkit {toolkit_id}")
        except NoResultFound:
            return {"ok": False, "error": f"index_meta {index_meta_id} not found"}, 404
        except OperationalError as e:
            session.rollback()
            log.warning(f"Lock timeout while deleting index_meta {index_meta_id}: {e}")
            return {"ok": False, "error": str(IndexMetaLockTimeoutError(index_name or index_meta_id))}, 409
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
    @require_folder_access(TOOLKIT_ENTITY_TYPES, 'toolkit_id', write=True)
    def patch(self, project_id: int, toolkit_id: int, index_meta_id: str):
        # Index schedules are rows of the Schedules module; writing the old settings key would start a second,
        # unseen schedule for the same index
        return {"ok": False, "error": INDEX_SCHEDULE_MANAGED_MESSAGE}, 400


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>/<int:toolkit_id>',
        '<int:project_id>/<int:toolkit_id>/<string:index_meta_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
