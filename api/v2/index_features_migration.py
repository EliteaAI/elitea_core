import json
import re
import traceback
from copy import deepcopy

from flask import request
from pylon.core.tools import log
from sqlalchemy import Boolean, cast, create_engine, inspect, Integer, Numeric, nullslast, String, Table, text, \
    MetaData, select, func, insert
from sqlalchemy.exc import NoResultFound, OperationalError
from sqlalchemy.orm import Mapped, mapped_column, Session

from tools import api_tools, auth, config as c, serialize, db, context
from ...models.indexer import EmbeddingStore
from ...utils.application_tools import get_session_for_schema


class Configuration(db.Base):
    __tablename__ = "configuration"
    __table_args__ = ({"schema": c.POSTGRES_TENANT_SCHEMA, "extend_existing": True})
    project_id: Mapped[int] = mapped_column(Integer, nullable=False)
    elitea_title: Mapped[str] = mapped_column(String, nullable=False, unique=True)


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.index_features_migration.create"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": False, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": False, "viewer": False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self):
        payload = dict(request.json)
        # if payload contains 'project_id' int value > 0, only process that project_id
        # if 'project_id' value equals 0, process all projects
        # Otherwise, return 400 error
        project_id = payload.get('project_id')
        if project_id is not None:
            try:
                project_id = int(project_id)
                if project_id < 0:
                    return {"error": "Invalid project_id. Must be >= 0."}, 400
            except ValueError:
                return {"error": "Invalid project_id. Must be an integer."}, 400
        #
        def get_all_project_ids():
            return [
                i['id'] for i in self.module.context.rpc_manager.call.project_list(
                    filter_={'create_success': True}
                )
            ]
        #
        project_ids = [project_id] if project_id else get_all_project_ids()
        verified = []
        #
        log.info(f"Verifying and fixing index_meta for projects {project_ids}")
        #
        for pid in project_ids:
            with db.get_session(pid) as session_db:
                pg_configs = session_db.query(Configuration.elitea_title).filter(
                    Configuration.type == "pgvector"
                ).distinct().all()
                log.info(f"Found {len(pg_configs)} pgvector configurations for project_id={pid}")
                for pg_config in pg_configs:
                    config_expanded = context.rpc_manager.timeout(3).configurations_expand(
                        project_id=pid,
                        settings={'private': False, 'elitea_title': pg_config[0]},
                        user_id=None,
                        unsecret=True
                    )
                    conn_str = config_expanded.get("connection_string")
                    #
                    # check conn_str has protocol postgresql+psycopg:// using regex
                    if not conn_str or not re.match(r'^postgresql\+psycopg://', conn_str):
                        log.info(f"Skipping configuration '{pg_config[0]}' for project_id={pid}: invalid or missing connection string.")
                        continue
                    #
                    sanitize_conn_str = re.sub(r'(postgresql\+psycopg://[^:]+:)[^@]+(@)', r'\1***\2', conn_str)
                    log.info(f"Processing connection string: {sanitize_conn_str}")
                    if conn_str and conn_str not in verified:
                        try:
                            self._upd_index_meta_for_new_features(conn_str, sanitize_conn_str, pid)
                        except OperationalError as e:
                            # Database is unreachable or connection refused: log and continue with other connections
                            log.error(
                                f"OperationalError while processing connection string '{sanitize_conn_str}' "
                                f"for project_id={pid}: {e}. Skipping this database."
                            )
                        except Exception as e:
                            log.exception(f"Error processing connection string '{sanitize_conn_str}': {e}")
                            # return 400 error with error message and stack trace
                            return {
                                "error": f"Error processing connection string '{sanitize_conn_str}':\n{e}",
                                "stacktrace": traceback.format_exc()
                            }, 400
                        finally:
                            # Mark this connection string as seen so it won't be retried in the same run
                            verified.append(conn_str)
                log.info(f"Migration completed for {len(verified)} unique connection strings.")
        #
        return "migration completed", 200

    def _migrate_figma_index_configuration(self, entry, schema: str) -> None:
        """Normalize and migrate Figma-related index_configuration for a single index_meta entry.

        - Ensure index_configuration is treated as JSON (string in storage).
        - Build urls_or_file_keys from file_or_page_url + file_keys_include (comma-separated).
        - Only runs when node_ids_exclude is present (Figma-specific config).
        """
        index_config_raw = entry.cmetadata.get("index_configuration")
        if not index_config_raw:
            return
        # Parse index_configuration if stored as JSON string
        if isinstance(index_config_raw, str):
            try:
                index_config = json.loads(index_config_raw)
            except Exception:
                log.exception(
                    f"JSON decode error for index_configuration in index_meta id '{entry.id}' in schema '{schema}'. "
                    f"Raw value: {index_config_raw}. Skipping Figma migration for this entry."
                )
                return
        elif isinstance(index_config_raw, dict):
            index_config = index_config_raw
        else:
            # Unexpected type, skip safely
            log.info(
                f"index_configuration of index_meta id '{entry.id}' in schema '{schema}' has unexpected type "
                f"{type(index_config_raw)}. Skipping Figma migration for this entry."
            )
            return

        # Only Figma-like configs have node_ids_exclude
        if "node_ids_exclude" not in index_config:
            # Keep storage normalized to JSON string even if we don't touch Figma-specific fields
            entry.cmetadata["index_configuration"] = json.dumps(index_config)
            return

        # Build urls_or_file_keys from file_or_page_url + file_keys_include
        file_or_page_url = index_config.pop("file_or_page_url", None)
        file_keys_include = index_config.pop("file_keys_include", [])

        parts = []
        if file_or_page_url:
            parts.append(str(file_or_page_url))
        if isinstance(file_keys_include, list):
            parts.extend([str(v) for v in file_keys_include if v])
        elif isinstance(file_keys_include, str) and file_keys_include:
            parts.append(file_keys_include)

        urls_or_file_keys = ",".join(parts)
        if urls_or_file_keys:
            index_config["urls_or_file_keys"] = urls_or_file_keys

        # Store index_configuration consistently as JSON string
        entry.cmetadata["index_configuration"] = json.dumps(index_config)
        log.info(
            f"Updated Figma index_configuration for index_meta id '{entry.id}' in schema '{schema}': "
            f"urls_or_file_keys='{urls_or_file_keys}'"
        )

    def _upd_index_meta_for_new_features(self, conn_str, sanitize_conn_str, project_id):
        log.info(f"Verifying and fixing index_meta for connection: {sanitize_conn_str}")
        try:
            engine = create_engine(conn_str)
            inspector = inspect(engine)
            schemas = inspector.get_schema_names()
        except OperationalError as e:
            # Connection or introspection failed for this database; let caller decide how to react
            log.error(
                f"OperationalError while connecting to or inspecting database for connection "
                f"'{sanitize_conn_str}': {e}"
            )
            raise

        log.info(f"Schemas found: {schemas}")

        for schema in schemas:
            try:
                toolkit_id = int(schema)
            except ValueError:
                toolkit_id = 0
                log.info(f"Schema '{schema}' is not convertible to int, Set toolkit_id value to 0")
            #
            tables = inspector.get_table_names(schema=schema)
            if "langchain_pg_embedding" not in tables:
                log.info(f"Schema '{schema}' skipped: 'langchain_pg_embedding' table not found.")
                continue
            # verify table contains required columns id, cmetadata, document
            columns = inspector.get_columns("langchain_pg_embedding", schema=schema)
            column_names = [col['name'] for col in columns]
            required_columns = {'id', 'cmetadata', 'document'}
            if not required_columns.issubset(set(column_names)):
                log.info(f"Schema '{schema}' skipped: required columns {required_columns} not found in 'langchain_pg_embedding' table.")
                continue
            #
            log.info(f"Schema '{schema}' will be processed (contains 'langchain_pg_embedding').")
            #
            try:
                with get_session_for_schema(conn_str, schema) as session:
                    # find all EmbeddingStore entries which has 'collection_suffix' key in dict cmetadata['index_configuration']
                    # replace key 'collection_suffix' with 'index_name' in dict cmetadata['index_configuration']
                    entries = session.query(EmbeddingStore).filter(
                        EmbeddingStore.cmetadata['type'].astext == 'index_meta'
                    ).all()
                    log.info(f"Found {len(entries)} index_meta entries to update in schema '{schema}'.")
                    for entry in entries:
                        log.info(f"Checking {entry.id} in schema '{schema}'.")
                        # Migrate Figma index_configuration and normalize to JSON string
                        self._migrate_figma_index_configuration(entry, schema)
                        # Add updated field if not present
                        if 'updated' not in entry.cmetadata:
                            entry.cmetadata['updated'] = 0
                            log.info(f"Added updated=0 to index_meta id '{entry.id}' in schema '{schema}'")
                        # Add toolkit_id field if not present
                        if 'toolkit_id' not in entry.cmetadata:
                            entry.cmetadata['toolkit_id'] = toolkit_id
                            log.info(f"Added toolkit_id={toolkit_id} to index_meta id '{entry.id}' in schema '{schema}'")
                        # init history if not present or empty
                        # add toolkit_id to history items
                        history_raw = entry.cmetadata.get('history', '{}')
                        if isinstance(history_raw, dict):
                            history = history_raw
                        else:
                            try:
                                history = json.loads(history_raw)
                            except Exception as e:
                                log.exception(
                                    f"JSON decode error for history in index_meta id '{entry.id}' in schema '{schema}'. Raw value: {history_raw}. Set empty history.")
                                history = {}
                        #
                        history_updated = False
                        if not history:
                            default = deepcopy(entry.cmetadata)
                            default.pop('history', None)
                            history = [default]
                            history_updated = True
                            log.info(f"Initialized default history with only one item for index_meta id '{entry.id}' in schema '{schema}'")
                        else:
                            for item in history:
                                if 'toolkit_id' not in item:
                                    item['toolkit_id'] = toolkit_id
                                    history_updated = True
                                    log.info(f"Added toolkit_id={toolkit_id} to history of '{entry.id}' in schema '{schema}'")
                        #
                        if history_updated:
                            entry.cmetadata['history'] = json.dumps(history)
                            log.info(f"Updated history for index_meta id '{entry.id}' in schema '{schema}'")
                    session.commit()
            except OperationalError as e:
                # If a particular schema cannot be accessed or queried, log and continue with other schemas
                log.error(
                    f"OperationalError while processing schema '{schema}' for connection '{sanitize_conn_str}': {e}. "
                    f"Skipping this schema."
                )
                continue
        #
        log.info(f"Completed verification and fix for connection: {sanitize_conn_str}")


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([''])

    mode_handlers = {
        c.ADMINISTRATION_MODE: PromptLibAPI
    }
