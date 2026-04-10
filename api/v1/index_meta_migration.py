import json
import re
import time
import traceback
import uuid

from flask import request
from pylon.core.tools import log
from sqlalchemy import Boolean, cast, create_engine, inspect, Integer, Numeric, nullslast, String, Table, text, \
    MetaData, select, func, insert
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Mapped, mapped_column, Session

from tools import api_tools, auth, config as c, serialize, db, context
from ...models.indexer import EmbeddingStore
from ...utils.application_tools import get_session_for_schema, toolkits_listing


class Configuration(db.Base):
    __tablename__ = "configuration"
    __table_args__ = ({"schema": c.POSTGRES_TENANT_SCHEMA, "extend_existing": True})
    project_id: Mapped[int] = mapped_column(Integer, nullable=False)
    elitea_title: Mapped[str] = mapped_column(String, nullable=False, unique=True)


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.applications.migration_index_meta.create"],
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
                            self._verify_and_fix_index_meta(conn_str, sanitize_conn_str, pid)
                        except Exception as e:
                            log.exception(f"Error processing connection string '{sanitize_conn_str}': {e}")
                            # return 400 error with error message and stack trace
                            return {
                                "error": f"Error processing connection string '{sanitize_conn_str}':\n{e}",
                                "stacktrace": traceback.format_exc()
                            }, 400
                        verified.append(conn_str)
                log.info(f"Migration completed for {len(verified)} unique connection strings.")
        #
        return "migration completed", 200

    def _verify_and_fix_index_meta(self, conn_str, sanitize_conn_str, project_id):
        log.info(f"Verifying and fixing index_meta for connection: {sanitize_conn_str}")
        engine = create_engine(conn_str)
        inspector = inspect(engine)
        schemas = inspector.get_schema_names()
        log.info(f"Schemas found: {schemas}")
        toolkits = toolkits_listing(project_id=project_id, query=None, limit=None)["rows"]

        for schema in schemas:
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
            existing_collections = []
            #
            with get_session_for_schema(conn_str, schema) as session:
                collections = session.query(
                    func.distinct(func.jsonb_extract_path_text(EmbeddingStore.cmetadata, 'collection'))
                ).all()
                log.info(f"Collections found in schema '{schema}': {collections}")

                for collection in collections:
                    if not collection or not collection[0]:
                        log.info(f"Skipping empty collection entry in schema '{schema}'.")
                        continue
                    collection = collection[0]
                    exists = session.query(
                        EmbeddingStore.cmetadata
                    ).filter(
                        EmbeddingStore.cmetadata['type'].astext == 'index_meta',
                        EmbeddingStore.cmetadata['collection'].astext == collection
                    ).first()
                    existing_collections.append(collection)
                    if exists:
                        log.info(f"index_meta already exists for collection '{collection}' in schema '{schema}'.")
                    else:
                        indexed = session.query(func.count()).filter(
                            EmbeddingStore.cmetadata['collection'].astext == collection
                        ).scalar()
                        created_on = time.time()
                        meta = {
                            "type": "index_meta",
                            "collection": collection,
                            "created_on": created_on,
                            "updated_on": created_on,
                            "indexed": indexed,
                            "state": "completed",
                            "history": "[]",
                            "index_configuration": f"{{\"collection_suffix\": \"{collection}\"}}"
                        }
                        ins = insert(EmbeddingStore).values(
                            id=str(uuid.uuid4()),
                            cmetadata=meta,
                            document=f"index_meta_{collection}"
                        )
                        log.info(f"Creating index_meta record for collection '{collection}' in schema '{schema}': {meta}")
                        session.execute(ins)
                    session.commit()
            #
            if existing_collections:
                # 1. if there are existing_collections, check and repair index_configuration
                with get_session_for_schema(conn_str, schema) as session:
                    # find all EmbeddingStore entries which has 'collection_suffix' key in dict cmetadata['index_configuration']
                    # replace key 'collection_suffix' with 'index_name' in dict cmetadata['index_configuration']
                    entries = session.query(EmbeddingStore).filter(
                        EmbeddingStore.cmetadata['type'].astext == 'index_meta'
                    ).all()
                    log.info(f"Found {len(entries)} index_meta entries to update in schema '{schema}'.")
                    for entry in entries:
                        index_config_raw = entry.cmetadata.get('index_configuration', '{}')
                        if isinstance(index_config_raw, dict):
                            index_config = index_config_raw
                        else:
                            try:
                                index_config = json.loads(index_config_raw)
                            except Exception as e:
                                log.exception(
                                    f"JSON decode error for index_configuration in index_meta id '{entry.id}' in schema '{schema}'. Raw value: {index_config_raw}. Set empty configuration.")
                                index_config = {}
                        #
                        if 'collection_suffix' in index_config:
                            collection_suffix = index_config.pop('collection_suffix')
                            index_config['index_name'] = collection_suffix
                            entry.cmetadata['index_configuration'] = json.dumps(index_config)
                            log.info(f"Updating index_meta id '{entry.id}' in schema '{schema}': set 'index_name' to '{collection_suffix}'")
                    session.commit()
                #
                # 2. Migrate to schema by toolkit_id
                skipped = []
                for toolkit in toolkits:
                    tk_id = str(toolkit.get("id"))
                    tk_name = toolkit.get("toolkit_name")
                    #
                    if schema == tk_name and tk_id not in schemas:
                        # there is no already existing schema with toolkit_id, so we should rename
                        log.info(f"MIGRATE TK_NAME {tk_name} SCHEMA TO TK_ID {tk_id} FOR PROJECT_ID {project_id}")
                        self._copy_and_remove_schema(engine, tk_name, tk_id)
                    else:
                        skipped.append(f"{tk_name}__{tk_id}")
                log.info(f"SKIP MIGRATION {skipped}")
        #
        log.info(f"Completed verification and fix for connection: {sanitize_conn_str}")

    def _copy_and_remove_schema(self, engine, source_schema, target_schema):
        with Session(engine) as session:
            # 1. Create the new schema
            log.info(f"Create new schema: {target_schema}")
            session.execute(text(f'CREATE SCHEMA "{target_schema}"'))
            # 2. Copy tables and data
            tables = session.execute(text(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema='{source_schema}'"
            )).fetchall()
            log.info(f"Copy tables from {source_schema} to {target_schema}")
            for (table_name,) in tables:
                # Copy table structure
                session.execute(text(
                    f'CREATE TABLE "{target_schema}"."{table_name}" (LIKE "{source_schema}"."{table_name}" INCLUDING ALL)'
                ))
                # Copy data
                session.execute(text(
                    f'INSERT INTO "{target_schema}"."{table_name}" SELECT * FROM "{source_schema}"."{table_name}"'
                ))
            # 3. Drop the original schema
            # log.info(f"Drop original schema: {source_schema}")
            # session.execute(text(f'DROP SCHEMA "{source_schema}" CASCADE'))
            #
            session.commit()


class API(api_tools.APIBase):
    module_name_override = "applications"

    url_params = api_tools.with_modes([''])

    mode_handlers = {
        c.ADMINISTRATION_MODE: PromptLibAPI
    }
