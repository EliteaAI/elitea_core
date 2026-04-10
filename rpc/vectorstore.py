import asyncio
import secrets
import string
import json
from collections import defaultdict
from typing import Optional
from urllib.parse import urlparse

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from pydantic import SecretStr
from pylon.core.tools import web, log
from tools import VaultClient, config as c, this

from ..utils.utils import get_public_project_id

VAULT_PGVECTOR_PASSWORD_KEY = 'pgvector_project_password'
VAULT_PGVECTOR_CONNSTR_KEY = 'pgvector_project_connstr'


def parse_postgres_connection_string(conn_str: str) -> dict:
    """
    Parse PostgreSQL connection string into dictionary of parameters.

    Args:
        conn_str: Connection string in format postgresql://user:password@host:port/dbname

    Returns:
        Dictionary with connection parameters
    """
    parsed = urlparse(conn_str)
    return {
        'user': parsed.username,
        'password': parsed.password,
        'host': parsed.hostname,
        'port': parsed.port,
        'database': parsed.path.lstrip('/')
    }


def create_project_pgvector_schema(project_id, db_params, schema_name):
    result = {
        "status": "created",
        "password": db_params["password"],
        "user": db_params["user"],
    }
    #
    cs_user = db_params["user"]
    cs_pass = db_params["password"]
    cs_host = db_params["host"]
    cs_port = db_params["port"]
    cs_db = db_params["database"]
    cs_schema = schema_name
    #
    # pylint: disable=C0301
    result["connection_string"] = \
        f'postgresql+psycopg://{cs_user}:{cs_pass}@{cs_host}:{cs_port}/{cs_db}?options=-csearch_path%3D{cs_schema},public'
    #
    try:
        conn = psycopg2.connect(**db_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        #
        cur = conn.cursor()
        #
        cur.execute(f"SELECT 1 FROM pg_namespace WHERE nspname = '{schema_name}'")
        if not cur.fetchone():
            log.info("Creating schema: %s", schema_name)
            cur.execute(f"CREATE SCHEMA {schema_name}")
    #
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(type(e))
        #
        log.exception("Failed to create project pgvector schema: %s -> %s", project_id, schema_name)
        raise
    #
    finally:
        if 'cur' in locals():
            cur.close()
        #
        if 'conn' in locals():
            conn.close()
    #
    return result


def create_project_pgvector_password(
        project_id: int,
        db_params: dict,
        pgvector_db: str,
        pgvector_user: str,
        pgvector_password: Optional[str] = None,

) -> dict[str, str]:
    use_schema_pgvector_mode = this.descriptor.config.get("use_schema_pgvector_mode", False)

    if use_schema_pgvector_mode:
        return create_project_pgvector_schema(project_id, db_params, pgvector_db)

    use_existing_pgvector_user = this.descriptor.config.get("use_existing_pgvector_user", False)

    cs_user = db_params["user"]

    log.info(f'Creating pgvector creds: {project_id=} {pgvector_user=} {pgvector_db=}')

    alphabet = string.ascii_letters + string.digits
    result = {'status': 'created with existing password', 'password': pgvector_password}
    if pgvector_password is None:
        pgvector_password = ''.join(secrets.choice(alphabet) for i in range(20))
        result = {'status': 'created with new password', 'password': pgvector_password}

    try:
        conn = psycopg2.connect(**db_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Create user if not exists
        cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname = '{pgvector_user}'")
        if not cur.fetchone():
            cur.execute(f"CREATE USER {pgvector_user} WITH PASSWORD '{pgvector_password}'")
        else:
            # Update password for existing user
            result['status'] = 'password reset'
            cur.execute(f"ALTER USER {pgvector_user} WITH PASSWORD '{pgvector_password}'")

        # Create database if not exists
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{pgvector_db}'")
        if not cur.fetchone():
            cur.execute(f"CREATE DATABASE {pgvector_db}")

        # Grant privileges
        cur.execute(f"GRANT ALL PRIVILEGES ON DATABASE {pgvector_db} TO {pgvector_user}")

        if use_existing_pgvector_user:
            cur.execute(f'GRANT ALL PRIVILEGES ON DATABASE {pgvector_db} TO "{cs_user}"')

        # Connect to the new database to grant schema privileges
        conn.close()
        db_params['database'] = pgvector_db
        conn = psycopg2.connect(**db_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Grant schema privileges
        cur.execute(f"GRANT ALL ON SCHEMA public TO {pgvector_user}")
        cur.execute(f"GRANT ALL ON ALL TABLES IN SCHEMA public TO {pgvector_user}")
        cur.execute(f"GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO {pgvector_user}")

        if use_existing_pgvector_user:
            cur.execute(f'GRANT ALL ON SCHEMA public TO "{cs_user}"')
            cur.execute(f'GRANT ALL ON ALL TABLES IN SCHEMA public TO "{cs_user}"')
            cur.execute(f'GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO "{cs_user}"')

        # Create extension if not exists
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")

        log.info(f'User {pgvector_user} and database {pgvector_db} created successfully')
        # vc.set_secrets(project_secrets)

    except Exception as e:
        result['status'] = 'error'
        result['error'] = str(type(e))
        log.exception('cur')
        raise
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

    return result


def get_or_create_project_pgvector_connstr(
        project_id: int,
        pgvector_connection_string: str,
        save_connstr_to_secrets: bool = False,
        force_recreate: bool = False
) -> dict:
    vc = VaultClient(project_id)

    project_secrets: dict = vc.get_secrets()

    result = {'status': 'built from password'}

    pgvector_user = f'project_{project_id}_user'
    pgvector_db = f'project_{project_id}'

    # Get database connection parameters
    try:
        db_params = json.loads(pgvector_connection_string)
    except:
        db_params = parse_postgres_connection_string(pgvector_connection_string)

    result['secret'] = '{{secret.%s}}' % VAULT_PGVECTOR_PASSWORD_KEY
    result['connection_string_secret'] = '{{secret.%s}}' % VAULT_PGVECTOR_CONNSTR_KEY

    try:
        if force_recreate:
            raise KeyError('Force recreate is set')

        result['connection_string'] = project_secrets[VAULT_PGVECTOR_CONNSTR_KEY]
        result['password'] = project_secrets[VAULT_PGVECTOR_PASSWORD_KEY]
        result['status'] = 'got from secrets'
    except KeyError:
        try:
            if force_recreate:
                raise KeyError('Force recreate is set')

            pgvector_password = project_secrets[VAULT_PGVECTOR_PASSWORD_KEY]

            if pgvector_password:
                password_results: dict = create_project_pgvector_password(
                    project_id=project_id,
                    db_params=db_params,
                    pgvector_user=pgvector_user,
                    pgvector_db=pgvector_db,
                    pgvector_password=pgvector_password
                )
                result.update(password_results)

                new_password = password_results['password']
                if pgvector_password != new_password:
                    project_secrets[VAULT_PGVECTOR_PASSWORD_KEY] = new_password
                    vc.set_secrets(project_secrets)
                    pgvector_password = new_password
        except KeyError:
            log.info(f'Creating pgvector creds: {project_id=} {pgvector_user=} {pgvector_db=}')
            password_results: dict = create_project_pgvector_password(
                project_id=project_id,
                db_params=db_params,
                pgvector_user=pgvector_user,
                pgvector_db=pgvector_db,
            )
            result.update(password_results)
            pgvector_password = password_results['password']
            project_secrets[VAULT_PGVECTOR_PASSWORD_KEY] = pgvector_password
            vc.set_secrets(project_secrets)

        use_existing_pgvector_user = this.descriptor.config.get("use_existing_pgvector_user", False)

        if "connection_string" not in result:
            if use_existing_pgvector_user:
                db_user = db_params["user"]
                db_pass = db_params["password"]
            else:
                db_user = result.get("user", pgvector_user)
                db_pass = pgvector_password
            #
            result['connection_string'] = \
                f'postgresql+psycopg://{db_user}:{db_pass}@{db_params["host"]}:{db_params["port"]}/{pgvector_db}'

        if save_connstr_to_secrets:
            project_secrets[VAULT_PGVECTOR_CONNSTR_KEY] = result['connection_string']
            vc.set_secrets(project_secrets)

        result.update(parse_postgres_connection_string(result['connection_string']))

    return result


async def get_or_create_project_pgvector_connstr_async(*args, **kwargs):
    return get_or_create_project_pgvector_connstr(*args, **kwargs)


# get_public_project_id() is now imported from utils.utils with Redis caching


class RPC:
    @web.rpc('applications_create_pgvector_credentials', 'create_pgvector_credentials')
    def create_pgvector_credentials(self,
                                    project_ids: list[int] | None = None,
                                    save_connstr_to_secrets: bool = True,
                                    concurrent_tasks: int = 20,
                                    public_pgvector_title: str = 'elitea-pgvector',
                                    force_recreate: bool = False,
                                    runtime_mode: str = 'sync',
                                    **kwargs) -> dict:

        public_project_id = get_public_project_id()
        if project_ids is None:
            project_ids: list[int] = [
                i['id'] for i in self.context.rpc_manager.timeout(20).project_list(
                    filter_={'create_success': True}
                )
                if i['id'] != public_project_id
            ]

        source_pgvector: dict = self.context.rpc_manager.timeout(3).configurations_get_first_filtered_project(
            project_id=public_project_id,
            filter_fields={'section': 'vectorstorage', 'elitea_title': public_pgvector_title}
        )

        assert source_pgvector is not None, f'No pgvector[elitea_title={public_pgvector_title}] integrations found'
        pgvector_connection_string: str | SecretStr = source_pgvector['data']['connection_string']

        if isinstance(pgvector_connection_string, SecretStr):
            pgvector_connection_string = pgvector_connection_string.get_secret_value()

        try:
            pgvector_connection_string = VaultClient(public_project_id).unsecret(pgvector_connection_string)
        except AttributeError:
            ...

        result = defaultdict(dict)

        if runtime_mode == "sync":
            log.info("Running in sync mode")
            #
            def process_project(project_id):
                try:
                    project_result = get_or_create_project_pgvector_connstr(
                        project_id=project_id,
                        pgvector_connection_string=pgvector_connection_string,
                        save_connstr_to_secrets=save_connstr_to_secrets,
                        force_recreate=force_recreate
                    )

                    connection_string_secret = project_result.get('connection_string_secret')
                    conf_details, created = self.context.rpc_manager.timeout(3).configurations_create_if_not_exists(
                        payload={
                            'elitea_title': public_pgvector_title,
                            'label': source_pgvector['label'],
                            'project_id': project_id,
                            'type': 'pgvector',
                            'source': 'system',
                            'section': 'vectorstorage',
                            'data': {
                                'connection_string': connection_string_secret,
                            },
                        }
                    )
                    if not created:
                        self.context.rpc_manager.timeout(3).configurations_update(
                            project_id=project_id,
                            config_id=conf_details['id'],
                            payload={
                                'data': {
                                    'connection_string': connection_string_secret,
                                },
                            }
                        )
                    project_result['configuration'] = conf_details
                    project_result['configuration_existed'] = not created

                    result[project_id] = project_result
                except Exception as e:
                    result[project_id]['status'] = 'error'
                    result[project_id]['message'] = str(e)
                    log.exception('cur')
            #
            for p in project_ids:
                log.info("Processing project: %s", p)
                process_project(p)
        else:
            log.info("Running in async mode")
            result_lock = asyncio.Lock()

            async def process_project(project_id: int, semaphore: asyncio.Semaphore) -> None:
                async with semaphore:
                    try:
                        project_result = await get_or_create_project_pgvector_connstr_async(
                            project_id=project_id,
                            pgvector_connection_string=pgvector_connection_string,
                            save_connstr_to_secrets=save_connstr_to_secrets,
                            force_recreate=force_recreate
                        )

                        connection_string_secret = project_result.get('connection_string_secret')
                        conf_details, created = self.context.rpc_manager.timeout(3).configurations_create_if_not_exists(
                            payload={
                                'elitea_title': public_pgvector_title,
                                'label': source_pgvector['label'],
                                'project_id': project_id,
                                'type': 'pgvector',
                                'source': 'system',
                                'section': 'vectorstorage',
                                'data': {
                                    'connection_string': connection_string_secret,
                                },
                            }
                        )
                        if not created:
                            self.context.rpc_manager.timeout(3).configurations_update(
                                project_id=project_id,
                                config_id=conf_details['id'],
                                payload={
                                    'data': {
                                        'connection_string': connection_string_secret,
                                    },
                                }
                            )
                        project_result['configuration'] = conf_details
                        project_result['configuration_existed'] = not created

                        async with result_lock:
                            result[project_id] = project_result
                    except Exception as e:
                        async with result_lock:
                            result[project_id]['status'] = 'error'
                            result[project_id]['message'] = str(e)
                        log.exception('cur')

            async def process_all_projects(concurrent_tasks: int = 20):
                # Limit concurrent tasks
                semaphore = asyncio.Semaphore(concurrent_tasks)
                tasks = [process_project(p, semaphore) for p in project_ids]
                await asyncio.gather(*tasks)

            # Run the async tasks
            asyncio.run(process_all_projects(concurrent_tasks))

        return result
