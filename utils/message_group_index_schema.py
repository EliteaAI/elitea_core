from sqlalchemy import text
from pylon.core.tools import log
from tools import db

from ..models import CONVERSATION_MESSAGE_GROUP_TABLE_NAME, MESSAGE_GROUP_AUTHOR_INDEX_NAME

INDEX_BUILD_LOCK_NAME = 'elitea_core_message_group_author_index'

_INDEX_STATES = text(
    "SELECT t.table_schema, x.indisvalid "
    "FROM information_schema.tables t "
    "LEFT JOIN pg_namespace n ON n.nspname = t.table_schema "
    "LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = :index "
    "LEFT JOIN pg_index x ON x.indexrelid = c.oid "
    "WHERE t.table_name = :table AND t.table_schema ~ '^p_[0-9]+$' "
    "AND x.indisvalid IS NOT TRUE"
)
_CREATE_INDEX = (
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS {index} "
    "ON p_{pid}.{table} (author_participant_id, conversation_id, created_at)"
)
_DROP_INVALID_INDEX = "DROP INDEX CONCURRENTLY IF EXISTS p_{pid}.{index}"


def schemas_without_valid_author_index(connection) -> dict:
    rows = connection.execute(
        _INDEX_STATES,
        {'table': CONVERSATION_MESSAGE_GROUP_TABLE_NAME, 'index': MESSAGE_GROUP_AUTHOR_INDEX_NAME},
    ).fetchall()
    return {int(schema[2:]): is_valid for schema, is_valid in rows}


def build_author_index(connection, pid: int, is_valid) -> None:
    names = {'pid': pid, 'index': MESSAGE_GROUP_AUTHOR_INDEX_NAME, 'table': CONVERSATION_MESSAGE_GROUP_TABLE_NAME}
    if is_valid is False:
        connection.execute(text(_DROP_INVALID_INDEX.format(**names)))
    connection.execute(text(_CREATE_INDEX.format(**names)))


def apply_author_index():
    migrated, failed = [], []
    with db.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as connection:
        is_builder = connection.execute(
            text('SELECT pg_try_advisory_lock(hashtext(:name))'), {'name': INDEX_BUILD_LOCK_NAME},
        ).scalar()
        if not is_builder:
            return migrated, failed
        try:
            for pid, is_valid in schemas_without_valid_author_index(connection).items():
                try:
                    build_author_index(connection, pid, is_valid)
                    migrated.append(pid)
                except Exception:  # pylint: disable=W0703
                    log.exception("message group author index: failed for project %s", pid)
                    failed.append({"project_id": pid})
        finally:
            connection.execute(
                text('SELECT pg_advisory_unlock(hashtext(:name))'), {'name': INDEX_BUILD_LOCK_NAME},
            )
    return migrated, failed
