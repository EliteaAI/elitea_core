"""Idempotent schema setup for conversation share tokens.

Creates the share token tables in any project schema (and the public schema) that
predates them.  `create_all` handles new projects automatically; this module handles
pre-existing tenants — run at startup by the safety-net thread in module.py.
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db


# DDL applied to each per-project (p_<pid>) schema
_PROJECT_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS p_{pid}.chat_conversation_share_tokens (
        id               SERIAL PRIMARY KEY,
        token            VARCHAR(64) NOT NULL UNIQUE,
        conversation_id  INTEGER NOT NULL
                             REFERENCES p_{pid}.chat_conversations(id) ON DELETE CASCADE,
        created_by       INTEGER NOT NULL,
        created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
        expires_at       TIMESTAMP,
        password_hash    VARCHAR(256),
        is_revoked       BOOLEAN NOT NULL DEFAULT FALSE,
        access_count     INTEGER NOT NULL DEFAULT 0,
        failed_attempts  INTEGER NOT NULL DEFAULT 0,
        locked_until     TIMESTAMP,
        scope            VARCHAR(32) NOT NULL DEFAULT 'all',
        message_group_ids JSON
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_p_{pid}_chat_conv_share_tokens_token "
    "ON p_{pid}.chat_conversation_share_tokens (token)",
    "CREATE INDEX IF NOT EXISTS ix_p_{pid}_chat_conv_share_tokens_conv "
    "ON p_{pid}.chat_conversation_share_tokens (conversation_id)",
    # Add rate-limit columns to tables created before this migration
    "ALTER TABLE p_{pid}.chat_conversation_share_tokens "
    "ADD COLUMN IF NOT EXISTS failed_attempts INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE p_{pid}.chat_conversation_share_tokens "
    "ADD COLUMN IF NOT EXISTS locked_until TIMESTAMP",
)

# DDL applied to the public schema (once, not per-project)
_PUBLIC_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS public.chat_conversation_share_token_index (
        token           VARCHAR(64) PRIMARY KEY,
        project_id      INTEGER NOT NULL,
        conversation_id INTEGER NOT NULL
    )
    """,
)


def _project_ids_missing_rate_limit_columns():
    """Return project IDs whose share token table exists but lacks the rate-limit columns."""
    query = text(
        "SELECT t.table_schema FROM information_schema.tables t "
        "WHERE t.table_name = 'chat_conversation_share_tokens' "
        "AND t.table_schema ~ '^p_[0-9]+$' "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM information_schema.columns c "
        "  WHERE c.table_schema = t.table_schema "
        "  AND c.table_name = 'chat_conversation_share_tokens' "
        "  AND c.column_name = 'failed_attempts'"
        ")"
    )
    with db.get_session(None) as session:
        rows = session.execute(query).fetchall()
    return [int(row[0][2:]) for row in rows]


def _project_ids_missing_share_token_table():
    """Return project IDs whose schema exists but lacks the share token table."""
    query = text(
        "SELECT t.table_schema FROM information_schema.tables t "
        "WHERE t.table_name = 'chat_conversations' "
        "AND t.table_schema ~ '^p_[0-9]+$' "
        "AND NOT EXISTS ("
        "  SELECT 1 FROM information_schema.tables t2 "
        "  WHERE t2.table_schema = t.table_schema "
        "  AND t2.table_name = 'chat_conversation_share_tokens'"
        ")"
    )
    with db.get_session(None) as session:
        rows = session.execute(query).fetchall()
    return [int(row[0][2:]) for row in rows]


def _public_index_missing():
    query = text(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' "
        "AND table_name = 'chat_conversation_share_token_index'"
    )
    with db.get_session(None) as session:
        row = session.execute(query).fetchone()
    return row is None


def apply_share_token_schema():
    """Provision the share token table in all project schemas that need it,
    and ensure the public index table exists. Returns (migrated, failed) lists."""
    migrated, failed = [], []

    # Provision the public index table first
    if _public_index_missing():
        try:
            with db.get_session(None) as session:
                for stmt in _PUBLIC_STATEMENTS:
                    session.execute(text(stmt))
                session.commit()
            log.info("share token schema: created public index table")
        except Exception:
            log.exception("share token schema: failed to create public index table")

    project_ids = _project_ids_missing_share_token_table()
    for pid in project_ids:
        try:
            with db.with_project_schema_session(pid) as session:
                for stmt in _PROJECT_STATEMENTS:
                    session.execute(text(stmt.format(pid=pid)))
                session.commit()
            migrated.append(pid)
        except Exception:
            log.exception("share token schema: failed to migrate project %s", pid)
            failed.append({"project_id": pid})

    # Add rate-limit columns to tables that predate them
    _RATE_LIMIT_ALTERS = (
        "ALTER TABLE p_{pid}.chat_conversation_share_tokens "
        "ADD COLUMN IF NOT EXISTS failed_attempts INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE p_{pid}.chat_conversation_share_tokens "
        "ADD COLUMN IF NOT EXISTS locked_until TIMESTAMP",
    )
    for pid in _project_ids_missing_rate_limit_columns():
        try:
            with db.with_project_schema_session(pid) as session:
                for stmt in _RATE_LIMIT_ALTERS:
                    session.execute(text(stmt.format(pid=pid)))
                session.commit()
            log.info("share token schema: added rate-limit columns to project %s", pid)
        except Exception:
            log.exception("share token schema: failed to add rate-limit columns to project %s", pid)

    return migrated, failed
