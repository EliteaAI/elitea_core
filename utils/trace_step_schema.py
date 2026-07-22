"""Compatibility guard for the normalized per-tenant trace-step table."""

from sqlalchemy import text

from pylon.core.tools import log


_LOCK_NAME = 'elitea_core_trace_step_schema_v1'
_TABLE_NAME = 'chat_message_trace_step'


def _tenant_schemas(connection):
    return connection.execute(text("""
        SELECT nspname
        FROM pg_namespace
        WHERE nspname ~ '^p_[0-9]+$'
        ORDER BY nspname
    """)).scalars().all()


def _columns(connection, schema):
    return set(connection.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table_name
    """), {'schema': schema, 'table_name': _TABLE_NAME}).scalars())


def _indexes(connection, schema):
    return set(connection.execute(text("""
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = :schema AND tablename = :table_name
    """), {'schema': schema, 'table_name': _TABLE_NAME}).scalars())


def ensure_trace_step_schema(engine):
    """Expand existing trace tables; new projects already use current ORM metadata.

    Multiple Core replicas can start together, so one transaction-scoped advisory
    lock serializes the DDL. Healthy schemas take only catalog reads and no locks on
    the trace table itself.
    """
    changed_schemas = []
    with engine.begin() as connection:
        connection.execute(
            text('SELECT pg_advisory_xact_lock(hashtext(:name))'),
            {'name': _LOCK_NAME},
        )
        quote = connection.dialect.identifier_preparer.quote

        for schema in _tenant_schemas(connection):
            columns = _columns(connection, schema)
            if not columns:
                continue

            qualified_name = f'{quote(schema)}.{quote(_TABLE_NAME)}'
            added_visibility = 'has_visible_content' not in columns
            required_columns = {
                'parent_agent_call_id': 'TEXT',
                'attrs': 'JSONB',
                'has_visible_content': 'BOOLEAN NOT NULL DEFAULT TRUE',
            }
            statements = [
                f'ADD COLUMN IF NOT EXISTS {quote(name)} {definition}'
                for name, definition in required_columns.items()
                if name not in columns
            ]
            if 'seq' in columns:
                statements.append(f'DROP COLUMN IF EXISTS {quote("seq")}')
            if statements:
                connection.execute(text(
                    f'ALTER TABLE {qualified_name} ' + ', '.join(statements)
                ))

            if added_visibility:
                connection.execute(text(f"""
                    UPDATE {qualified_name}
                    SET has_visible_content = CASE
                        WHEN kind <> 'thinking_step' THEN TRUE
                        WHEN NULLIF(BTRIM(text), '') IS NOT NULL THEN TRUE
                        WHEN NULLIF(BTRIM(thinking), '') IS NOT NULL THEN TRUE
                        WHEN NULLIF(BTRIM(parent_agent_name), '') IS NOT NULL THEN TRUE
                        ELSE FALSE
                    END
                    WHERE kind = 'thinking_step'
                """))

            indexes = _indexes(connection, schema)
            obsolete_indexes = {
                'ix_chat_message_trace_step_group_seq',
                'ix_chat_message_trace_step_run_id',
            }
            for index_name in obsolete_indexes & indexes:
                connection.execute(text(
                    f'DROP INDEX {quote(schema)}.{quote(index_name)}'
                ))

            required_indexes = {
                'ix_chat_message_trace_step_group_started': '(message_group_id, started_at)',
                'ix_chat_message_trace_step_group_kind': '(message_group_id, kind)',
            }
            missing_indexes = required_indexes.keys() - indexes
            for index_name, fields in required_indexes.items():
                if index_name in missing_indexes:
                    connection.execute(text(
                        f'CREATE INDEX {quote(index_name)} ON {qualified_name} {fields}'
                    ))

            if statements or added_visibility or obsolete_indexes & indexes or missing_indexes:
                changed_schemas.append(schema)

    if changed_schemas:
        log.info('Expanded trace-step schema for tenants: %s', ', '.join(changed_schemas))
    else:
        log.info('Trace-step tenant schema is current')
