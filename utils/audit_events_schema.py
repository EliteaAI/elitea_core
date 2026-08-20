"""Compatibility guard for the shared audit_events analytics table.

audit_events lives in one shared schema (POSTGRES_SCHEMA, e.g. 'centry'), not
per-tenant like the trace-step table, so this is a single catalog check plus
ALTER TABLE rather than a per-project loop.
"""

from sqlalchemy import text

from pylon.core.tools import log

from tools import config as c


_LOCK_NAME = 'elitea_core_audit_events_schema_v1'
_TABLE_NAME = 'audit_events'

_REQUIRED_COLUMNS = {
    'input_tokens': 'INTEGER',
    'output_tokens': 'INTEGER',
    'cache_read_tokens': 'INTEGER',
    'cache_creation_tokens': 'INTEGER',
    'llm_cost': 'NUMERIC(18, 8)',
    # Provenance for the token/cost values written by the tracing plugin.
    # token_source ∈ {'langfuse', 'audit', NULL}; cost_source ∈ {'observed',
    # 'estimated:litellm-<version>', NULL}. See tracing/utils/PRICING.md.
    # cost_source is sized generously (64) so future LiteLLM tag variants
    # like 'estimated:litellm-v1.100.0-stable.patch.2' (41+ chars) don't
    # overflow silently — the write path swallows StringDataRightTruncation.
    'token_source': 'VARCHAR(16)',
    'cost_source': 'VARCHAR(64)',
}
_REQUIRED_INDEXES = {
    'ix_audit_events_model_name': '(model_name)',
}


def _columns(connection, schema):
    """Return {column_name: character_maximum_length_or_None} for the audit table."""
    rows = connection.execute(text("""
        SELECT column_name, character_maximum_length
        FROM information_schema.columns
        WHERE table_schema = :schema AND table_name = :table_name
    """), {'schema': schema, 'table_name': _TABLE_NAME}).all()
    return {name: length for name, length in rows}


def _indexes(connection, schema):
    return set(connection.execute(text("""
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = :schema AND tablename = :table_name
    """), {'schema': schema, 'table_name': _TABLE_NAME}).scalars())


def _required_varchar_width(coltype):
    """Extract the VARCHAR(N) width, or None if the column type is not VARCHAR."""
    if not coltype.upper().startswith('VARCHAR('):
        return None
    try:
        return int(coltype[8:-1])
    except (ValueError, IndexError):
        return None


def ensure_audit_events_schema(engine, dry_run=False):
    """Expand an existing audit_events table; new deploys already use current ORM metadata.

    Multiple Core replicas can start together, so one transaction-scoped advisory
    lock serializes the DDL. A healthy schema takes only catalog reads and no lock
    on the audit_events table itself.

    With ``dry_run=True``, reports what would change without applying it (and
    without taking the advisory lock, since nothing is written).

    Returns a dict: ``{"table_present": bool, "added_columns": [...], "added_indexes": [...]}``.
    """
    schema = c.POSTGRES_SCHEMA

    if dry_run:
        with engine.connect() as connection:
            columns = _columns(connection, schema)
            if not columns:
                return {
                    "table_present": False,
                    "added_columns": [],
                    "widened_columns": [],
                    "added_indexes": [],
                }
            indexes = _indexes(connection, schema)
            return {
                "table_present": True,
                "added_columns": [name for name in _REQUIRED_COLUMNS if name not in columns],
                "widened_columns": _columns_needing_widen(columns),
                "added_indexes": sorted(_REQUIRED_INDEXES.keys() - indexes),
            }

    with engine.begin() as connection:
        connection.execute(
            text('SELECT pg_advisory_xact_lock(hashtext(:name))'),
            {'name': _LOCK_NAME},
        )
        quote = connection.dialect.identifier_preparer.quote

        columns = _columns(connection, schema)
        if not columns:
            log.info('audit_events schema: table not present yet, skipping (create_all will provision it)')
            return {
                "table_present": False,
                "added_columns": [],
                "widened_columns": [],
                "added_indexes": [],
            }

        qualified_name = f'{quote(schema)}.{quote(_TABLE_NAME)}'
        added_columns = [name for name in _REQUIRED_COLUMNS if name not in columns]
        statements = [
            f'ADD COLUMN IF NOT EXISTS {quote(name)} {_REQUIRED_COLUMNS[name]}'
            for name in added_columns
        ]
        if statements:
            connection.execute(text(
                f'ALTER TABLE {qualified_name} ' + ', '.join(statements)
            ))

        # Widen existing VARCHAR columns that are narrower than required.
        # ADD COLUMN IF NOT EXISTS above will not touch a column that already
        # exists, so any env that ran an earlier version of this same PR (or
        # any other codepath that installed the column at a smaller width)
        # would silently keep the narrower type and truncate writes.
        widened = _columns_needing_widen(columns)
        for name, target_width in widened:
            connection.execute(text(
                f'ALTER TABLE {qualified_name} '
                f'ALTER COLUMN {quote(name)} TYPE VARCHAR({target_width})'
            ))

        indexes = _indexes(connection, schema)
        missing_indexes = sorted(_REQUIRED_INDEXES.keys() - indexes)
        for index_name in missing_indexes:
            connection.execute(text(
                f'CREATE INDEX {quote(index_name)} ON {qualified_name} {_REQUIRED_INDEXES[index_name]}'
            ))

        if statements or widened or missing_indexes:
            log.info(
                'audit_events schema: added columns=%s widened=%s indexes=%s',
                added_columns, widened, missing_indexes,
            )
        else:
            log.info('audit_events schema is current')

        return {
            "table_present": True,
            "added_columns": added_columns,
            "widened_columns": widened,
            "added_indexes": missing_indexes,
        }


def _columns_needing_widen(existing_columns):
    """Return [(name, target_width), ...] for VARCHAR columns narrower than required."""
    widened = []
    for name, coltype in _REQUIRED_COLUMNS.items():
        target = _required_varchar_width(coltype)
        if target is None:
            continue
        existing_width = existing_columns.get(name)
        if existing_width is not None and existing_width < target:
            widened.append((name, target))
    return widened
