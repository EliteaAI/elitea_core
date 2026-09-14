"""Compatibility guard for the shared audit_events analytics table.

audit_events lives in one shared schema (POSTGRES_SCHEMA, e.g. 'centry'), not
per-tenant like the trace-step table, so this is a single catalog check plus
ALTER TABLE rather than a per-project loop.
"""

from sqlalchemy import text

from pylon.core.tools import log

from tools import config as c


_LOCK_NAME = 'elitea_core_audit_events_schema_v1'
_STATS_LOCK_NAME = 'elitea_core_audit_events_statistics_v1'
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
    'ix_audit_events_timestamp': '(timestamp)',
    'ix_audit_events_user_id': '(user_id)',
    'ix_audit_events_project_id': '(project_id)',
    'ix_audit_events_trace_id': '(trace_id)',
    'ix_audit_events_entity': '(entity_type, entity_id)',
    'ix_audit_events_model_name': '(model_name)',
    'ix_audit_events_project_timestamp': '(project_id, timestamp)',
    'ix_audit_events_tool_name': '(tool_name) WHERE tool_name IS NOT NULL',
    'ix_audit_events_is_error': '(is_error) WHERE is_error IS TRUE',
    # Every analytics aggregate scopes by (project_id, event_type, timestamp).
    # Without this leading-column match the planner falls back to the
    # timestamp-only index and discards most of the rows it scans.
    'ix_audit_events_project_event_type_timestamp': '(project_id, event_type, timestamp)',
}

# audit_events is insert-only, so n_dead_tup stays at 0 and the default
# dead-tuple autovacuum threshold never trips — planner statistics are never
# refreshed and analytics queries get costed against a stale row estimate.
# These reloptions make autovacuum analyze on insert volume instead.
_REQUIRED_TABLE_OPTIONS = {
    'autovacuum_analyze_scale_factor': '0.02',
    'autovacuum_vacuum_insert_threshold': '5000',
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


def _table_options(connection, schema):
    """Return {reloption: value} currently set on the audit table."""
    rows = connection.execute(text("""
        SELECT c.reloptions
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = :schema AND c.relname = :table_name
    """), {'schema': schema, 'table_name': _TABLE_NAME}).scalars().all()
    options = {}
    for reloptions in rows:
        for entry in reloptions or []:
            name, _, value = entry.partition('=')
            options[name] = value
    return options


def _options_needing_set(existing_options):
    """Return the required reloptions whose current value differs."""
    return {
        name: value
        for name, value in _REQUIRED_TABLE_OPTIONS.items()
        if existing_options.get(name) != value
    }


def _statistics_missing(connection, schema):
    """True when planner statistics have never been gathered for the audit table."""
    row = connection.execute(text("""
        SELECT last_analyze, last_autoanalyze
        FROM pg_stat_user_tables
        WHERE schemaname = :schema AND relname = :table_name
    """), {'schema': schema, 'table_name': _TABLE_NAME}).first()
    if row is None:
        return True
    return row[0] is None and row[1] is None


def ensure_audit_events_statistics(engine, dry_run=False):
    """Keep planner statistics on the insert-only audit_events table current.

    Applies the autovacuum reloptions that make analyze trigger on insert
    volume, and runs a one-off ``ANALYZE`` when statistics have never been
    gathered at all (which is what leaves analytics queries costed against a
    stale row estimate and picking the wrong index).

    Both steps take only SHARE UPDATE EXCLUSIVE, so concurrent audit writes are
    not blocked. Idempotent: once the reloptions are in place and statistics
    exist, a re-run is catalog reads only.

    Returns ``{"table_present": bool, "set_options": {...}, "analyzed": bool}``.
    """
    schema = c.POSTGRES_SCHEMA

    if dry_run:
        with engine.connect() as connection:
            if not _columns(connection, schema):
                return {"table_present": False, "set_options": {}, "analyzed": False}
            return {
                "table_present": True,
                "set_options": _options_needing_set(_table_options(connection, schema)),
                "analyzed": _statistics_missing(connection, schema),
            }

    with engine.begin() as connection:
        connection.execute(
            text('SELECT pg_advisory_xact_lock(hashtext(:name))'),
            {'name': _STATS_LOCK_NAME},
        )
        quote = connection.dialect.identifier_preparer.quote

        if not _columns(connection, schema):
            log.info('audit_events statistics: table not present yet, skipping')
            return {"table_present": False, "set_options": {}, "analyzed": False}

        qualified_name = f'{quote(schema)}.{quote(_TABLE_NAME)}'

        set_options = _options_needing_set(_table_options(connection, schema))
        if set_options:
            assignments = ', '.join(
                f'{name} = {value}' for name, value in set_options.items()
            )
            connection.execute(text(
                f'ALTER TABLE {qualified_name} SET ({assignments})'
            ))

        analyzed = _statistics_missing(connection, schema)
        if analyzed:
            connection.execute(text(f'ANALYZE {qualified_name}'))

        if set_options or analyzed:
            log.info(
                'audit_events statistics: set_options=%s analyzed=%s',
                set_options, analyzed,
            )
        else:
            log.info('audit_events statistics are current')

        return {
            "table_present": True,
            "set_options": set_options,
            "analyzed": analyzed,
        }


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
