"""Webhook-trigger shape of ``entity_schedules`` in project schemas that predate it (#7398).

New projects get it from ``create_all``; existing ones from the ``ensure_webhook_trigger_columns`` admin task.
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db

TABLE = "entity_schedules"
RUNS = "entity_schedule_runs"
ACTIVE_INDEX = "uq_entity_schedule_runs_active"

COLUMNS = (
    ("trigger_kind", "VARCHAR(16) NOT NULL DEFAULT 'cron'",
     "cron (scheduled) or webhook (started by an HTTP delivery)"),
    ("webhook_type", "VARCHAR(16)", "Webhook rows: github, gitlab or custom"),
    ("gitlab_auth_method", "VARCHAR(16)", "GitLab webhook rows: secret_token or signing_token"),
    ("secret_ref", "TEXT", "Webhook rows: vault reference of the per-row secret (hidden project secrets)"),
    ("signing_secret_ref", "TEXT", "GitLab signing webhook rows: vault reference of the signing token"),
    ("previous_secret_ref", "TEXT", "Webhook rows: vault reference of the rotated-out secret, valid until previous_secret_until"),
    ("previous_secret_until", "TIMESTAMPTZ", "Webhook rows: end of the rotation overlap"),
    ("legacy_version_id", "INTEGER", "Migrated legacy pipeline webhook: the old URL version id"),
    ("legacy_application_id", "INTEGER", "Migrated legacy agent webhook: the old URL application id"),
    ("credential_refs", "JSONB", "configuration_uuids the version uses, written by the Connections check"),
)
RELAXED = (
    ("cron", "Cron rows only; NULL on webhook rows"),
    ("timezone", "Cron rows only; NULL on webhook rows"),
    ("next_run", "Cron rows only; always NULL on webhook rows"),
)
CONSTRAINTS = (
    ("ck_entity_schedules_trigger_kind", "trigger_kind IN ('cron', 'webhook')"),
    ("ck_entity_schedules_cron_fields", "trigger_kind = 'webhook' OR (cron IS NOT NULL AND timezone IS NOT NULL)"),
    ("ck_entity_schedules_webhook_no_next_run", "trigger_kind <> 'webhook' OR next_run IS NULL"),
)
INDEXES = (
    ("uq_entity_schedules_legacy_version", "UNIQUE", "(legacy_version_id) WHERE legacy_version_id IS NOT NULL"),
    ("uq_entity_schedules_legacy_application", "UNIQUE",
     "(legacy_application_id) WHERE legacy_application_id IS NOT NULL"),
    ("ix_entity_schedules_credential_refs", "", "USING gin (credential_refs)"),
)
TABLE_COMMENT = "Triggers of agent and pipeline versions: cron schedules and webhooks (trigger_kind)"

# A schema needs the task while the column is missing or the active-run index still covers webhook runs
_PENDING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t WHERE t.table_name = :table "
    "AND t.table_schema ~ '^p_[0-9]+$' AND (NOT EXISTS (SELECT 1 FROM information_schema.columns col "
    "WHERE col.table_schema = t.table_schema AND col.table_name = :table AND col.column_name = 'trigger_kind') "
    "OR EXISTS (SELECT 1 FROM pg_indexes i WHERE i.schemaname = t.table_schema AND i.indexname = :index "
    "AND i.indexdef NOT LIKE '%webhook%'))")


def project_ids_pending():
    with db.get_session(None) as session:
        rows = session.execute(_PENDING_QUERY, {"table": TABLE, "index": ACTIVE_INDEX}).fetchall()
    return [int(row[0][2:]) for row in rows]


def statements(schema):
    """The idempotent DDL for one schema, in order; the schema name comes from an integer id only."""
    table, runs = f'"{schema}".{TABLE}', f'"{schema}".{RUNS}'
    out = [f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {ddl}" for name, ddl, _ in COLUMNS]
    out += [f"ALTER TABLE {table} ALTER COLUMN {name} DROP NOT NULL" for name, _ in RELAXED]
    out += [
        f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{name}' "
        f"AND conrelid = '{table}'::regclass) THEN ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({check}); "
        f"END IF; END $$"
        for name, check in CONSTRAINTS
    ]
    out += [f"CREATE {unique} INDEX IF NOT EXISTS {name} ON {table} {body}" for name, unique, body in INDEXES]
    out += [
        f"DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = '{schema}' AND indexname = "
        f"'{ACTIVE_INDEX}' AND indexdef NOT LIKE '%webhook%') THEN DROP INDEX \"{schema}\".{ACTIVE_INDEX}; "
        f"END IF; END $$",
        f"CREATE UNIQUE INDEX IF NOT EXISTS {ACTIVE_INDEX} ON {runs} (schedule_id) "
        f"WHERE status IN ('running', 'waiting_for_approval') AND trigger <> 'webhook'",
        f"COMMENT ON TABLE {table} IS '{TABLE_COMMENT}'",
    ]
    out += [f"COMMENT ON COLUMN {table}.{name} IS '{comment}'" for name, _, comment in COLUMNS]
    out += [f"COMMENT ON COLUMN {table}.{name} IS '{comment}'" for name, comment in RELAXED]
    return out


def apply_webhook_trigger_columns(project_ids, dry_run=False):
    """``(migrated, failed)``; one transaction per project, so a failure leaves that schema unchanged."""
    migrated, failed = [], []
    for pid in project_ids:
        if dry_run:
            migrated.append(pid)
            continue
        try:
            with db.get_session(pid) as session:
                for sql in statements(f"p_{int(pid)}"):
                    session.execute(text(sql))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("webhook trigger schema: failed in project %s", pid)
            failed.append(pid)
    return migrated, failed
