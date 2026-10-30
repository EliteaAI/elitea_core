"""Toolkit mandates in ``autonomous_mandate`` of project schemas that predate them (#7305, C-1009).

A mandate covers an agent version or the index schedules of one toolkit (``toolkit_id``; the three version columns
NULL). New projects get the column, the check and the partial unique index from ``create_all``. Existing schemas get them
from the startup safety net in ``module.py``: one catalog query, then, per schema that lacks any of them, one
transaction under a 5 s ``lock_timeout`` (tried again a few times): ``ADD COLUMN IF NOT EXISTS toolkit_id`` and
``toolkit_digest`` (the toolkit as approved, C9-SEC-73), ``DROP NOT
NULL`` on ``application_id``, ``agent_version`` and ``agent_binding_ref``, the check ``autonomous_mandate_kind_ck`` when
the catalog does not have it (every existing row is an agent mandate, so it holds), and ``CREATE UNIQUE INDEX IF NOT
EXISTS`` for one live toolkit mandate per toolkit and owner. Until then, in that schema, creating or listing mandates
fails (the INSERT and the list name the column) and index schedules are refused as before; the admission of agent-mandate
runs is not touched (the column is deferred and never read for them).
"""
import time

from sqlalchemy import text
from pylon.core.tools import log
from tools import db

from ..models.agent_identity import LIVE_MANDATE_STATES, MANDATE_KIND_CHECK, AutonomousMandate

TABLE = AutonomousMandate.__tablename__
CHECK, INDEX = "autonomous_mandate_kind_ck", "uq_autonomous_mandate_live_toolkit"
NULLABLE = ("application_id", "agent_version", "agent_binding_ref")
LOCK_TIMEOUT = "5s"
ATTEMPTS, RETRY_DELAY_S = 3, 5
_LIVE = ", ".join(repr(state) for state in LIVE_MANDATE_STATES)
_MISSING_QUERY = text(
    "SELECT t.table_schema FROM information_schema.tables t WHERE t.table_name = :table "
    "AND t.table_schema ~ '^p_[0-9]+$' AND ("
    "NOT EXISTS (SELECT 1 FROM information_schema.columns col WHERE col.table_schema = t.table_schema "
    "AND col.table_name = :table AND col.column_name = 'toolkit_id') "
    "OR NOT EXISTS (SELECT 1 FROM information_schema.columns col WHERE col.table_schema = t.table_schema "
    "AND col.table_name = :table AND col.column_name = 'toolkit_digest') "
    "OR EXISTS (SELECT 1 FROM information_schema.columns col WHERE col.table_schema = t.table_schema "
    "AND col.table_name = :table AND col.column_name = ANY(:nullable) AND col.is_nullable = 'NO') "
    "OR NOT EXISTS (SELECT 1 FROM pg_constraint con JOIN pg_namespace ns ON ns.oid = con.connamespace "
    "WHERE ns.nspname = t.table_schema AND con.conname = :check) "
    "OR NOT EXISTS (SELECT 1 FROM pg_indexes ix WHERE ix.schemaname = t.table_schema AND ix.indexname = :index))")


def project_ids_missing_toolkit_mandates():
    with db.get_session(None) as session:
        rows = session.execute(_MISSING_QUERY, {
            "table": TABLE, "nullable": list(NULLABLE), "check": CHECK, "index": INDEX}).fetchall()
    return [int(row[0][2:]) for row in rows]


def _apply_once(pid) -> None:
    schema = f'"p_{int(pid)}"'
    altered = ", ".join(["ADD COLUMN IF NOT EXISTS toolkit_id INTEGER",
                         "ADD COLUMN IF NOT EXISTS toolkit_digest VARCHAR(80)"]
                        + [f"ALTER COLUMN {name} DROP NOT NULL" for name in NULLABLE])
    with db.get_session(pid) as session:
        session.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
        session.execute(text(f"ALTER TABLE {schema}.{TABLE} {altered}"))
        present = session.execute(text(
            "SELECT 1 FROM pg_constraint con JOIN pg_namespace ns ON ns.oid = con.connamespace "
            "WHERE ns.nspname = :schema AND con.conname = :check"), {"schema": f"p_{int(pid)}", "check": CHECK}).first()
        if present is None:
            session.execute(text(f"ALTER TABLE {schema}.{TABLE} ADD CONSTRAINT {CHECK} CHECK ({MANDATE_KIND_CHECK})"))
        session.execute(text(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {INDEX} ON {schema}.{TABLE} (toolkit_id, owner_user_id) "
            f"WHERE toolkit_id IS NOT NULL AND state IN ({_LIVE})"))
        session.commit()


def apply_toolkit_mandates(project_ids, attempts=ATTEMPTS, delay_s=RETRY_DELAY_S, sleep=time.sleep):
    """``(migrated, failed)``; one transaction per project, each tried up to ``attempts`` times. The schema name is
    built from the integer id only."""
    migrated, pending = [], [int(pid) for pid in project_ids]
    for attempt in range(attempts):
        failed = []
        for pid in pending:
            try:
                _apply_once(pid)
                migrated.append(pid)
            except Exception:  # pylint: disable=W0703
                failed.append(pid)
        pending = failed
        if not pending:
            break
        if attempt + 1 < attempts:
            sleep(delay_s)
    if pending:
        log.error("mandate toolkit schema: toolkit mandates could not be enabled in projects %s; creating one there "
                  "fails until the next start", pending)
    return migrated, pending
