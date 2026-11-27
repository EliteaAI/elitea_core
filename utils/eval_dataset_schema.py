"""Idempotent per-project schema setup for eval dataset scoping.

Adds the columns eval dataset scoping relies on to each project (tenant) schema, plus the
case-level ``expected_trajectory`` reference (#6809 item 4) and ``time_limit_seconds`` override
(performance P-5).
`create_all` provisions these for new projects automatically; existing project
schemas predate the columns and are brought up to date here via the admin
migration task.
"""
from sqlalchemy import text

from pylon.core.tools import log
from tools import db

_COLUMN_STATEMENTS = (
    "ALTER TABLE p_{pid}.eval_dataset ADD COLUMN IF NOT EXISTS agent_id INTEGER",
    "ALTER TABLE p_{pid}.eval_dataset "
    "ADD COLUMN IF NOT EXISTS is_shared BOOLEAN NOT NULL DEFAULT false",
)

# A schema that got agent_id without its FK can hold datasets whose agent was since deleted, and the
# FK cannot be added over them. They are deleted first, as ON DELETE CASCADE would have done (their
# cases cascade, suites pointing at them get dataset_id = NULL). A dry run only selects them.
_ORPHANS = (
    "FROM p_{pid}.eval_dataset d WHERE d.agent_id IS NOT NULL "
    "AND NOT EXISTS (SELECT 1 FROM p_{pid}.applications a WHERE a.id = d.agent_id)"
)
_SELECT_ORPHANS = "SELECT d.id " + _ORPHANS + " ORDER BY d.id"
_DELETE_ORPHANS = "DELETE " + _ORPHANS + " RETURNING d.id"

_CONSTRAINT_STATEMENTS = (
    "DO $$ BEGIN "
    "ALTER TABLE p_{pid}.eval_dataset "
    "ADD CONSTRAINT eval_dataset_agent_id_fkey "
    "FOREIGN KEY (agent_id) REFERENCES p_{pid}.applications(id) ON DELETE CASCADE; "
    "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
    # Earlier builds named this index eval_dataset_agent_id_idx, which create_all's
    # ix_tenant_eval_dataset_agent_id duplicated: rename it, or drop it when both exist.
    "DO $$ BEGIN "
    "IF to_regclass('p_{pid}.eval_dataset_agent_id_idx') IS NOT NULL THEN "
    "IF to_regclass('p_{pid}.ix_tenant_eval_dataset_agent_id') IS NULL THEN "
    "ALTER INDEX p_{pid}.eval_dataset_agent_id_idx RENAME TO ix_tenant_eval_dataset_agent_id; "
    "ELSE DROP INDEX p_{pid}.eval_dataset_agent_id_idx; "
    "END IF; END IF; END $$",
    "CREATE INDEX IF NOT EXISTS ix_tenant_eval_dataset_agent_id "
    "ON p_{pid}.eval_dataset (agent_id)",
    "ALTER TABLE p_{pid}.eval_dataset_case ADD COLUMN IF NOT EXISTS expected_trajectory JSONB",
    "ALTER TABLE p_{pid}.eval_dataset_case ADD COLUMN IF NOT EXISTS time_limit_seconds INTEGER",
)

_MIGRATION_STATEMENTS = _COLUMN_STATEMENTS + _CONSTRAINT_STATEMENTS

# One progress line per this many projects, so a large fleet logs a handful of lines, not one per schema.
_PROGRESS_EVERY = 50


def apply_eval_dataset_columns(project_ids, dry_run=False):
    """Returns ``(migrated, failed, orphans)``; ``orphans`` lists the orphaned dataset ids per project,
    deleted on a real run and only reported on a dry run (which changes nothing)."""
    migrated, failed, orphans = [], [], []
    total = len(project_ids)
    log.info("eval dataset schema: %s %s project(s)", "checking" if dry_run else "migrating", total)
    for done, pid in enumerate(project_ids, start=1):
        try:
            with db.with_project_schema_session(pid) as session:
                for statement in _COLUMN_STATEMENTS:
                    session.execute(text(statement.format(pid=pid)))
                query = _SELECT_ORPHANS if dry_run else _DELETE_ORPHANS
                dataset_ids = sorted(row[0] for row in session.execute(text(query.format(pid=pid))))
                if dataset_ids:
                    log.warning("eval dataset schema: project %s has %s orphaned dataset(s)%s: %s", pid,
                                len(dataset_ids), "" if dry_run else ", deleted", dataset_ids)
                    orphans.append({"project_id": pid, "dataset_ids": dataset_ids})
                if dry_run:
                    session.rollback()
                else:
                    for statement in _CONSTRAINT_STATEMENTS:
                        session.execute(text(statement.format(pid=pid)))
                    session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("eval dataset schema: failed to migrate project %s", pid)
            failed.append({"project_id": pid})
        if done % _PROGRESS_EVERY == 0 and done < total:
            log.info("eval dataset schema: %s/%s done (%s failed)", done, total, len(failed))
    log.info("eval dataset schema: finished, %s=%s failed=%s orphaned_datasets=%s",
             "checked" if dry_run else "migrated", len(migrated), len(failed),
             sum(len(o["dataset_ids"]) for o in orphans))
    return migrated, failed, orphans
