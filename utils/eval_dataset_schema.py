"""Idempotent per-project schema setup for eval dataset scoping.

Adds the columns eval dataset scoping relies on to each project (tenant) schema.
`create_all` provisions these for new projects automatically; existing project
schemas predate the columns and are brought up to date here via the admin
migration task.
"""
from sqlalchemy import text

from pylon.core.tools import log
from tools import db

_MIGRATION_STATEMENTS = (
    "ALTER TABLE p_{pid}.eval_dataset ADD COLUMN IF NOT EXISTS agent_id INTEGER",
    "ALTER TABLE p_{pid}.eval_dataset "
    "ADD COLUMN IF NOT EXISTS is_shared BOOLEAN NOT NULL DEFAULT false",
    "DO $$ BEGIN "
    "ALTER TABLE p_{pid}.eval_dataset "
    "ADD CONSTRAINT eval_dataset_agent_id_fkey "
    "FOREIGN KEY (agent_id) REFERENCES p_{pid}.applications(id) ON DELETE CASCADE; "
    "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
    "CREATE INDEX IF NOT EXISTS eval_dataset_agent_id_idx "
    "ON p_{pid}.eval_dataset (agent_id)",
)


def apply_eval_dataset_columns(project_ids):
    migrated, failed = [], []
    for pid in project_ids:
        try:
            with db.with_project_schema_session(pid) as session:
                for statement in _MIGRATION_STATEMENTS:
                    session.execute(text(statement.format(pid=pid)))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("eval dataset schema: failed to migrate project %s", pid)
            failed.append({"project_id": pid})
    return migrated, failed
