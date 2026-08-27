"""Idempotent per-project schema setup for eval dimension agent-scoping and the
folded-in Code engine columns.

Adds the column/index/constraint changes eval dimension agent-scoping (and, since the
Code-engine fold, the `code`/`return_contract` columns formerly on the now-deleted
`eval_code_validation` table) rely on to each project (tenant) schema. `create_all`
provisions these for new projects automatically from models/evaluation.py's
__table_args__; existing project schemas predate them and are brought up to date here
via the admin migration task. Keep the index/constraint names here identical to what
SQLAlchemy generates for a fresh schema, or the two paths diverge.
"""
from sqlalchemy import text

from pylon.core.tools import log
from tools import db

_MIGRATION_STATEMENTS = (
    "ALTER TABLE p_{pid}.eval_dimension ADD COLUMN IF NOT EXISTS agent_id INTEGER",
    # Code-engine fold (EL-2444): dimensions with allowed_engines == ['code'] author
    # their verdict via a Python script instead of a rubric.
    "ALTER TABLE p_{pid}.eval_dimension ADD COLUMN IF NOT EXISTS code TEXT",
    "ALTER TABLE p_{pid}.eval_dimension ADD COLUMN IF NOT EXISTS return_contract VARCHAR(16)",
    "DO $$ BEGIN "
    "ALTER TABLE p_{pid}.eval_dimension "
    "ADD CONSTRAINT eval_dimension_agent_id_fkey "
    "FOREIGN KEY (agent_id) REFERENCES p_{pid}.applications(id) ON DELETE CASCADE; "
    "EXCEPTION WHEN duplicate_object THEN NULL; END $$",
    # Named to match what SQLAlchemy's index=True generates for a fresh schema
    # (ix_<table>_<column>) so existing and freshly-provisioned schemas don't diverge.
    "CREATE INDEX IF NOT EXISTS ix_eval_dimension_agent_id "
    "ON p_{pid}.eval_dimension (agent_id)",
    "DROP INDEX IF EXISTS p_{pid}.eval_dimension_agent_id_idx",
    # Replace the blanket (tier, name) unique constraint with two partial unique indexes —
    # see models/evaluation.py's __table_args__ comment for why: agent_adhoc must be unique
    # per (name, agent_id), not a single project-wide namespace, while project/platform tiers
    # keep the tier-wide namespace unchanged.
    "ALTER TABLE p_{pid}.eval_dimension "
    "DROP CONSTRAINT IF EXISTS _eval_dimension_tier_name_uc",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_eval_dimension_tier_name "
    "ON p_{pid}.eval_dimension (tier, name) WHERE tier != 'agent_adhoc'",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_eval_dimension_tier_name_agent "
    "ON p_{pid}.eval_dimension (tier, name, agent_id) WHERE tier = 'agent_adhoc'",
)


def apply_eval_dimension_columns(project_ids):
    migrated, failed = [], []
    for pid in project_ids:
        try:
            with db.with_project_schema_session(pid) as session:
                for statement in _MIGRATION_STATEMENTS:
                    session.execute(text(statement.format(pid=pid)))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("eval dimension schema: failed to migrate project %s", pid)
            failed.append({"project_id": pid})
    return migrated, failed
