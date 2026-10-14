"""Idempotent per-project schema fix for eval case usage cost precision (#6716).

``eval_case_usage.cost`` was first created as NUMERIC(18,8), but the usage ledger it is settled
from counts nano-USD, so the ninth decimal place was dropped. Fresh environments get NUMERIC(18,9)
from create_all; existing p_N schemas are widened here. Projects without the table are skipped.
"""
from sqlalchemy import text

from pylon.core.tools import log
from tools import db

_MIGRATION_STATEMENTS = (
    "DO $$ BEGIN "
    "IF EXISTS (SELECT 1 FROM information_schema.columns "
    "WHERE table_schema = 'p_{pid}' AND table_name = 'eval_case_usage' "
    "AND column_name = 'cost' AND numeric_scale IS DISTINCT FROM 9) THEN "
    "ALTER TABLE p_{pid}.eval_case_usage ALTER COLUMN cost TYPE NUMERIC(18,9); "
    "END IF; END $$",
)


def apply_eval_case_usage_columns(project_ids):
    migrated, failed = [], []
    for pid in project_ids:
        try:
            with db.with_project_schema_session(pid) as session:
                for statement in _MIGRATION_STATEMENTS:
                    session.execute(text(statement.format(pid=pid)))
                session.commit()
            migrated.append(pid)
        except Exception:  # pylint: disable=W0703
            log.exception("eval case usage schema: failed to migrate project %s", pid)
            failed.append({"project_id": pid})
    return migrated, failed
