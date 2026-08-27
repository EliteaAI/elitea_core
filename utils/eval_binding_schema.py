"""Idempotent per-project schema setup for the eval_binding uniqueness guards.

``EvalBinding.__table_args__`` declares three ``UniqueConstraint``s, but eval tables are
provisioned by ``create_all``, which creates tables and never alters existing ones. Any schema whose
``eval_binding`` predates those constraints therefore has no DB-level guard, leaving only the
app-level pre-check — which is a read-then-insert race. A duplicate binding scores the same criterion
twice and silently doubles its weight in the headline, so this is a correctness guard, not tidiness.

Postgres has no ``ADD CONSTRAINT IF NOT EXISTS``, so each statement is wrapped in a ``DO`` block
guarded on ``pg_constraint``. That keeps the constraint *name* identical to the model's, which a
``CREATE UNIQUE INDEX`` fallback would not — so a migrated schema and a freshly created one stay
free of drift.
"""
from sqlalchemy import text
from pylon.core.tools import log
from tools import db


#: (constraint name, columns) — mirrors ``EvalBinding.__table_args__``. NULLs are distinct in
#: Postgres, so each constraint only bites on the column actually set for that binding kind.
_BINDING_CONSTRAINTS = (
    ('_eval_binding_suite_dimension_uc', 'suite_id, dimension_id'),
    ('_eval_binding_suite_platform_uc', 'suite_id, platform_key'),
)

_ADD_CONSTRAINT = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = 'p_{pid}' AND t.relname = 'eval_binding' AND c.conname = '{name}'
    ) THEN
        ALTER TABLE p_{pid}.eval_binding ADD CONSTRAINT {name} UNIQUE ({columns});
    END IF;
END $$;
"""

_DUPLICATE_QUERY = """
SELECT '{column}' AS column_name, suite_id, {column} AS value, count(*) AS rows
  FROM p_{pid}.eval_binding
 WHERE {column} IS NOT NULL
 GROUP BY suite_id, {column}
HAVING count(*) > 1
"""


def find_duplicate_bindings(project_ids):
    """Existing duplicates, per project. ``ADD CONSTRAINT`` fails on a table that already
    violates it, so this is what a dry run reports: the rows an operator must resolve first."""
    duplicates = {}
    for pid in project_ids:
        rows = []
        try:
            with db.with_project_schema_session(pid) as session:
                for _, columns in _BINDING_CONSTRAINTS:
                    column = columns.split(', ')[1]
                    rows.extend(
                        dict(row._mapping) for row in
                        session.execute(text(_DUPLICATE_QUERY.format(pid=pid, column=column)))
                    )
        except Exception:  # pylint: disable=W0703
            log.exception('eval_binding schema: could not scan project %s', pid)
            continue
        if rows:
            duplicates[pid] = rows
    return duplicates


def apply_eval_binding_constraints(project_ids):
    """Add the three constraints to each project schema. Returns ``(migrated, failed)``.

    One project is one transaction, and a failure is logged and skipped rather than aborting the
    batch — a schema carrying duplicates (or missing the table entirely) must not stop the others
    from being guarded.
    """
    migrated, failed = [], []
    for pid in project_ids:
        try:
            with db.with_project_schema_session(pid) as session:
                for name, columns in _BINDING_CONSTRAINTS:
                    session.execute(text(_ADD_CONSTRAINT.format(pid=pid, name=name, columns=columns)))
                session.commit()
            migrated.append(pid)
        except Exception as exc:  # pylint: disable=W0703
            log.exception('eval_binding schema: failed to migrate project %s', pid)
            failed.append({'project_id': pid, 'error': str(exc)})
    return migrated, failed
