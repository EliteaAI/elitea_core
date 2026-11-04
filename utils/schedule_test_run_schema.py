"""Test Run columns and indexes of entity_schedule_runs in project schemas that predate them.

New projects get them from create_all. Existing schemas get them from the admin task `migrate_schedule_test_runs`.
The columns are not mapped by the ORM, so a schema that has not run the task still reads and writes saved runs.
"""
from .skill_publish_schema import apply_project_statements

# `pid` is an integer from the project list, never user text
TEST_RUN_STATEMENTS = (
    "ALTER TABLE p_{pid}.entity_schedule_runs ADD COLUMN IF NOT EXISTS form JSONB NULL",
    "ALTER TABLE p_{pid}.entity_schedule_runs ADD COLUMN IF NOT EXISTS application_id INTEGER NULL "
    "REFERENCES p_{pid}.applications(id) ON DELETE CASCADE",
    "ALTER TABLE p_{pid}.entity_schedule_runs ALTER COLUMN schedule_id DROP NOT NULL",
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_schedule_runs_active_test ON p_{pid}.entity_schedule_runs "
    "(requested_by, application_id) WHERE schedule_id IS NULL AND status IN ('running', 'waiting_for_approval')",
    "CREATE INDEX IF NOT EXISTS ix_entity_schedule_runs_test_owner ON p_{pid}.entity_schedule_runs "
    "(requested_by, application_id, started_at DESC, id DESC) WHERE schedule_id IS NULL",
    "CREATE INDEX IF NOT EXISTS ix_entity_schedule_runs_version ON p_{pid}.entity_schedule_runs (version_id)",
)


def apply_schedule_test_run_columns(project_ids):
    """Run the statements in one transaction per project; returns (migrated ids, failed projects)."""
    return apply_project_statements(project_ids, TEST_RUN_STATEMENTS)


def dry_run_flag(param) -> bool:
    """A dry run unless the param says `dry_run=false`; `dry_run` and `dry_run=true` also mean a dry run."""
    for segment in (param or "").split(";"):
        key, _, value = segment.strip().partition("=")
        if key.strip().lower() == "dry_run":
            return value.strip().lower() not in ("false", "0", "no")
    return True


def run_schedule_test_run_migration(project_ids, dry_run: bool) -> dict:
    """The result of the admin task; a dry run counts the projects it would change and writes nothing."""
    if dry_run:
        return {"migrated": len(project_ids), "failed": 0, "failed_projects": [], "dry_run": True}
    migrated, failed = apply_schedule_test_run_columns(project_ids)
    return {"migrated": len(migrated), "failed": len(failed), "failed_projects": failed, "dry_run": False}
