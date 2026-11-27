"""Admin task: index the message groups by conversation (EliteaAI/elitea_issues#7682, PR elitea_core#766 review).

The run outcome reads each conversation's last message group (the highest id). Without an index on
`conversation_id`, Postgres walks the primary key for every conversation, so the Status filter takes seconds on a
project with thousands of chats. `(conversation_id, id)` lets each lookup stop at the last group.

`CREATE INDEX CONCURRENTLY` builds the index without locking writes to the table, and it cannot run inside a
transaction, so each project's statement runs on an autocommit connection (the pattern of legacy_schedule_migration).

A concurrent build that fails (a deadlock, a statement timeout, a cancel) leaves an INVALID index under the same name.
The planner never uses it and `IF NOT EXISTS` would skip it, so a project holding one is dropped and built again.
"""

from sqlalchemy import text

from pylon.core.tools import log


INDEX_NAME = 'ix_chat_message_group_conversation_last'

CREATE_STATEMENT = (
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS {index} '
    'ON p_{pid}.chat_message_group (conversation_id, id)'
)

DROP_STATEMENT = 'DROP INDEX CONCURRENTLY IF EXISTS p_{pid}.{index}'

# The index of this name in the project, with the flag the planner uses to decide whether it may read it
INDEX_STATE = text(
    'SELECT i.indisvalid FROM pg_index i '
    'JOIN pg_class c ON c.oid = i.indexrelid '
    'JOIN pg_namespace n ON n.oid = c.relnamespace '
    'WHERE n.nspname = :schema AND c.relname = :index'
)

PRESENT = 'present'
INVALID = 'invalid'
MISSING = 'missing'


def _index_state(connection, project_id):
    """MISSING, PRESENT (valid) or INVALID (left behind by a failed concurrent build)."""
    row = connection.execute(INDEX_STATE, {'schema': f'p_{project_id}', 'index': INDEX_NAME}).first()
    if row is None:
        return MISSING
    return PRESENT if row[0] else INVALID


def _create_index(connection, project_id):
    connection.execute(text(CREATE_STATEMENT.format(index=INDEX_NAME, pid=project_id)))


def _drop_index(connection, project_id):
    connection.execute(text(DROP_STATEMENT.format(index=INDEX_NAME, pid=project_id)))


def run_message_group_index(project_ids, dry_run: bool) -> dict:
    """Create the conversation index in each project; a project that has a valid one is left alone.

    Returns the projects that were changed (or would be, in a dry run), the ones that already had a valid index, the
    ones whose invalid index was rebuilt (or would be), and the ones that failed. A failing project is listed and the
    run goes on. A rebuilt project is also listed under `changed`.
    """
    from tools import db  # imported here: tools.db needs the running plugin

    changed, present, rebuilt, failed = [], [], [], []
    with db.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as connection:
        for project_id in project_ids:
            try:
                state = _index_state(connection, project_id)
                if state == PRESENT:
                    present.append(project_id)
                    continue
                if state == INVALID:
                    rebuilt.append(project_id)
                    if not dry_run:
                        _drop_index(connection, project_id)
                if not dry_run:
                    _create_index(connection, project_id)
                changed.append(project_id)
            except Exception:  # pylint: disable=W0703
                log.exception('message group index: failed for project %s', project_id)
                failed.append({'project_id': project_id})
    return {'changed': changed, 'present': present, 'rebuilt': rebuilt, 'failed': failed, 'dry_run': dry_run}
