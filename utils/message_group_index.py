"""Admin task: index the message groups by conversation (EliteaAI/elitea_issues#7682, PR elitea_core#766 review).

The run outcome reads each conversation's last message group (the highest id). Without an index on
`conversation_id`, Postgres walks the primary key for every conversation, so the Status filter takes seconds on a
project with thousands of chats. `(conversation_id, id)` lets each lookup stop at the last group.

`CREATE INDEX CONCURRENTLY` builds the index without locking writes to the table, and it cannot run inside a
transaction, so each project's statement runs on an autocommit connection (the pattern of legacy_schedule_migration).
"""

from sqlalchemy import text

from pylon.core.tools import log


INDEX_NAME = 'ix_chat_message_group_conversation_last'

CREATE_STATEMENT = (
    'CREATE INDEX CONCURRENTLY IF NOT EXISTS {index} '
    'ON p_{pid}.chat_message_group (conversation_id, id)'
)

INDEX_EXISTS = text(
    'SELECT 1 FROM pg_indexes WHERE schemaname = :schema AND indexname = :index'
)


def _index_present(connection, project_id):
    return connection.execute(
        INDEX_EXISTS, {'schema': f'p_{project_id}', 'index': INDEX_NAME},
    ).first() is not None


def _create_index(connection, project_id):
    connection.execute(text(CREATE_STATEMENT.format(index=INDEX_NAME, pid=project_id)))


def run_message_group_index(project_ids, dry_run: bool) -> dict:
    """Create the conversation index in each project; a project that has it already is left alone.

    Returns the projects that were changed (or would be, in a dry run), the ones that already had the index, and the
    ones that failed. A failing project is listed and the run goes on.
    """
    from tools import db  # imported here: tools.db needs the running plugin

    changed, present, failed = [], [], []
    with db.engine.connect().execution_options(isolation_level='AUTOCOMMIT') as connection:
        for project_id in project_ids:
            try:
                if _index_present(connection, project_id):
                    present.append(project_id)
                    continue
                if not dry_run:
                    _create_index(connection, project_id)
                changed.append(project_id)
            except Exception:  # pylint: disable=W0703
                log.exception('message group index: failed for project %s', project_id)
                failed.append({'project_id': project_id})
    return {'changed': changed, 'present': present, 'failed': failed, 'dry_run': dry_run}
