"""Test Runs: a run of the unsaved Save form, stored as one run row that has no schedule.

The row has `schedule_id` NULL, `trigger` `test`, `requested_by` the caller, and the form and the agent or pipeline
in the unmapped columns `form` and `application_id`. No schedule row is written, so the schedule list, Last Run,
Next Run, the tick and the export never see a Test Run. Every read of a Test Run goes through `test_run_scope`.
"""
from collections import namedtuple
from contextlib import contextmanager

from pylon.core.tools import log
from sqlalchemy import delete, select
from sqlalchemy.exc import DBAPIError

from ..models.entity_schedule import RUN_APPLICATION_ID, RUN_FORM, EntityScheduleRun
from ..models.pd.entity_schedule import ScheduleCreate

ACTIVE = ('running', 'waiting_for_approval')
# Every click on Test adds a run, so the table would grow without end. The newest runs are the ones Run History
# shows; an older run is no use to the owner.
KEEP_TEST_RUNS = 50
UNIQUE_VIOLATION = '23505'
# The schema has not got the Test Run columns yet (the admin task `migrate_schedule_test_runs` did not run): a missing
# column, or a schedule id that still cannot be NULL
OLD_SCHEMA = ('23502', '42703')  # not-null violation, undefined column
# SQLite (the unit tests) names the same errors without a pgcode
SQLITE_CODES = {'SQLITE_CONSTRAINT_UNIQUE': UNIQUE_VIOLATION, 'SQLITE_CONSTRAINT_NOTNULL': '23502'}

# The chat of a trimmed run: the uuid names its bucket, the thread ids name its checkpoints
TrimmedChat = namedtuple('TrimmedChat', 'uuid thread_ids')


class TestRunUnavailable(Exception):
    """The project schema has no Test Run columns yet; the routes answer 503 `test_run_unavailable`."""

    __test__ = False  # not a pytest class, although the name starts with Test


def error_code(error: DBAPIError) -> str | None:
    orig = getattr(error, 'orig', None)
    return getattr(orig, 'pgcode', None) or SQLITE_CODES.get(getattr(orig, 'sqlite_errorname', None))


@contextmanager
def old_schema_guard():
    """Turn the database errors of a schema without the Test Run columns into `TestRunUnavailable`."""
    try:
        yield
    except DBAPIError as e:
        if error_code(e) in OLD_SCHEMA:
            raise TestRunUnavailable() from e
        raise


def test_run_scope(user_id: int, application_id: int | None = None) -> list:
    """The conditions that pick the Test Runs of one owner, of one agent or pipeline when `application_id` is given."""
    scope = [
        EntityScheduleRun.schedule_id.is_(None), EntityScheduleRun.trigger == 'test',
        EntityScheduleRun.requested_by == user_id,
    ]
    if application_id is not None:
        scope.append(RUN_APPLICATION_ID == application_id)
    return scope


def insert_test_run(session, user_id: int, body: ScheduleCreate) -> int | None:
    """Insert a running Test Run of the form; its id, or None when the owner has an active Test Run of this entity.

    Raises:
        TestRunUnavailable: The schema has not got the Test Run columns yet.
    """
    form = body.model_dump(mode='json', exclude={'risk_ack', 'enabled', 'signing_secret_value'})  # no secret in a run row
    statement = EntityScheduleRun.__table__.insert().values(
        schedule_id=None, trigger='test', status='running', requested_by=user_id,
        **{RUN_FORM.key: form, RUN_APPLICATION_ID.key: body.application_id},
    )
    try:
        with old_schema_guard():
            # A savepoint, so the conflict rolls back the insert only
            with session.begin_nested():
                return session.execute(statement).inserted_primary_key[0]
    except DBAPIError as e:
        if error_code(e) == UNIQUE_VIOLATION:
            return None
        raise


def active_test_run_id(session, user_id: int, application_id: int) -> int | None:
    with old_schema_guard():
        return session.scalar(select(EntityScheduleRun.id).where(
            *test_run_scope(user_id, application_id), EntityScheduleRun.status.in_(ACTIVE)))


def load_test_run(session, user_id: int, run_id: int, application_id: int | None = None) -> EntityScheduleRun | None:
    """The Test Run of this owner (of this agent or pipeline, when given); None for another owner, a saved run or
    another entity, so the route answers 404 for each of them alike."""
    with old_schema_guard():
        return session.scalars(select(EntityScheduleRun).where(
            EntityScheduleRun.id == run_id, *test_run_scope(user_id, application_id))).one_or_none()


def form_of_test_run(session, run_id: int) -> dict:
    """The stored Save form of a Test Run."""
    return session.scalar(select(RUN_FORM).where(EntityScheduleRun.id == run_id)) or {}


def trim_test_runs(session, user_id: int, application_id: int) -> list[TrimmedChat]:
    """Delete the Test Runs of the owner and the agent beyond the newest `KEEP_TEST_RUNS`, and the chats of those runs.

    An active run stays. The chats go in the same transaction. The caller passes the returned chats to
    `remove_trimmed_chats` after the commit: a bucket or a checkpoint cannot be rolled back.
    """
    scope = test_run_scope(user_id, application_id)
    old = (select(EntityScheduleRun.id).where(*scope)
           .order_by(EntityScheduleRun.started_at.desc(), EntityScheduleRun.id.desc()).offset(KEEP_TEST_RUNS))
    doomed = session.execute(select(EntityScheduleRun.id, EntityScheduleRun.conversation_id).where(
        EntityScheduleRun.id.in_(old), EntityScheduleRun.status.not_in(ACTIVE))).all()
    if not doomed:
        return []
    session.execute(delete(EntityScheduleRun).where(EntityScheduleRun.id.in_([run_id for run_id, _ in doomed])))
    # Late: only the trim reads the chat models, which the route tests do not load
    from ..models.conversation import Conversation
    from ..models.message_group import ConversationMessageGroup
    chat_ids = {chat_id for _, chat_id in doomed if chat_id is not None}
    chats = []
    for conversation in session.scalars(select(Conversation).where(Conversation.id.in_(chat_ids))) if chat_ids else []:
        threads = {str(conversation.uuid)}
        threads.update(
            meta['thread_id'] for (meta,) in session.execute(
                select(ConversationMessageGroup.meta).where(
                    ConversationMessageGroup.conversation_id == conversation.id,
                    ConversationMessageGroup.meta.isnot(None)))
            if isinstance(meta, dict) and meta.get('thread_id'))
        chats.append(TrimmedChat(str(conversation.uuid), sorted(threads)))
        session.delete(conversation)
    return chats


def remove_trimmed_chats(project_id: int, chats: list[TrimmedChat]) -> None:
    """Remove the checkpoints and the buckets of deleted chats, after the commit; a failure is logged, never raised."""
    for chat in chats:
        try:
            # Late: the storage and the vector store helpers are needed only when a run was trimmed
            from tools import MinioClient, this

            from .vectorstore import get_pgvector_connection_string
            this.module.event_node.emit('indexer_delete_checkpoint', {
                'thread_ids': chat.thread_ids, 'pgvector_connstr': get_pgvector_connection_string(project_id),
            })
            MinioClient.from_project_id(project_id).remove_bucket(f'conversation_{chat.uuid}')
        except Exception:  # noqa: BLE001 -- the rows are gone; the age cleanup removes what is left
            log.exception('Test run: cannot remove the checkpoints or the bucket of chat %s', chat.uuid)
