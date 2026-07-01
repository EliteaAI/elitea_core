"""PostgreSQL advisory lock for database migrations.

Ensures only one pod runs migrations at a time during horizontal scaling.
Uses pg_try_advisory_lock with a configurable timeout and explicit unlock.
"""

import contextlib
import time

import sqlalchemy
import sqlalchemy.pool

from pylon.core.tools import log


DEFAULT_LOCK_ID = 900100
DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_POLL_INTERVAL = 2.0


class MigrationLockTimeout(Exception):
    """Raised when the migration lock cannot be acquired within the timeout."""


class MigrationLockError(Exception):
    """Raised when advisory lock operations fail unexpectedly."""


@contextlib.contextmanager
def migration_lock(
    db_url,
    lock_id=DEFAULT_LOCK_ID,
    timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    poll_interval=DEFAULT_POLL_INTERVAL,
):
    """Context manager that acquires a PostgreSQL advisory lock for migrations.

    Only one pod can hold this lock at a time. Other pods wait up to
    timeout_seconds before raising MigrationLockTimeout.

    Args:
        db_url: SQLAlchemy database URL string.
        lock_id: Integer advisory lock ID (default 900100).
        timeout_seconds: Maximum seconds to wait for lock acquisition.
        poll_interval: Seconds between lock acquisition attempts.

    Raises:
        MigrationLockTimeout: If the lock cannot be acquired within timeout.
        MigrationLockError: If lock operations fail unexpectedly.
    """
    engine = sqlalchemy.create_engine(db_url, poolclass=sqlalchemy.pool.NullPool)
    connection = engine.connect()
    acquired = False

    try:
        acquired = _acquire_lock(connection, lock_id, timeout_seconds, poll_interval)
        yield connection
    finally:
        if acquired:
            _release_lock(connection, lock_id)
        connection.close()
        engine.dispose()


def _acquire_lock(connection, lock_id, timeout_seconds, poll_interval):
    """Attempt to acquire advisory lock with polling and timeout."""
    start_time = time.time()
    attempts = 0

    while True:
        attempts += 1
        elapsed = time.time() - start_time

        result = connection.execute(
            sqlalchemy.text("SELECT pg_try_advisory_lock(:lock_id)"),
            {"lock_id": lock_id},
        ).scalar()

        if result:
            log.info(
                "Migration lock %d acquired after %.1fs (%d attempts)",
                lock_id, elapsed, attempts,
            )
            return True

        if elapsed + poll_interval > timeout_seconds:
            raise MigrationLockTimeout(
                f"Could not acquire migration lock {lock_id} within "
                f"{timeout_seconds}s ({attempts} attempts, {elapsed:.1f}s elapsed)"
            )

        log.info(
            "Migration lock %d held by another process, retrying in %.1fs "
            "(attempt %d, %.1fs elapsed)",
            lock_id, poll_interval, attempts, elapsed,
        )
        time.sleep(poll_interval)


def _release_lock(connection, lock_id):
    """Release advisory lock explicitly."""
    try:
        result = connection.execute(
            sqlalchemy.text("SELECT pg_advisory_unlock(:lock_id)"),
            {"lock_id": lock_id},
        ).scalar()

        if result:
            log.info("Migration lock %d released", lock_id)
        else:
            log.warning(
                "Migration lock %d release returned False (lock not held?)", lock_id
            )
    except Exception as exc:
        log.error("Failed to release migration lock %d: %s", lock_id, exc)


def run_migrations_with_lock(
    module,
    db_url,
    lock_id=DEFAULT_LOCK_ID,
    timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    poll_interval=DEFAULT_POLL_INTERVAL,
    **migration_kwargs,
):
    """Run database migrations while holding an advisory lock.

    This is the primary integration point. It wraps the standard
    db_migrations.run_db_migrations call with advisory lock protection.

    Args:
        module: Pylon module descriptor (passed to run_db_migrations).
        db_url: SQLAlchemy database URL string.
        lock_id: Integer advisory lock ID.
        timeout_seconds: Max seconds to wait for lock.
        poll_interval: Seconds between attempts.
        **migration_kwargs: Additional kwargs passed to run_db_migrations
            (payload, migrations_path, version_table, revision).

    Returns:
        True if migrations ran successfully.

    Raises:
        MigrationLockTimeout: If lock cannot be acquired.
    """
    log.info(
        "Attempting to acquire migration lock %d for %s",
        lock_id, getattr(getattr(module, 'descriptor', None), 'name', str(module)),
    )

    with migration_lock(db_url, lock_id, timeout_seconds, poll_interval):
        from tools import db_migrations  # noqa: E0401
        db_migrations.run_db_migrations(module, db_url, **migration_kwargs)

    return True
