"""
Utility for claiming database rows using SELECT ... FOR UPDATE SKIP LOCKED.

When multiple pods run the same polling loop (e.g., scheduled tasks, work queues),
SKIP LOCKED ensures each row is processed by exactly one pod — other pods skip
already-locked rows instead of blocking.

Usage:
    from .skip_locked import claim_rows

    with session.begin():
        rows = claim_rows(session, Schedule, Schedule.active == True, limit=10)
        for row in rows:
            row.run()
            row.last_run = datetime.now()
"""

from sqlalchemy.orm import Session, Query


def claim_rows(session, model, *filters, limit=None, order_by=None):
    """
    Claim rows using SELECT ... FOR UPDATE SKIP LOCKED.

    Selects rows matching the given filters and locks them for the duration
    of the current transaction. Rows already locked by another session are
    silently skipped.

    Args:
        session: SQLAlchemy session (must be within a transaction).
        model: SQLAlchemy model class to query.
        *filters: SQLAlchemy filter expressions (e.g., Model.active == True).
        limit: Maximum number of rows to claim. None means no limit.
        order_by: Column or expression to order by before claiming.
                  Useful for deterministic claiming (e.g., by id or priority).

    Returns:
        List of model instances that were successfully locked.
        Empty list if no rows are available (all locked or none match).
    """
    query = session.query(model)

    for f in filters:
        query = query.filter(f)

    if order_by is not None:
        query = query.order_by(order_by)

    if limit is not None:
        query = query.limit(limit)

    query = query.with_for_update(skip_locked=True)

    return query.all()


def claim_one(session, model, *filters, order_by=None):
    """
    Claim a single row using SELECT ... FOR UPDATE SKIP LOCKED.

    Args:
        session: SQLAlchemy session (must be within a transaction).
        model: SQLAlchemy model class to query.
        *filters: SQLAlchemy filter expressions.
        order_by: Column or expression to order by before claiming.

    Returns:
        A single model instance if one was successfully locked, or None.
    """
    query = session.query(model)

    for f in filters:
        query = query.filter(f)

    if order_by is not None:
        query = query.order_by(order_by)

    query = query.limit(1).with_for_update(skip_locked=True)

    return query.first()


def build_skip_locked_query(session, model, *filters, limit=None, order_by=None):
    """
    Build a query with FOR UPDATE SKIP LOCKED without executing it.

    Useful when the caller needs to further customize the query or
    inspect it before execution.

    Args:
        session: SQLAlchemy session.
        model: SQLAlchemy model class.
        *filters: SQLAlchemy filter expressions.
        limit: Maximum rows.
        order_by: Ordering expression.

    Returns:
        SQLAlchemy Query object with FOR UPDATE SKIP LOCKED applied.
    """
    query = session.query(model)

    for f in filters:
        query = query.filter(f)

    if order_by is not None:
        query = query.order_by(order_by)

    if limit is not None:
        query = query.limit(limit)

    query = query.with_for_update(skip_locked=True)

    return query
