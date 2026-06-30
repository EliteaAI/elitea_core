"""Audit logging for security-relevant actions.

Records user actions (login/logout, permission changes, data export, admin actions)
to an append-only PostgreSQL table for compliance and forensic analysis.

The audit_log table is append-only: rows are only INSERTed, never UPDATEd or DELETEd
by application code. Retention is handled by a periodic cleanup that archives old
entries to S3 before deletion.

Usage:
    from elitea_core.utils.audit_logger import AuditLogger

    logger = AuditLogger(db_url="postgresql://...", s3_client=s3_client)

    # Log an action
    logger.log(
        actor="user@example.com",
        action="user.login",
        resource="auth/session",
        details={"method": "oidc", "provider": "keycloak"},
        ip_address="10.0.0.1",
    )

    # Query audit log
    entries = logger.query(actor="user@example.com", limit=50)

    # Run retention (archive old entries to S3, then delete)
    logger.run_retention(retention_days=90)
"""

import json
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import sqlalchemy
from sqlalchemy import (
    Column, DateTime, Index, Integer, String, Text,
    Table, MetaData, text, and_, desc,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.pool import NullPool

from pylon.core.tools import log


AUDIT_SCHEMA = "centry"
AUDIT_TABLE_NAME = "audit_log"
DEFAULT_RETENTION_DAYS = 90
DEFAULT_S3_BUCKET = "elitea-backups"
DEFAULT_S3_PREFIX = "audit/"
ARCHIVE_BATCH_SIZE = 1000

AUDIT_ACTIONS = (
    "user.login",
    "user.logout",
    "user.login_failed",
    "permission.grant",
    "permission.revoke",
    "permission.change",
    "data.export",
    "data.import",
    "data.delete",
    "admin.user_create",
    "admin.user_delete",
    "admin.user_update",
    "admin.project_create",
    "admin.project_delete",
    "admin.settings_change",
    "admin.secret_rotate",
    "api_key.create",
    "api_key.revoke",
    "session.invalidate",
)


metadata = MetaData(schema=AUDIT_SCHEMA)

audit_log_table = Table(
    AUDIT_TABLE_NAME,
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("event_id", String(36), nullable=False, unique=True),
    Column("timestamp", DateTime(timezone=True), nullable=False, index=True),
    Column("actor", String(255), nullable=False, index=True),
    Column("action", String(100), nullable=False, index=True),
    Column("resource", String(500), nullable=False),
    Column("details", JSONB, nullable=True),
    Column("ip_address", String(45), nullable=True),
    Column("service", String(100), nullable=True),
    Column("request_id", String(64), nullable=True),
    Index("ix_audit_log_actor_timestamp", "actor", "timestamp"),
    Index("ix_audit_log_action_timestamp", "action", "timestamp"),
)


class AuditLogError(Exception):
    """Base exception for audit logging operations."""
    pass


class AuditRetentionError(AuditLogError):
    """Raised when retention archive/purge fails."""
    pass


class AuditLogger:
    """Append-only audit logger backed by PostgreSQL.

    Stores security-relevant events in a dedicated table with structured
    fields for actor, action, resource, and details. Supports retention
    management with S3 archival.
    """

    def __init__(
        self,
        db_url: str,
        s3_client=None,
        s3_bucket: str = DEFAULT_S3_BUCKET,
        s3_prefix: str = DEFAULT_S3_PREFIX,
        service_name: Optional[str] = None,
    ):
        """
        Args:
            db_url: SQLAlchemy database URL for PostgreSQL.
            s3_client: Optional boto3-compatible S3 client for archival.
            s3_bucket: S3 bucket for archived audit entries.
            s3_prefix: Key prefix for archived entries.
            service_name: Name of the service emitting events (e.g. "pylon_main").
        """
        if not db_url:
            raise ValueError("db_url must be non-empty")

        self._engine = sqlalchemy.create_engine(db_url, poolclass=NullPool)
        self._s3 = s3_client
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix.rstrip("/") + "/" if s3_prefix else ""
        self._service_name = service_name

    def ensure_table(self):
        """Create the audit_log table if it does not exist.

        Safe to call multiple times (idempotent via IF NOT EXISTS).
        """
        metadata.create_all(self._engine, checkfirst=True)

    def log(
        self,
        actor: str,
        action: str,
        resource: str,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        request_id: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> str:
        """Record an audit event.

        Args:
            actor: Who performed the action (email, user ID, or "system").
            action: What was done (e.g. "user.login", "permission.grant").
            resource: What was affected (e.g. "project/42", "user/john@co.com").
            details: Additional context as a JSON-serializable dict.
            ip_address: Client IP address (IPv4 or IPv6).
            request_id: Correlation ID for tracing.
            timestamp: Override event time (defaults to now UTC).

        Returns:
            The unique event_id for this audit entry.

        Raises:
            AuditLogError: If the insert fails.
        """
        if not actor:
            raise ValueError("actor must be non-empty")
        if not action:
            raise ValueError("action must be non-empty")
        if not resource:
            raise ValueError("resource must be non-empty")

        event_id = str(uuid.uuid4())
        ts = timestamp or datetime.now(timezone.utc)

        try:
            with self._engine.connect() as conn:
                conn.execute(
                    audit_log_table.insert().values(
                        event_id=event_id,
                        timestamp=ts,
                        actor=actor,
                        action=action,
                        resource=resource,
                        details=details,
                        ip_address=ip_address,
                        service=self._service_name,
                        request_id=request_id,
                    )
                )
                conn.commit()
        except Exception as e:
            raise AuditLogError(f"Failed to insert audit event: {e}") from e

        return event_id

    def query(
        self,
        actor: Optional[str] = None,
        action: Optional[str] = None,
        resource: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Query audit log entries with optional filters.

        Args:
            actor: Filter by actor (exact match).
            action: Filter by action (exact match or prefix with wildcard).
            resource: Filter by resource (exact match).
            since: Only entries at or after this timestamp.
            until: Only entries before this timestamp.
            limit: Maximum number of entries to return.
            offset: Number of entries to skip (for pagination).

        Returns:
            List of audit entry dicts ordered by timestamp descending.
        """
        conditions = []
        if actor:
            conditions.append(audit_log_table.c.actor == actor)
        if action:
            if action.endswith(".*"):
                conditions.append(
                    audit_log_table.c.action.like(action[:-1] + "%")
                )
            else:
                conditions.append(audit_log_table.c.action == action)
        if resource:
            conditions.append(audit_log_table.c.resource == resource)
        if since:
            conditions.append(audit_log_table.c.timestamp >= since)
        if until:
            conditions.append(audit_log_table.c.timestamp < until)

        query = (
            audit_log_table.select()
            .order_by(desc(audit_log_table.c.timestamp))
            .limit(limit)
            .offset(offset)
        )
        if conditions:
            query = query.where(and_(*conditions))

        try:
            with self._engine.connect() as conn:
                result = conn.execute(query)
                rows = result.fetchall()
        except Exception as e:
            raise AuditLogError(f"Failed to query audit log: {e}") from e

        return [self._row_to_dict(row) for row in rows]

    def count(
        self,
        actor: Optional[str] = None,
        action: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> int:
        """Count audit log entries matching the given filters.

        Args:
            actor: Filter by actor.
            action: Filter by action.
            since: Only entries at or after this timestamp.
            until: Only entries before this timestamp.

        Returns:
            Number of matching entries.
        """
        conditions = []
        if actor:
            conditions.append(audit_log_table.c.actor == actor)
        if action:
            if action.endswith(".*"):
                conditions.append(
                    audit_log_table.c.action.like(action[:-1] + "%")
                )
            else:
                conditions.append(audit_log_table.c.action == action)
        if since:
            conditions.append(audit_log_table.c.timestamp >= since)
        if until:
            conditions.append(audit_log_table.c.timestamp < until)

        count_query = sqlalchemy.select(
            sqlalchemy.func.count()
        ).select_from(audit_log_table)
        if conditions:
            count_query = count_query.where(and_(*conditions))

        try:
            with self._engine.connect() as conn:
                result = conn.execute(count_query)
                return result.scalar() or 0
        except Exception as e:
            raise AuditLogError(f"Failed to count audit entries: {e}") from e

    def run_retention(
        self,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        archive_to_s3: bool = True,
        batch_size: int = ARCHIVE_BATCH_SIZE,
    ) -> Dict[str, Any]:
        """Archive and purge audit entries older than retention_days.

        Flow:
        1. Select entries older than the cutoff date
        2. If archive_to_s3=True and s3_client configured, upload as JSONL to S3
        3. Delete archived entries from PostgreSQL

        Args:
            retention_days: Number of days to keep entries (default 90).
            archive_to_s3: Whether to upload to S3 before deletion.
            batch_size: Number of rows to process per batch.

        Returns:
            Dict with keys: archived_count, deleted_count, s3_key (if archived).

        Raises:
            AuditRetentionError: If archive or delete fails.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        result = {"archived_count": 0, "deleted_count": 0, "s3_key": None}

        try:
            with self._engine.connect() as conn:
                count_q = sqlalchemy.select(
                    sqlalchemy.func.count()
                ).select_from(audit_log_table).where(
                    audit_log_table.c.timestamp < cutoff
                )
                total = conn.execute(count_q).scalar() or 0

                if total == 0:
                    return result

                if archive_to_s3 and self._s3:
                    s3_key = self._archive_to_s3(conn, cutoff, batch_size)
                    result["s3_key"] = s3_key

                delete_stmt = audit_log_table.delete().where(
                    audit_log_table.c.timestamp < cutoff
                )
                del_result = conn.execute(delete_stmt)
                conn.commit()
                result["deleted_count"] = del_result.rowcount
                result["archived_count"] = total

        except AuditRetentionError:
            raise
        except Exception as e:
            raise AuditRetentionError(
                f"Retention cleanup failed: {e}"
            ) from e

        log.info(
            "Audit retention complete: archived=%d, deleted=%d, s3_key=%s",
            result["archived_count"],
            result["deleted_count"],
            result["s3_key"],
        )
        return result

    def _archive_to_s3(self, conn, cutoff: datetime, batch_size: int) -> str:
        """Upload expired entries as JSONL to S3 in batches.

        Processes rows in pages of batch_size to avoid loading all into RAM.
        Each batch is uploaded as a separate S3 object.

        Returns the S3 key prefix used for the archive.
        """
        timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        s3_prefix = f"{self._s3_prefix}audit_{timestamp_str}"
        part = 0
        last_id = None

        while True:
            select_q = (
                audit_log_table.select()
                .where(audit_log_table.c.timestamp < cutoff)
                .order_by(audit_log_table.c.id)
                .limit(batch_size)
            )
            if last_id is not None:
                select_q = select_q.where(audit_log_table.c.id > last_id)

            rows = conn.execute(select_q).fetchall()
            if not rows:
                break

            lines = []
            for row in rows:
                entry = self._row_to_dict(row)
                if entry.get("timestamp") and isinstance(entry["timestamp"], datetime):
                    entry["timestamp"] = entry["timestamp"].isoformat()
                lines.append(json.dumps(entry, default=str))
                last_id = row[0]  # id column

            body = "\n".join(lines) + "\n"
            s3_key = f"{s3_prefix}_part{part:04d}.jsonl"

            try:
                self._s3.put_object(
                    Bucket=self._s3_bucket,
                    Key=s3_key,
                    Body=body.encode("utf-8"),
                    ContentType="application/x-ndjson",
                )
            except Exception as e:
                raise AuditRetentionError(
                    f"Failed to upload archive batch {part} to S3: {e}"
                ) from e

            part += 1

        return f"{s3_prefix}_part*.jsonl"

    def get_entry(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a single audit entry by its event_id.

        Args:
            event_id: The unique identifier of the audit event.

        Returns:
            The audit entry dict, or None if not found.
        """
        if not event_id:
            return None

        query = audit_log_table.select().where(
            audit_log_table.c.event_id == event_id
        )
        try:
            with self._engine.connect() as conn:
                result = conn.execute(query)
                row = result.fetchone()
        except Exception as e:
            raise AuditLogError(f"Failed to get audit entry: {e}") from e

        if row is None:
            return None
        return self._row_to_dict(row)

    @staticmethod
    def _row_to_dict(row) -> Dict[str, Any]:
        """Convert a SQLAlchemy Row to a plain dict."""
        mapping = row._mapping if hasattr(row, "_mapping") else row
        return {
            "id": mapping["id"],
            "event_id": mapping["event_id"],
            "timestamp": mapping["timestamp"],
            "actor": mapping["actor"],
            "action": mapping["action"],
            "resource": mapping["resource"],
            "details": mapping["details"],
            "ip_address": mapping["ip_address"],
            "service": mapping["service"],
            "request_id": mapping["request_id"],
        }
