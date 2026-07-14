"""Redis backup to S3-compatible storage for disaster recovery.

Provides scheduled backup of Redis RDB snapshots to S3 and restore capability.
Integrates with the leader election system so only one pod performs backups.

Usage:
    from elitea_core.utils.redis_backup import RedisBackupManager

    manager = RedisBackupManager(
        redis_client=redis_client,
        s3_client=s3_client,
        bucket="elitea-backups",
        prefix="redis/",
    )

    # Trigger backup (typically called by distributed cron)
    result = manager.backup()
    # {"timestamp": "2026-06-30T12:00:00Z", "key": "redis/20260630T120000Z.rdb", "size_bytes": 1234}

    # List available backups
    backups = manager.list_backups(limit=10)

    # Restore from a specific backup
    manager.restore(timestamp="2026-06-30T12:00:00Z")
"""

import io
import time
from datetime import datetime, timezone
from typing import Optional

from pylon.core.tools import log


DEFAULT_BUCKET = "elitea-backups"
DEFAULT_PREFIX = "redis/"
BGSAVE_POLL_INTERVAL = 1.0
BGSAVE_MAX_WAIT = 300
BACKUP_RETENTION_DAYS = 7
MAX_BACKUP_LIST = 100


class RedisBackupError(Exception):
    """Base exception for Redis backup operations."""
    pass


class BackupTimeoutError(RedisBackupError):
    """Raised when BGSAVE does not complete within the timeout."""
    pass


class RestoreError(RedisBackupError):
    """Raised when restore operation fails."""
    pass


class RedisBackupManager:
    """Manages Redis RDB backup to and restore from S3-compatible storage.

    The backup flow:
    1. Record current lastsave timestamp
    2. Trigger BGSAVE
    3. Poll lastsave until it advances (or timeout)
    4. Read dump.rdb from Redis data directory via CONFIG GET dir
    5. Upload RDB file to S3 with timestamped key

    The restore flow:
    1. Download RDB from S3
    2. Write to Redis data directory
    3. Redis must be restarted to pick up the restored dump
    """

    def __init__(
        self,
        redis_client,
        s3_client,
        bucket: str = DEFAULT_BUCKET,
        prefix: str = DEFAULT_PREFIX,
        bgsave_timeout: float = BGSAVE_MAX_WAIT,
        poll_interval: float = BGSAVE_POLL_INTERVAL,
    ):
        """
        Args:
            redis_client: Redis client instance (must support bgsave, lastsave, config_get).
            s3_client: boto3 S3 client (or compatible, e.g. MinIO).
            bucket: S3 bucket name for backups.
            prefix: Key prefix within the bucket (e.g. "redis/").
            bgsave_timeout: Max seconds to wait for BGSAVE completion.
            poll_interval: Seconds between lastsave polls.
        """
        if redis_client is None:
            raise ValueError("redis_client must not be None")
        if s3_client is None:
            raise ValueError("s3_client must not be None")
        if not bucket:
            raise ValueError("bucket must be non-empty")

        self._redis = redis_client
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix.rstrip("/") + "/" if prefix else ""
        self._bgsave_timeout = bgsave_timeout
        self._poll_interval = poll_interval

    @property
    def bucket(self) -> str:
        return self._bucket

    @property
    def prefix(self) -> str:
        return self._prefix

    def backup(self) -> dict:
        """Trigger BGSAVE and upload the RDB snapshot to S3.

        Returns:
            dict with keys: timestamp (ISO), key (S3 key), size_bytes (int).

        Raises:
            BackupTimeoutError: If BGSAVE doesn't complete within timeout.
            RedisBackupError: If upload or Redis command fails.
        """
        log.info("Redis backup started: bucket=%s, prefix=%s", self._bucket, self._prefix)

        try:
            initial_lastsave = self._redis.lastsave()
        except Exception as e:
            raise RedisBackupError(f"Failed to get initial lastsave: {e}") from e

        try:
            self._redis.bgsave()
        except Exception as e:
            error_msg = str(e).lower()
            if "already in progress" not in error_msg:
                raise RedisBackupError(f"BGSAVE command failed: {e}") from e
            log.info("BGSAVE already in progress, waiting for completion")

        new_lastsave = self._wait_for_bgsave(initial_lastsave)
        timestamp = datetime.fromtimestamp(new_lastsave, tz=timezone.utc)
        timestamp_str = timestamp.strftime("%Y%m%dT%H%M%SZ")

        rdb_data = self._read_rdb_dump()

        s3_key = f"{self._prefix}{timestamp_str}.rdb"
        try:
            self._s3.put_object(
                Bucket=self._bucket,
                Key=s3_key,
                Body=rdb_data,
                ContentType="application/octet-stream",
            )
        except Exception as e:
            raise RedisBackupError(f"Failed to upload RDB to S3: {e}") from e

        size_bytes = len(rdb_data)
        log.info(
            "Redis backup complete: key=%s, size=%d bytes, timestamp=%s",
            s3_key, size_bytes, timestamp.isoformat(),
        )

        return {
            "timestamp": timestamp.isoformat(),
            "key": s3_key,
            "size_bytes": size_bytes,
        }

    def restore(self, timestamp: str) -> dict:
        """Download an RDB snapshot from S3 for restore.

        This downloads the RDB file and returns its contents. The actual restore
        requires placing the file in Redis data directory and restarting Redis,
        which is an operational procedure (not automated to avoid accidental data loss).

        Args:
            timestamp: ISO timestamp string or the key suffix (e.g. "20260630T120000Z").

        Returns:
            dict with keys: key (S3 key), size_bytes (int), data (bytes).

        Raises:
            RestoreError: If download fails or backup not found.
        """
        s3_key = self._resolve_backup_key(timestamp)

        try:
            response = self._s3.get_object(Bucket=self._bucket, Key=s3_key)
            rdb_data = response["Body"].read()
        except Exception as e:
            raise RestoreError(f"Failed to download backup {s3_key}: {e}") from e

        log.info("Redis backup downloaded: key=%s, size=%d bytes", s3_key, len(rdb_data))

        return {
            "key": s3_key,
            "size_bytes": len(rdb_data),
            "data": rdb_data,
        }

    def list_backups(self, limit: int = MAX_BACKUP_LIST) -> list:
        """List available backups in S3, sorted newest first.

        Args:
            limit: Maximum number of backups to return.

        Returns:
            List of dicts with keys: key, timestamp (ISO), size_bytes.
        """
        try:
            response = self._s3.list_objects_v2(
                Bucket=self._bucket,
                Prefix=self._prefix,
                MaxKeys=min(limit, 1000),
            )
        except Exception as e:
            log.error("Failed to list backups: %s", e)
            return []

        contents = response.get("Contents", [])
        backups = []
        for obj in contents:
            key = obj.get("Key", "")
            if not key.endswith(".rdb"):
                continue
            backups.append({
                "key": key,
                "size_bytes": obj.get("Size", 0),
                "last_modified": obj.get("LastModified"),
            })

        backups.sort(key=lambda b: b["key"], reverse=True)
        return backups[:limit]

    def delete_old_backups(self, retention_days: int = BACKUP_RETENTION_DAYS) -> int:
        """Delete backups older than retention_days.

        Args:
            retention_days: Number of days to retain backups.

        Returns:
            Number of backups deleted.
        """
        cutoff = datetime.now(tz=timezone.utc).timestamp() - (retention_days * 86400)
        backups = self.list_backups(limit=1000)

        deleted = 0
        for backup in backups:
            last_modified = backup.get("last_modified")
            if last_modified is None:
                continue

            if hasattr(last_modified, "timestamp"):
                mod_ts = last_modified.timestamp()
            else:
                mod_ts = float(last_modified)

            if mod_ts < cutoff:
                try:
                    self._s3.delete_object(Bucket=self._bucket, Key=backup["key"])
                    deleted += 1
                    log.info("Deleted old backup: %s", backup["key"])
                except Exception as e:
                    log.warning("Failed to delete backup %s: %s", backup["key"], e)

        if deleted:
            log.info("Deleted %d old Redis backups (retention=%d days)", deleted, retention_days)

        return deleted

    def _wait_for_bgsave(self, initial_lastsave) -> int:
        """Poll lastsave() until it advances beyond initial_lastsave."""
        elapsed = 0.0
        while elapsed < self._bgsave_timeout:
            time.sleep(self._poll_interval)
            elapsed += self._poll_interval
            try:
                current = self._redis.lastsave()
            except Exception as e:
                log.warning("Error polling lastsave: %s", e)
                continue
            if self._lastsave_advanced(initial_lastsave, current):
                return self._lastsave_to_epoch(current)
        raise BackupTimeoutError(
            f"BGSAVE did not complete within {self._bgsave_timeout}s"
        )

    def _lastsave_advanced(self, initial, current) -> bool:
        """Check if lastsave timestamp has advanced."""
        return self._lastsave_to_epoch(current) > self._lastsave_to_epoch(initial)

    def _lastsave_to_epoch(self, value) -> int:
        """Convert lastsave return value to epoch seconds.

        Redis lastsave() may return a datetime object or an integer depending
        on the client configuration.
        """
        if hasattr(value, "timestamp"):
            return int(value.timestamp())
        return int(value)

    def _read_rdb_dump(self) -> bytes:
        """Read the RDB dump file from Redis.

        Uses the SYNC-based dump approach: reads from the Redis data directory.
        In containerized environments, this requires the data volume to be
        accessible, or alternatively uses Redis DEBUG RELOAD patterns.

        For simplicity and broad compatibility, we use the CONFIG GET dir
        approach with a file read.
        """
        try:
            config_dir = self._redis.config_get("dir")
            data_dir = config_dir.get("dir", "/data")
        except Exception:
            data_dir = "/data"

        try:
            config_dbfilename = self._redis.config_get("dbfilename")
            db_filename = config_dbfilename.get("dbfilename", "dump.rdb")
        except Exception:
            db_filename = "dump.rdb"

        rdb_path = f"{data_dir}/{db_filename}"

        try:
            with open(rdb_path, "rb") as f:
                return f.read()
        except FileNotFoundError:
            raise RedisBackupError(
                f"RDB file not found at {rdb_path}. "
                "Ensure Redis persistence is enabled and the data volume is accessible."
            )
        except PermissionError:
            raise RedisBackupError(
                f"Permission denied reading {rdb_path}. "
                "Ensure the backup process has read access to the Redis data volume."
            )
        except Exception as e:
            raise RedisBackupError(f"Failed to read RDB file at {rdb_path}: {e}") from e

    def _resolve_backup_key(self, timestamp: str) -> str:
        """Resolve a timestamp or partial key to a full S3 key."""
        if timestamp.startswith(self._prefix):
            return timestamp
        if timestamp.endswith(".rdb"):
            return f"{self._prefix}{timestamp}"

        cleaned = timestamp.replace("-", "").replace(":", "").replace(" ", "T")
        if not cleaned.endswith("Z"):
            cleaned += "Z"
        if not cleaned.endswith(".rdb"):
            cleaned += ".rdb"
        return f"{self._prefix}{cleaned}"
