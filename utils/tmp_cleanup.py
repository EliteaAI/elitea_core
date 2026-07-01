"""
Periodic /tmp cleanup for horizontal scaling.

In a multi-replica deployment with emptyDir volumes, /tmp can accumulate stale
files from crashed tasks, abandoned uploads, or leaked tempfiles. This module
provides a background thread that periodically removes files older than a
configurable threshold.

Files that cannot be removed (open handles, permission errors) are skipped
gracefully. The cleanup only targets regular files — directories are left alone
unless they become empty after their contents are cleaned.

Configuration via constructor or environment variables:
  TMP_CLEANUP_PATH       — base directory to clean (default: /tmp)
  TMP_CLEANUP_MAX_AGE    — max file age in seconds (default: 3600 = 1 hour)
  TMP_CLEANUP_INTERVAL   — seconds between cleanup runs (default: 1800 = 30 min)
"""

import os
import threading
import time

from pylon.core.tools import log


DEFAULT_PATH = "/tmp"
DEFAULT_MAX_AGE = 3600
DEFAULT_INTERVAL = 1800


class TmpCleanup:
    """Background thread that periodically removes stale files from /tmp."""

    def __init__(self, path: str = None, max_age: int = None,
                 interval: int = None):
        """
        Args:
            path: Directory to clean (default: /tmp or TMP_CLEANUP_PATH env).
            max_age: Maximum file age in seconds before removal (default: 3600).
            interval: Seconds between cleanup runs (default: 1800).
        """
        self._path = path or os.environ.get("TMP_CLEANUP_PATH", DEFAULT_PATH)
        self._max_age = max_age if max_age is not None else int(
            os.environ.get("TMP_CLEANUP_MAX_AGE", DEFAULT_MAX_AGE)
        )
        self._interval = interval if interval is not None else int(
            os.environ.get("TMP_CLEANUP_INTERVAL", DEFAULT_INTERVAL)
        )
        self._running = False
        self._thread = None
        self._stop_event = threading.Event()

    @property
    def path(self) -> str:
        return self._path

    @property
    def max_age(self) -> int:
        return self._max_age

    @property
    def interval(self) -> int:
        return self._interval

    @property
    def is_running(self) -> bool:
        return self._running

    def start(self):
        """Start the background cleanup thread."""
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="tmp_cleanup",
            daemon=True,
        )
        self._thread.start()
        log.info(
            "TmpCleanup started: path=%s max_age=%ds interval=%ds",
            self._path, self._max_age, self._interval,
        )

    def stop(self):
        """Stop the background cleanup thread."""
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
        self._thread = None
        log.info("TmpCleanup stopped")

    def run_once(self) -> dict:
        """Run a single cleanup pass and return summary.

        Returns:
            dict with keys: files_deleted, bytes_reclaimed, files_skipped, errors
        """
        return self._cleanup(time.time())

    def _run_loop(self):
        """Background loop: sleep then clean."""
        while self._running:
            if self._stop_event.wait(timeout=self._interval):
                break
            try:
                summary = self._cleanup(time.time())
                if summary["files_deleted"] > 0 or summary["errors"] > 0:
                    log.info(
                        "TmpCleanup: deleted=%d reclaimed=%s skipped=%d errors=%d",
                        summary["files_deleted"],
                        _human_size(summary["bytes_reclaimed"]),
                        summary["files_skipped"],
                        summary["errors"],
                    )
            except Exception:
                log.exception("TmpCleanup: unexpected error in cleanup loop")

    def _cleanup(self, now: float) -> dict:
        """Walk the target directory and remove stale files.

        Args:
            now: Current timestamp (for testability).

        Returns:
            Summary dict.
        """
        summary = {
            "files_deleted": 0,
            "bytes_reclaimed": 0,
            "files_skipped": 0,
            "errors": 0,
        }

        if not os.path.isdir(self._path):
            return summary

        cutoff = now - self._max_age
        self._walk_and_clean(self._path, cutoff, summary)
        return summary

    def _walk_and_clean(self, dirpath: str, cutoff: float, summary: dict):
        """Recursively walk directory and remove stale files."""
        try:
            entries = list(os.scandir(dirpath))
        except PermissionError:
            summary["errors"] += 1
            return

        for entry in entries:
            try:
                if entry.is_symlink():
                    self._try_remove_file(entry, cutoff, summary)
                elif entry.is_file(follow_symlinks=False):
                    self._try_remove_file(entry, cutoff, summary)
                elif entry.is_dir(follow_symlinks=False):
                    self._walk_and_clean(entry.path, cutoff, summary)
                    self._try_remove_empty_dir(entry.path)
            except OSError:
                summary["errors"] += 1

    def _try_remove_file(self, entry, cutoff: float, summary: dict):
        """Remove a file if it's older than the cutoff time."""
        try:
            stat = entry.stat(follow_symlinks=False)
        except OSError:
            summary["errors"] += 1
            return

        mtime = stat.st_mtime
        if mtime >= cutoff:
            summary["files_skipped"] += 1
            return

        try:
            os.unlink(entry.path)
            summary["files_deleted"] += 1
            summary["bytes_reclaimed"] += stat.st_size
        except OSError:
            summary["files_skipped"] += 1

    def _try_remove_empty_dir(self, dirpath: str):
        """Remove directory if it became empty after cleaning. Best-effort."""
        try:
            os.rmdir(dirpath)
        except OSError:
            pass


def _human_size(size_bytes: int) -> str:
    """Convert bytes to human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f}KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f}MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f}GB"
