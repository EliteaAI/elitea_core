"""Safety primitives for effectful internal-MCP version edits."""

from datetime import datetime, timezone
from hashlib import sha256


INTERNAL_MCP_ENVIRON_KEY = 'elitea.internal_mcp_request'


class InstructionsPatchConflictError(ValueError):
    """The requested patch was built from stale or ambiguous instructions."""


def instructions_sha256(instructions: str | None) -> str:
    """Return the stable content fingerprint exposed by the read-before-write API."""
    return sha256((instructions or '').encode('utf-8')).hexdigest()


def apply_instructions_patch(
    current: str | None,
    *,
    expected_sha256: str,
    replacement: str,
    old_text: str | None = None,
    replace_all: bool = False,
) -> str:
    """Apply a concurrency-safe full or single-exact-match instructions change."""
    current = current or ''
    actual_sha256 = instructions_sha256(current)
    if expected_sha256.lower() != actual_sha256:
        raise InstructionsPatchConflictError(
            'Instructions changed after they were read. Read the version again and retry.'
        )

    if replace_all:
        updated = replacement
    else:
        if not old_text:
            raise InstructionsPatchConflictError(
                'old_text is required unless replace_all is true.'
            )
        occurrences = current.count(old_text)
        if occurrences != 1:
            raise InstructionsPatchConflictError(
                f'old_text must match exactly once; found {occurrences} matches.'
            )
        updated = current.replace(old_text, replacement, 1)

    if not updated.strip():
        raise InstructionsPatchConflictError(
            'Refusing to replace non-empty instructions with empty content.'
        )
    if updated == current:
        raise InstructionsPatchConflictError('The patch would not change the instructions.')
    return updated


def build_mcp_backup_version_name(version_id: int, now: datetime | None = None) -> str:
    """Build a compact, collision-resistant version name within the 128-char DB limit."""
    timestamp = (now or datetime.now(timezone.utc)).strftime('%Y%m%dT%H%M%S%fZ')
    return f'mcp-backup-{version_id}-{timestamp}'
