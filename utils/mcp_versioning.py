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


def apply_instructions_patch_batch(current: str | None, *, expected_sha256: str, patches) -> str:
    """Apply several edits in order, all or nothing.

    Only the first patch is checked against ``expected_sha256``; the rest are chained against the
    text the previous one produced. A caller cannot supply per-patch hashes, by design — the batch
    is a set of proposals made against one starting text, and letting each item carry its own hash
    would allow reads from different versions to be mixed into a single apply.

    Failures name the offending index. Without it, "old_text must match exactly once" is untraceable
    once several edits are in flight, and the likely cause — an earlier patch having consumed the
    text a later one anchors to — is invisible.
    """
    if not patches:
        raise InstructionsPatchConflictError('No patches to apply.')

    updated = current or ''
    for index, patch in enumerate(patches):
        try:
            updated = apply_instructions_patch(
                updated,
                expected_sha256=expected_sha256 if index == 0 else instructions_sha256(updated),
                old_text=patch.get('old_text'),
                replacement=patch.get('replacement', ''),
                replace_all=patch.get('replace_all', False),
            )
        except InstructionsPatchConflictError as exc:
            raise InstructionsPatchConflictError(f'Patch {index}: {exc}') from exc
    return updated


def _build_backup_version_name(prefix: str, version_id: int, now: datetime | None) -> str:
    """Compact, collision-resistant version name within the 128-char DB limit.

    Microsecond precision because two backups of the same version within one second is ordinary
    when edits are applied one item at a time.
    """
    timestamp = (now or datetime.now(timezone.utc)).strftime('%Y%m%dT%H%M%S%fZ')
    return f'{prefix}-{version_id}-{timestamp}'


def build_mcp_backup_version_name(version_id: int, now: datetime | None = None) -> str:
    return _build_backup_version_name('mcp-backup', version_id, now)


def build_enhance_backup_version_name(version_id: int, now: datetime | None = None) -> str:
    """Backup name for an in-place "Enhance with AI" apply.

    Distinct prefix from the MCP backups so the version list distinguishes "an agent edited this
    through a tool call" from "a human accepted an AI enhancement proposal" — the two have very
    different review implications when someone later asks why the instructions changed.
    """
    return _build_backup_version_name('enhance-backup', version_id, now)


def build_enhance_fork_version_name(version_id: int, now: datetime | None = None) -> str:
    """Default name for the fork target of an "Enhance with AI" apply.

    Fork is the default apply mode (§6/§6.1.1): it leaves the evaluated version untouched so the
    same run can be re-run against the original for comparison. The name is a default the user can
    override, not an internal artifact, so it reads as a version rather than as a backup.
    """
    return _build_backup_version_name('enhanced', version_id, now)
