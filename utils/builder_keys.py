"""Elitea Builder capability key (#7383).

Pure helpers, no pylon imports. The old builder keys are not accepted anywhere at run time; they
exist here only so the one-off migration can rewrite stored rows.
"""

BUILDER_KEY = 'elitea_builder'
LEGACY_BUILDER_KEYS = ('internal_mcp', 'skill_builder', 'project_context_builder')
_MIGRATED_KEYS = frozenset((BUILDER_KEY, *LEGACY_BUILDER_KEYS))


def is_builder_enabled(internal_tools) -> bool:
    return BUILDER_KEY in (internal_tools or [])


def migrate_builder_keys(internal_tools) -> list:
    """Replace old builder keys with BUILDER_KEY at the first builder position, keeping order."""
    result = []
    builder_added = False
    for key in internal_tools or []:
        if key in _MIGRATED_KEYS:
            if not builder_added:
                result.append(BUILDER_KEY)
                builder_added = True
            continue
        result.append(key)
    return result


def migrate_meta(meta):
    """Return (meta, changed). A copy is returned only when internal_tools changes."""
    if not meta or not isinstance(meta.get('internal_tools'), list):
        return meta, False
    migrated = migrate_builder_keys(meta['internal_tools'])
    if migrated == meta['internal_tools']:
        return meta, False
    return {**meta, 'internal_tools': migrated}, True


def migrate_rows(rows, dry_run: bool, flag_modified=None) -> int:
    """Rewrite `meta` of ORM rows in place; returns how many rows needed it."""
    updated = 0
    for row in rows:
        new_meta, changed = migrate_meta(row.meta)
        if not changed:
            continue
        updated += 1
        if not dry_run:
            row.meta = new_meta
            if flag_modified:
                flag_modified(row, 'meta')
    return updated


def run_builder_keys_migration(project_ids, migrate_project, migrate_settings, dry_run: bool) -> dict:
    """Per project: versions + conversations (`migrate_project`) and module settings (`migrate_settings`).
    One failing project is listed and the run goes on."""
    result = {
        "updated_versions": 0, "updated_conversations": 0, "updated_settings": 0,
        "failed_projects": [], "dry_run": dry_run,
    }
    for pid in project_ids:
        try:
            versions, conversations = migrate_project(pid, dry_run)
            settings = migrate_settings(pid, dry_run)
        except Exception:  # pylint: disable=W0703
            result["failed_projects"].append(pid)
            continue
        result["updated_versions"] += versions
        result["updated_conversations"] += conversations
        result["updated_settings"] += settings
    return result


def run_enable_default_modules(project_ids, enable_project, enable_legacy, dry_run: bool) -> dict:
    """R-2.0.9: the Elitea Builder defaults on, Ask User on for Chats and off for Agents, per project
    (`enable_project`), then once in the legacy personalization (`enable_legacy`). One failing project is listed and the run goes on."""
    result = {"updated_settings": 0, "updated_legacy_users": 0, "failed_projects": [], "legacy_failed": False,
              "dry_run": dry_run}
    for pid in project_ids:
        try:
            result["updated_settings"] += enable_project(pid, dry_run)
        except Exception:  # pylint: disable=W0703
            result["failed_projects"].append(pid)
    try:
        result["updated_legacy_users"] = enable_legacy(dry_run)
    except Exception:  # pylint: disable=W0703
        result["legacy_failed"] = True
    return result
