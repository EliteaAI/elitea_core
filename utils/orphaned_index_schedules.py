"""Find and remove index schedules whose index no longer exists (#7169).

An orphan is an ``indexes_meta.<name>`` entry on a toolkit whose own pgvector store has no
``index_meta`` row for ``<name>``. Index names are unique only inside one toolkit, so every
judgement here is per toolkit, against that toolkit's store.
"""

import copy
import json

from pylon.core.tools import log  # pylint: disable=E0611,E0401
from sqlalchemy import inspect, text
from sqlalchemy.orm.attributes import flag_modified


def parse_prune_params(param: str | None) -> tuple[int | None, int | None, bool]:
    """``project_id=<all|N>;toolkit_id=<N>;dry_run=false`` -> (project_id, toolkit_id, dry_run).

    Dry run unless ``dry_run=false`` is spelled out. Raises ValueError on a bad value.
    """
    params = {}
    for part in (param or "").split(";"):
        key, sep, value = part.partition("=")
        if sep:
            params[key.strip().lower()] = value.strip()
    project_id = params.get("project_id", "all")
    project_id = None if project_id.lower() in ("", "all") else int(project_id)
    toolkit_id = int(params["toolkit_id"]) if params.get("toolkit_id") else None
    if toolkit_id is not None and project_id is None:
        raise ValueError("toolkit_id requires project_id: toolkit ids are only unique per project")
    dry_run = params.get("dry_run", "true").lower() != "false"
    return project_id, toolkit_id, dry_run


def pgvector_settings(settings: dict | None) -> dict:
    """Only the toolkit's ``pgvector_configuration``, for expansion.

    Expanding the whole settings also resolves every other credential the toolkit names, so
    one missing Jira or ADO configuration would stop the vector store from being checked.
    """
    pgvector = (settings or {}).get("pgvector_configuration")
    if not isinstance(pgvector, dict):
        raise ValueError("toolkit has no pgvector_configuration")
    return {"pgvector_configuration": copy.deepcopy(pgvector)}


def find_orphans(indexes_meta: dict, present_names: set) -> dict:
    """Entries whose index is not in ``present_names``. Enabled and disabled schedules alike."""
    return {name: entry for name, entry in indexes_meta.items() if name not in present_names}


def present_index_names(engine, schema: str) -> set:
    """Names of the indexes that have an ``index_meta`` row in ``schema``, in any state.

    Read-only on purpose: ``get_session_for_schema`` would create the schema and its tables,
    so a dry run would leave empty schemas behind. A reachable store without the table has no
    index at all. Any error propagates, so the caller can tell that apart from "no index".
    """
    if not str(schema).isdigit():
        raise ValueError(f"unexpected toolkit schema {schema!r}")
    if not inspect(engine).has_table("langchain_pg_embedding", schema=schema):
        return set()
    with engine.connect() as conn:
        rows = conn.execute(text(
            f'SELECT DISTINCT cmetadata ->> \'collection\' AS name '  # nosec B608 - digits only
            f'FROM "{schema}".langchain_pg_embedding '
            f"WHERE cmetadata ->> 'type' = 'index_meta'"
        ))
        return {row.name for row in rows if row.name}


def prune_toolkit_orphans(project_session, toolkit, observed: dict) -> tuple[dict, list]:
    """Remove the ``observed`` orphans that are still exactly as observed.

    Returns ``(removed, changed)``. An entry edited or already removed since the scan is left
    alone and reported as changed: someone re-scheduled the index, or the scheduler's own
    prune (#7023) got there first. Only ``indexes_meta`` keys are removed; the toolkit, its
    other indexes and the vector store are not touched.
    """
    project_session.refresh(toolkit)
    live = toolkit.meta.get("indexes_meta", {})
    removed, changed = {}, []
    for name, entry in observed.items():
        if live.get(name) == entry:
            removed[name] = live.pop(name)
        else:
            changed.append(name)
    if not removed:
        return removed, changed
    try:
        flag_modified(toolkit, "meta")
        project_session.commit()
    except Exception:
        project_session.rollback()
        raise
    return removed, changed


def prune_orphaned_index_schedules(project_ids, open_project_session, list_toolkits,
                                   resolve_connection_string, present_index_names,
                                   toolkit_id=None, dry_run=True) -> dict:
    """Scan ``project_ids`` for orphaned index schedules; remove them unless ``dry_run``.

    Collaborators are passed in so the walk can run without the platform:
    ``open_project_session(pid)`` is a context manager, ``list_toolkits(session, toolkit_id)``
    returns the toolkits that carry ``indexes_meta``, ``resolve_connection_string(pid, toolkit)``
    returns the toolkit's pgvector connection string and ``present_index_names(conn, schema)``
    the index names in its store.

    A toolkit whose store cannot be checked is skipped, never pruned. A failing project is
    logged and the scan moves on.
    """
    prefix = "[DRY RUN] " if dry_run else ""
    stats = {"projects": 0, "failed_projects": 0, "toolkits": 0, "orphans": 0,
             "removed": 0, "changed": 0, "skipped": 0}
    for project_id in project_ids:
        stats["projects"] += 1
        try:
            with open_project_session(project_id) as session:
                for toolkit in list_toolkits(session, toolkit_id):
                    indexes_meta = (toolkit.meta or {}).get("indexes_meta") or {}
                    if not isinstance(indexes_meta, dict) or not indexes_meta:
                        continue
                    stats["toolkits"] += 1
                    _prune_toolkit(session, project_id, toolkit, indexes_meta,
                                   resolve_connection_string, present_index_names,
                                   dry_run, prefix, stats)
        except Exception as exc:  # pylint: disable=W0703
            stats["failed_projects"] += 1
            log.exception("%sprune_orphaned_index_schedules: project=%s failed, continuing: %r",
                          prefix, project_id, exc)
    log.info(
        "%sprune_orphaned_index_schedules finished: projects=%s failed_projects=%s toolkits=%s "
        "orphans=%s removed=%s changed=%s skipped=%s",
        prefix, stats["projects"], stats["failed_projects"], stats["toolkits"],
        stats["orphans"], stats["removed"], stats["changed"], stats["skipped"],
    )
    return stats


def _prune_toolkit(session, project_id, toolkit, indexes_meta, resolve_connection_string,
                   present_index_names, dry_run, prefix, stats):
    ctx = _ctx(project_id, toolkit.id)
    try:
        connection_string = resolve_connection_string(project_id, toolkit)
        if not connection_string:
            raise ValueError("no pgvector connection string")
        present = present_index_names(connection_string, str(toolkit.id))
    except Exception as exc:  # pylint: disable=W0703
        stats["skipped"] += 1
        log.warning("%s%s skipped, cannot check its indexes (schedules kept): %r",
                    prefix, ctx, exc)
        return

    orphans = find_orphans(indexes_meta, present)
    if not orphans:
        return
    stats["orphans"] += len(orphans)
    for name, entry in orphans.items():
        owners = sorted((entry or {}).get("schedules", {}))
        log.warning("%s%s orphaned schedule (toolkit type %s), owners %s",
                    prefix, _ctx(project_id, toolkit.id, name), toolkit.type, owners)
    if dry_run:
        return

    try:
        removed, changed = prune_toolkit_orphans(session, toolkit, orphans)
    except Exception as exc:  # pylint: disable=W0703
        stats["skipped"] += 1
        log.exception("%s failed to remove orphaned schedules (rolled back, kept): %r",
                      _ctx(project_id, toolkit.id), exc)
        return
    stats["removed"] += len(removed)
    stats["changed"] += len(changed)
    for name, entry in removed.items():
        # This line is the only backup of the entry: keep it complete.
        log.warning("%s removed orphaned schedules entry: %s",
                    _ctx(project_id, toolkit.id, name), json.dumps(entry, sort_keys=True, default=str))
    for name in changed:
        log.warning("%s changed since the scan, kept", _ctx(project_id, toolkit.id, name))


def _ctx(project_id, toolkit_id, index_name=None) -> str:
    # Same shape as index_scheduling.index_log_context, so one grep finds both.
    suffix = f" index={index_name}" if index_name is not None else ""
    return f"[idx project={project_id} toolkit={toolkit_id}{suffix}]"
