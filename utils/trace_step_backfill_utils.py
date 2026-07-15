#!/usr/bin/python3
# coding=utf-8

#   Copyright 2026 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""Helpers for the backfill_legacy_trace_steps admin task (Epic #5724, TS-7/#5800)."""

import datetime

from sqlalchemy import text

from pylon.core.tools import log  # pylint: disable=E0611,E0401

from .trace_step_writer import sync_trace_steps

# Mirrors pylon_indexer/plugins/indexer_worker/methods/agent_common.py:441
# (TS-5/#5729). Different pylon process -> cannot be imported; keep in sync by hand.
MAX_TOOL_OUTPUT_CHARS = 200000
TRUNCATION_SUFFIX = '…[truncated]'


def parse_backfill_params(raw_param: str) -> dict:
    """Parse "project_ids=<all|N|lo-hi>;cutoff_days=<180|all>;dry_run=<true|false>"; raises ValueError on bad input."""
    tokens = [t.strip() for t in raw_param.split(';') if t.strip()]
    params = {}
    for token in tokens:
        if '=' not in token:
            raise ValueError(f"Invalid param token (expected key=value): {token!r}")
        key, _, value = token.partition('=')
        params[key.strip()] = value.strip()

    project_id_raw = params.get('project_ids', 'all').strip()
    if project_id_raw == 'all':
        project_id_spec = ('all', None)
    elif '-' in project_id_raw:
        parts = project_id_raw.split('-', 1)
        try:
            lo, hi = int(parts[0]), int(parts[1])
        except ValueError:
            raise ValueError(f"Invalid project_ids range: {project_id_raw!r}")  # pylint: disable=W0707
        if lo > hi:
            raise ValueError(f"Invalid project_ids range: {lo} > {hi}")
        project_id_spec = ('range', (lo, hi))
    else:
        try:
            project_id_spec = ('single', int(project_id_raw))
        except ValueError:
            raise ValueError(f"Invalid project_ids: {project_id_raw!r}")  # pylint: disable=W0707

    cutoff_raw = params.get('cutoff_days', '180').strip().lower()
    if cutoff_raw == 'all':
        cutoff_days = 'all'
    else:
        try:
            cutoff_days = int(cutoff_raw)
        except ValueError:
            raise ValueError(f"Invalid cutoff_days: {cutoff_raw!r} (expected int or 'all')")  # pylint: disable=W0707
        if cutoff_days < 0:
            raise ValueError(f"Invalid cutoff_days: {cutoff_days} (must be >= 0)")

    dry_run_raw = params.get('dry_run', 'true').strip().lower()
    if dry_run_raw in ('true', '1', 'yes'):
        dry_run = True
    elif dry_run_raw in ('false', '0', 'no'):
        dry_run = False
    else:
        raise ValueError(f"Invalid dry_run value: {dry_run_raw!r} (expected true/false)")

    return {
        'project_id_spec': project_id_spec,
        'cutoff_days': cutoff_days,
        'dry_run': dry_run,
    }


# Truncation runs in the Postgres backend (CASE/left()/||) so only bounded JSON reaches Python.
_CANDIDATE_SQL = """
SELECT
    id,
    (SELECT jsonb_object_agg(
        key,
        CASE WHEN length(value->>'tool_output') > :cap
            THEN jsonb_set(value, '{{tool_output}}', to_jsonb(left(value->>'tool_output', :cap) || :suffix))
            ELSE value END)
     FROM jsonb_each(meta->'tool_calls')) AS tool_calls,
    (SELECT jsonb_agg(
        CASE WHEN length(elem->>'text') > :cap
            THEN jsonb_set(elem, '{{text}}', to_jsonb(left(elem->>'text', :cap) || :suffix))
            ELSE elem END
        || CASE WHEN length(elem->>'thinking') > :cap
            THEN jsonb_build_object('thinking', left(elem->>'thinking', :cap) || :suffix)
            ELSE '{{}}'::jsonb END)
     FROM jsonb_array_elements(meta->'thinking_steps') elem) AS thinking_steps,
    (
        EXISTS (SELECT 1 FROM jsonb_each(meta->'tool_calls') v
                WHERE length(v.value->>'tool_output') > :cap)
        OR EXISTS (SELECT 1 FROM jsonb_array_elements(meta->'thinking_steps') e
                   WHERE length(e->>'text') > :cap OR length(e->>'thinking') > :cap)
    ) AS any_truncated
FROM {table}
WHERE is_streaming = false AND (meta ? 'tool_calls' OR meta ? 'thinking_steps')
{cutoff_clause}
ORDER BY created_at DESC
"""  # nosec B608 - project_id is platform-resolved (resolve_target_project_ids), not user input

_STREAMING_SKIP_COUNT_SQL = """
SELECT count(*) FROM {table}
WHERE is_streaming = true AND (meta ? 'tool_calls' OR meta ? 'thinking_steps')
{cutoff_clause}
"""  # nosec B608

_TRACE_STEP_EXISTS_SQL = "SELECT 1 FROM {table} WHERE message_group_id = :group_id LIMIT 1"  # nosec B608

_STRIP_AND_MARK_SQL = """
UPDATE {table}
SET meta = (meta - CAST(ARRAY['tool_calls', 'thinking_steps'] AS text[]))
           || jsonb_build_object('_migrated_trace_steps', jsonb_build_object(
                'keys', ARRAY['tool_calls', 'thinking_steps'],
                'reason', 'backfilled by backfill_legacy_trace_steps',
                'migrated_at', :migrated_at))
WHERE id = :group_id
"""  # nosec B608


def backfill_project(session, project_id: int, cutoff_days, dry_run: bool, yield_to_hub) -> dict:
    """Backfill one project's message groups. Returns the reconciliation counters dict."""
    group_table = f"p_{project_id}.chat_message_group"
    trace_step_table = f"p_{project_id}.chat_message_trace_step"

    if cutoff_days == 'all':
        cutoff_clause = ""
        cutoff_params = {}
    else:
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=cutoff_days)
        cutoff_clause = "AND created_at >= :cutoff"
        cutoff_params = {'cutoff': cutoff}

    counters = {
        'scanned': 0,
        'migrated': 0,
        'skipped_streaming': 0,
        'skipped_no_keys': 0,
        'skipped_already_migrated': 0,
        'truncated_results': 0,
        'meta_stripped': 0,
        'errors': 0,
    }

    counters['skipped_streaming'] = session.execute(
        text(_STREAMING_SKIP_COUNT_SQL.format(table=group_table, cutoff_clause=cutoff_clause)),
        cutoff_params,
    ).scalar() or 0

    candidates = session.execute(
        text(_CANDIDATE_SQL.format(table=group_table, cutoff_clause=cutoff_clause)),
        {'cap': MAX_TOOL_OUTPUT_CHARS, 'suffix': TRUNCATION_SUFFIX, **cutoff_params},
    ).fetchall()

    for row in candidates:
        yield_to_hub()

        already_migrated = session.execute(
            text(_TRACE_STEP_EXISTS_SQL.format(table=trace_step_table)),
            {'group_id': row.id},
        ).first()
        if already_migrated:
            counters['skipped_already_migrated'] += 1
            continue

        tool_calls = row.tool_calls or {}
        thinking_steps = row.thinking_steps or []
        if not tool_calls and not thinking_steps:
            counters['skipped_no_keys'] += 1
            continue

        counters['scanned'] += 1
        if row.any_truncated:
            counters['truncated_results'] += 1

        if dry_run:
            counters['migrated'] += 1
            continue

        try:
            sync_trace_steps(session, row.id, tool_calls, thinking_steps)
            session.execute(
                text(_STRIP_AND_MARK_SQL.format(table=group_table)),
                {
                    'group_id': row.id,
                    'migrated_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                },
            )
            session.commit()
            counters['migrated'] += 1
            counters['meta_stripped'] += 1
        except Exception:  # pylint: disable=W0703
            session.rollback()
            counters['errors'] += 1
            log.exception(
                "backfill_legacy_trace_steps: error migrating project %s group %s",
                project_id, row.id,
            )

    return counters
