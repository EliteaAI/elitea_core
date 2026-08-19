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

"""Prose for the canonical indexing report produced by the SDK.

TWIN: the notifications plugin backfills messages for rows written before this
existed and carries a copy of this logic in its ``tasks/db_tasks.py``. Changes
here belong there too.
"""

import json

DEFAULT_ITEM_LABELS = {'singular': 'document', 'plural': 'documents'}
DEFAULT_DEPENDENT_LABELS = {'singular': 'attachment', 'plural': 'attachments'}


def parse_indexing_report(report):
    """Return the report as a dict, whether it arrives parsed or as a JSON string."""
    if isinstance(report, dict):
        return report
    if isinstance(report, str) and report.strip():
        try:
            parsed = json.loads(report)
        except (TypeError, ValueError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _noun(count, labels, default):
    labels = labels or default
    return labels.get('singular' if count == 1 else 'plural', default['plural'])


def is_up_to_date_run(totals):
    """A run where nothing changed — described by its unchanged count, never by a zero."""
    return (
        (totals.get('indexed') or 0) == 0
        and (totals.get('failed') or 0) == 0
        and (totals.get('unchanged') or 0) > 0
    )


def summarize_indexing_report(report):
    """One-line breakdown of a run, or None when no report is available."""
    report = parse_indexing_report(report)
    if not report:
        return None
    totals = report.get('totals') or {}
    labels = report.get('item_labels')
    indexed = totals.get('indexed') or 0

    if is_up_to_date_run(totals):
        unchanged = totals.get('unchanged') or 0
        return f'Up to date — {unchanged} {_noun(unchanged, labels, DEFAULT_ITEM_LABELS)} unchanged'

    unchanged = totals.get('unchanged') or 0
    parts = [f'{indexed} {_noun(indexed, labels, DEFAULT_ITEM_LABELS)} indexed']
    for key, wording in (('skipped', 'skipped'), ('not_indexed', 'not indexed'), ('failed', 'failed')):
        count = totals.get(key) or 0
        if count > 0:
            parts.append(f'{count} {wording}')
    if unchanged:
        parts.append(f'{unchanged} unchanged')
    summary = ', '.join(parts)

    dependent = totals.get('dependent_not_indexed') or 0
    if dependent:
        dependent_noun = _noun(dependent, report.get('dependent_labels'), DEFAULT_DEPENDENT_LABELS)
        summary += f' ({dependent} {dependent_noun} not indexed)'
    return summary


def build_index_notification_message(index_data_status, initiator=None):
    """Notification text for a finished indexing run."""
    index_name = index_data_status.get('index_name') or 'Index'
    link = f'[{index_name}]()'

    if (index_data_status.get('error') or '').strip():
        return f'Index {link} is failed.'

    scheduled_text = ' by schedule' if initiator == 'schedule' else ''
    report = parse_indexing_report(index_data_status.get('report'))

    if report and is_up_to_date_run(report.get('totals') or {}):
        totals = report.get('totals') or {}
        unchanged = totals.get('unchanged') or 0
        unchanged_noun = _noun(unchanged, report.get('item_labels'), DEFAULT_ITEM_LABELS)
        return f'Index {link} is up to date{scheduled_text} — {unchanged} {unchanged_noun} unchanged.'

    breakdown = summarize_indexing_report(report)
    if breakdown is None:
        # Pre-report run: the persisted count is all there is, and it names no item type.
        indexed = index_data_status.get('indexed') or 0
        breakdown = f'{indexed} {_noun(indexed, None, DEFAULT_ITEM_LABELS)} indexed'

    if index_data_status.get('reindex'):
        return f'Index {link} is successfully reindexed{scheduled_text}. {breakdown}.'
    return f'Index {link} is successfully created. {breakdown}.'
