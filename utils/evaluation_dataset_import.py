"""Pure CSV / JSON case-import parsing for datasets (EVAL-P1-B3, §17.2).

Stdlib only (``csv`` / ``json``) so it is unit-testable without the ORM. Each parser turns
raw file text into ``(rows, errors)`` where ``rows`` are validated case dicts ready for
``EvalDatasetCaseCreateModel`` and ``errors`` is a per-row report ``{'row': int, 'error': str}``.
Invalid rows are skipped (never abort the whole import) so the API can return an accepted-count
plus the reasons the rest were rejected (§17.2 import error report).

Row shape (both formats)::

    {'input': str, 'variables': dict, 'expected_output': Optional[str], 'source_ref': Optional[str]}

CSV convention: a header row is required. ``input`` and ``expected_output`` are reserved
columns; every other column becomes a ``variables`` key (string value). ``input`` is required.
JSON convention: a top-level array of objects, or an object with a ``cases`` array. Each object
carries ``input`` (required), optional ``expected_output``, optional ``variables`` (object),
optional ``source_ref``.
"""

import csv
import io
import json
from typing import List, Optional, Tuple

Rows = List[dict]
Errors = List[dict]

_RESERVED = {'input', 'expected_output', 'source_ref'}


def _clean_expected(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_row(raw: dict, row_no: int) -> Tuple[Optional[dict], Optional[dict]]:
    """Validate one already-parsed mapping into a case dict, or an error entry."""
    input_val = raw.get('input')
    if input_val is None or not str(input_val).strip():
        return None, {'row': row_no, 'error': 'missing required field "input"'}

    variables = raw.get('variables') or {}
    if not isinstance(variables, dict):
        return None, {'row': row_no, 'error': '"variables" must be an object'}

    source_ref = raw.get('source_ref')
    return {
        'input': str(input_val),
        'variables': variables,
        'expected_output': _clean_expected(raw.get('expected_output')),
        'source_ref': str(source_ref) if source_ref not in (None, '') else None,
    }, None


def parse_csv(content: str) -> Tuple[Rows, Errors]:
    """Parse CSV text. ``input``/``expected_output``/``source_ref`` are reserved columns;
    all other columns fold into ``variables``. Data rows are numbered from 1 (header excluded)."""
    rows: Rows = []
    errors: Errors = []
    reader = csv.DictReader(io.StringIO(content))
    if reader.fieldnames is None:
        return rows, [{'row': 0, 'error': 'empty CSV (no header row)'}]
    if 'input' not in reader.fieldnames:
        return rows, [{'row': 0, 'error': 'CSV header must contain an "input" column'}]

    for i, record in enumerate(reader, start=1):
        variables = {
            k: v for k, v in record.items()
            if k is not None and k not in _RESERVED and v not in (None, '')
        }
        normalized, error = _normalize_row(
            {
                'input': record.get('input'),
                'expected_output': record.get('expected_output'),
                'source_ref': record.get('source_ref'),
                'variables': variables,
            },
            i,
        )
        (rows if normalized else errors).append(normalized or error)
    return rows, errors


def parse_json(content: str) -> Tuple[Rows, Errors]:
    """Parse a JSON array of case objects (or ``{"cases": [...]}``). Objects are numbered
    from 1."""
    try:
        payload = json.loads(content)
    except (json.JSONDecodeError, ValueError) as exc:
        return [], [{'row': 0, 'error': f'invalid JSON: {exc}'}]

    if isinstance(payload, dict):
        payload = payload.get('cases')
    if not isinstance(payload, list):
        return [], [{'row': 0, 'error': 'JSON must be an array of cases or an object with a "cases" array'}]

    rows: Rows = []
    errors: Errors = []
    for i, record in enumerate(payload, start=1):
        if not isinstance(record, dict):
            errors.append({'row': i, 'error': 'each case must be an object'})
            continue
        normalized, error = _normalize_row(record, i)
        (rows if normalized else errors).append(normalized or error)
    return rows, errors


def parse_import(fmt: str, content: str) -> Tuple[Rows, Errors]:
    """Dispatch to :func:`parse_csv` / :func:`parse_json` by ``fmt`` ('csv' | 'json')."""
    fmt = (fmt or '').lower()
    if fmt == 'csv':
        return parse_csv(content)
    if fmt == 'json':
        return parse_json(content)
    return [], [{'row': 0, 'error': f'unsupported format "{fmt}"'}]
