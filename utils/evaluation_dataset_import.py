"""Pure CSV / JSON / JSONL case-import parsing for datasets (EVAL-P1-B3, §17.2).

Stdlib only (``csv`` / ``json``, plus the stdlib ``expected_trajectory`` validator) so it is unit-testable without the ORM. Each parser turns
raw file text into ``(rows, errors)`` where ``rows`` are validated case dicts ready for
``EvalDatasetCaseCreateModel`` and ``errors`` is a per-row report ``{'row': int, 'error': str}``.
Invalid rows are skipped (never abort the whole import) so the API can return an accepted-count
plus the reasons the rest were rejected (§17.2 import error report).

Row shape (both formats)::

    {'input': str, 'variables': dict, 'expected_output': Optional[str], 'source_ref': Optional[str]}

plus ``'expected_trajectory': dict`` when the row carries one (#6809 item 4; see
``evaluation_expected_trajectory``). A malformed one rejects the row.

CSV convention: a header row is required. ``input``, ``expected_output``, ``expected_trajectory``
(a JSON object in the cell) and ``source_ref`` are reserved columns; every other column becomes a
``variables`` key (string value). ``input`` is required.
JSON convention: a top-level array of objects, or an object with a ``cases`` array. Each object
carries ``input`` (required), optional ``expected_output``, optional ``expected_trajectory``
(object), optional ``variables`` (object), optional ``source_ref``.
JSONL convention: one such object per line; blank lines are ignored. Rows are numbered by line.
"""

import csv
import io
import json
from typing import List, Optional, Tuple

from .evaluation_expected_trajectory import validate_or_error as _validate_expected_trajectory

Rows = List[dict]
Errors = List[dict]

_RESERVED = {'input', 'expected_output', 'expected_trajectory', 'source_ref'}

# Import is a single synchronous request that inserts one row per case and then every run over the
# dataset invokes the agent once per case, so an unbounded file is both a request-time and a
# run-cost amplifier. Over-cap rows are reported as errors rather than silently truncated.
MAX_CASES = 5000
MAX_CELL_CHARS = 100_000


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

    input_text = str(input_val)
    expected_text = _clean_expected(raw.get('expected_output'))
    for field, text in (('input', input_text), ('expected_output', expected_text)):
        if text is not None and len(text) > MAX_CELL_CHARS:
            return None, {
                'row': row_no,
                'error': f'"{field}" exceeds the {MAX_CELL_CHARS} character limit',
            }

    expected_trajectory, error = _validate_expected_trajectory(raw.get('expected_trajectory'), row_no)
    if error:
        return None, error

    source_ref = raw.get('source_ref')
    row = {
        'input': input_text,
        'variables': variables,
        'expected_output': expected_text,
        'source_ref': str(source_ref) if source_ref not in (None, '') else None,
    }
    if expected_trajectory is not None:
        row['expected_trajectory'] = expected_trajectory
    return row, None


def parse_csv(content: str) -> Tuple[Rows, Errors]:
    """Parse CSV text. ``input``/``expected_output``/``expected_trajectory``/``source_ref`` are
    reserved columns;
    all other columns fold into ``variables``. Data rows are numbered from 1 (header excluded)."""
    rows: Rows = []
    errors: Errors = []
    reader = csv.DictReader(io.StringIO(content))
    if reader.fieldnames is None:
        return rows, [{'row': 0, 'error': 'empty CSV (no header row)'}]
    if 'input' not in reader.fieldnames:
        return rows, [{'row': 0, 'error': 'CSV header must contain an "input" column'}]

    for i, record in enumerate(reader, start=1):
        if i > MAX_CASES:
            errors.append({'row': i, 'error': f'import is limited to {MAX_CASES} cases'})
            break
        variables = {
            k: v for k, v in record.items()
            if k is not None and k not in _RESERVED and v not in (None, '')
        }
        normalized, error = _normalize_row(
            {
                'input': record.get('input'),
                'expected_output': record.get('expected_output'),
                'expected_trajectory': record.get('expected_trajectory'),
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
        if i > MAX_CASES:
            errors.append({'row': i, 'error': f'import is limited to {MAX_CASES} cases'})
            break
        if not isinstance(record, dict):
            errors.append({'row': i, 'error': 'each case must be an object'})
            continue
        normalized, error = _normalize_row(record, i)
        (rows if normalized else errors).append(normalized or error)
    return rows, errors


def parse_jsonl(content: str) -> Tuple[Rows, Errors]:
    """Parse JSON Lines: one case object per line. Blank lines are skipped; rows are numbered by
    their line, so the error report points at the line to fix."""
    rows: Rows = []
    errors: Errors = []
    count = 0
    for line_no, line in enumerate(content.splitlines(), start=1):
        if not line.strip():
            continue
        count += 1
        if count > MAX_CASES:
            errors.append({'row': line_no, 'error': f'import is limited to {MAX_CASES} cases'})
            break
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, ValueError) as exc:
            errors.append({'row': line_no, 'error': f'invalid JSON: {exc}'})
            continue
        if not isinstance(record, dict):
            errors.append({'row': line_no, 'error': 'each case must be an object'})
            continue
        normalized, error = _normalize_row(record, line_no)
        (rows if normalized else errors).append(normalized or error)
    if count == 0 and not errors:
        errors.append({'row': 0, 'error': 'empty JSONL (no cases)'})
    return rows, errors


def parse_import(fmt: str, content: str) -> Tuple[Rows, Errors]:
    """Dispatch to :func:`parse_csv` / :func:`parse_json` / :func:`parse_jsonl` by ``fmt``."""
    fmt = (fmt or '').lower()
    if fmt == 'csv':
        return parse_csv(content)
    if fmt == 'json':
        return parse_json(content)
    if fmt == 'jsonl':
        return parse_jsonl(content)
    return [], [{'row': 0, 'error': f'unsupported format "{fmt}"'}]
