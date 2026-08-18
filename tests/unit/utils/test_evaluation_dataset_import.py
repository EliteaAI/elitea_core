"""Unit tests for evaluation_dataset_import.py — the B3 CSV/JSON case-import contract (§17.2).

Pure stdlib functions loaded directly from their path. These lock the row shape
(input/variables/expected_output/source_ref), the reserved-column convention, and the
per-row error report (invalid rows skipped, never abort the import).
"""
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def imp(utils_path):
    return load_utils_module(utils_path, 'evaluation_dataset_import')


# --- CSV ----------------------------------------------------------------------

def test_csv_basic(imp):
    rows, errors = imp.parse_csv('input,expected_output\nwhat is 2+2?,4\n')
    assert errors == []
    assert rows == [{'input': 'what is 2+2?', 'variables': {}, 'expected_output': '4', 'source_ref': None}]


def test_csv_extra_columns_fold_into_variables(imp):
    rows, errors = imp.parse_csv('input,expected_output,order_id,tier\nrefund,ok,8842,gold\n')
    assert errors == []
    assert rows[0]['variables'] == {'order_id': '8842', 'tier': 'gold'}


def test_csv_missing_input_column_is_header_error(imp):
    rows, errors = imp.parse_csv('question,expected_output\nq,a\n')
    assert rows == []
    assert errors == [{'row': 0, 'error': 'CSV header must contain an "input" column'}]


def test_csv_blank_input_row_rejected(imp):
    rows, errors = imp.parse_csv('input,expected_output\n,a\nreal,b\n')
    assert len(rows) == 1 and rows[0]['input'] == 'real'
    assert errors == [{'row': 1, 'error': 'missing required field "input"'}]


def test_csv_empty_expected_is_none(imp):
    rows, _ = imp.parse_csv('input,expected_output\nq,\n')
    assert rows[0]['expected_output'] is None


def test_csv_empty_content(imp):
    rows, errors = imp.parse_csv('')
    assert rows == [] and errors == [{'row': 0, 'error': 'empty CSV (no header row)'}]


def test_csv_source_ref_reserved(imp):
    rows, _ = imp.parse_csv('input,source_ref\nq,conv-9\n')
    assert rows[0]['source_ref'] == 'conv-9' and rows[0]['variables'] == {}


# --- JSON ---------------------------------------------------------------------

def test_json_array(imp):
    rows, errors = imp.parse_json('[{"input": "q1", "expected_output": "a1"}, {"input": "q2"}]')
    assert errors == []
    assert rows[0] == {'input': 'q1', 'variables': {}, 'expected_output': 'a1', 'source_ref': None}
    assert rows[1]['expected_output'] is None


def test_json_cases_wrapper(imp):
    rows, errors = imp.parse_json('{"cases": [{"input": "q"}]}')
    assert errors == [] and len(rows) == 1


def test_json_with_variables_object(imp):
    rows, _ = imp.parse_json('[{"input": "q", "variables": {"k": "v"}}]')
    assert rows[0]['variables'] == {'k': 'v'}


def test_json_bad_variables_type_rejected(imp):
    rows, errors = imp.parse_json('[{"input": "q", "variables": "nope"}]')
    assert rows == [] and errors == [{'row': 1, 'error': '"variables" must be an object'}]


def test_json_missing_input_rejected(imp):
    rows, errors = imp.parse_json('[{"expected_output": "a"}]')
    assert rows == [] and errors[0]['error'] == 'missing required field "input"'


def test_json_non_object_row_rejected(imp):
    rows, errors = imp.parse_json('["just a string"]')
    assert rows == [] and errors == [{'row': 1, 'error': 'each case must be an object'}]


def test_json_invalid_syntax(imp):
    rows, errors = imp.parse_json('{not json')
    assert rows == [] and errors[0]['row'] == 0 and 'invalid JSON' in errors[0]['error']


def test_json_not_array_or_cases(imp):
    rows, errors = imp.parse_json('{"foo": 1}')
    assert rows == [] and 'must be an array' in errors[0]['error']


# --- dispatcher ---------------------------------------------------------------

def test_parse_import_dispatch_csv(imp):
    rows, _ = imp.parse_import('csv', 'input\nq\n')
    assert rows[0]['input'] == 'q'


def test_parse_import_dispatch_json(imp):
    rows, _ = imp.parse_import('json', '[{"input": "q"}]')
    assert rows[0]['input'] == 'q'


def test_parse_import_unknown_format(imp):
    rows, errors = imp.parse_import('xml', '<x/>')
    assert rows == [] and 'unsupported format' in errors[0]['error']
