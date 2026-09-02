"""Pins what `extract_json_from_text` accepts from a model asked for a JSON draft (#6416).

The `instructions` field carries a whole Markdown document, so real answers arrive fenced, with
prose either side, and with braces inside the value. `utils/utils.py` reaches the ORM on import, so
the function is loaded on its own here — it depends on nothing but `json` and `re`.
"""
import ast
import json
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture(scope='module')
def extract_json_from_text():
    source = (PLUGIN_ROOT / 'utils' / 'utils.py').read_text()
    definition = next(
        node for node in ast.parse(source).body
        if isinstance(node, ast.FunctionDef) and node.name == 'extract_json_from_text'
    )

    namespace = {}
    exec(compile(f'import json, re\n\n{ast.get_source_segment(source, definition)}', 'utils.py', 'exec'), namespace)
    return namespace['extract_json_from_text']


DRAFT = '{"name": "release-notes", "description": "d", "instructions": "# T\\n\\nDo it."}'


@pytest.mark.parametrize('label, raw', [
    ('bare', DRAFT),
    ('fenced', f'```json\n{DRAFT}\n```'),
    ('unlabelled fence', f'```\n{DRAFT}\n```'),
    ('prose preamble', f'Here is your skill:\n{DRAFT}'),
    ('trailing prose', f'{DRAFT}\n\nLet me know if you want changes!'),
    ('nested code fence in the value',
     '{"name": "x", "instructions": "Run:\\n```bash\\necho hi\\n```\\nDone."}'),
])
def test_a_usable_answer_survives_extraction(extract_json_from_text, label, raw):
    assert json.loads(extract_json_from_text(raw))


def test_a_sign_off_containing_a_brace_no_longer_drags_in(extract_json_from_text):
    """`rfind('}')` used to swallow the pleasantry and fail the whole draft as `Extra data`."""
    raw = f'{DRAFT}\n\nUse {{placeholders}} as needed.'

    assert json.loads(extract_json_from_text(raw))


def test_only_the_first_object_is_taken(extract_json_from_text):
    """Two objects concatenated are `Extra data` to json.loads; the draft is the first."""
    parsed = json.loads(extract_json_from_text(f'{DRAFT}\n{{"name": "second"}}'))

    assert parsed['name'] == 'release-notes'


@pytest.mark.parametrize('raw', [
    '{"name": "x", "instructions": "line one\nline two"}',   # unescaped control character
    '{"name": "x", "instructions": "Say "hi" loudly"}',      # unescaped quote
    '{"name": "release-notes", "instructions": "# T',        # cut off
])
def test_a_genuinely_malformed_answer_still_reaches_the_caller(extract_json_from_text, raw):
    """It must not raise: the caller reports the decode error, and needs a candidate to describe."""
    candidate = extract_json_from_text(raw)

    assert candidate
    with pytest.raises(json.JSONDecodeError):
        json.loads(candidate)


@pytest.mark.parametrize('raw', ['', 'no json here at all', 'prose with a } but no opener'])
def test_text_without_an_object_is_returned_unchanged(extract_json_from_text, raw):
    assert extract_json_from_text(raw) == raw
