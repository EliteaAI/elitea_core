"""Pins what `extract_json_from_text` accepts from a model asked for a JSON draft (#6416).

The `instructions` field carries a whole Markdown document, so real answers arrive fenced, with
prose either side, and with braces inside the value. `utils/utils.py` reaches the ORM on import, so
the function is loaded on its own here — it depends on nothing but `json` and `re`.
"""
import ast
import json
import pathlib
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


WANTED = (
    '_JSON_STRING_ESCAPES',
    '_MAX_OBJECT_STARTS',
    '_escape_control_characters_in_strings',
    '_decode_object',
    'extract_json_from_text',
)


@pytest.fixture(scope='module')
def utils():
    """The extractor and its repair, lifted from a module whose imports reach the ORM."""
    source = (PLUGIN_ROOT / 'utils' / 'utils.py').read_text()
    wanted = [
        node for node in ast.parse(source).body
        if getattr(node, 'name', None) in WANTED
        or (isinstance(node, ast.Assign) and getattr(node.targets[0], 'id', None) in WANTED)
    ]
    assert len(wanted) == len(WANTED), 'extractor pieces moved or were renamed'

    namespace = {}
    body = '\n\n'.join(ast.get_source_segment(source, node) for node in wanted)
    exec(compile(f'import json, re\nfrom typing import Optional\n\n{body}', 'utils.py', 'exec'), namespace)
    return types.SimpleNamespace(**{name: namespace[name] for name in WANTED})


@pytest.fixture(scope='module')
def extract_json_from_text(utils):
    return utils.extract_json_from_text


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
    '{"name": "x", "instructions": "Say "hi" loudly"}',      # unescaped quote — ambiguous
    '{"name": "release-notes", "instructions": "# T',        # cut off — nothing to repair
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


MARKDOWN_INSTRUCTIONS = """# Release Notes

1. Group changes by area.
2. Call out breaking changes first.

\tExample: `v2.1.0 - adds retries`
"""


def test_a_markdown_document_with_raw_newlines_is_repaired(extract_json_from_text):
    """The likeliest way this fails: the model writes the Markdown but forgets to escape it."""
    raw = '{"name": "release-notes", "instructions": "' + MARKDOWN_INSTRUCTIONS + '"}'

    parsed = json.loads(extract_json_from_text(raw))

    assert parsed['name'] == 'release-notes'
    assert parsed['instructions'] == MARKDOWN_INSTRUCTIONS


def test_the_repair_preserves_the_document_exactly(extract_json_from_text):
    """A repair that silently dropped or mangled the newlines would be worse than the 422."""
    raw = '{"instructions": "line one\nline two\r\nline three\ttabbed"}'

    assert json.loads(extract_json_from_text(raw))['instructions'] == (
        'line one\nline two\r\nline three\ttabbed'
    )


def test_a_repaired_object_still_stops_at_its_own_end(extract_json_from_text):
    raw = '{"instructions": "a\nb"}\n\nHope that helps!'

    assert json.loads(extract_json_from_text(raw)) == {'instructions': 'a\nb'}


def test_an_answer_that_cannot_be_repaired_is_left_alone(extract_json_from_text):
    """An unescaped quote desynchronises the string tracking, so the repair must not be trusted."""
    raw = '{"name": "x", "instructions": "Say "hi"\nloudly"}'

    candidate = extract_json_from_text(raw)

    assert candidate
    with pytest.raises(json.JSONDecodeError):
        json.loads(candidate)


@pytest.mark.parametrize('valid', [
    '{"a": "already \\n escaped"}',
    '{"a": 1, "b": [2, 3], "c": {"d": null}}',
    '{\n  "pretty": "printed",\n  "across": "lines"\n}',
    '{"unicode": "caf\u00e9 \u2014 dash"}',
])
def test_valid_json_is_never_altered(utils, valid):
    """Control characters below 0x20 are illegal inside a JSON string, so escaping one cannot
    change the meaning of a document that was already valid."""
    assert utils._escape_control_characters_in_strings(valid) == valid
    assert json.loads(utils.extract_json_from_text(valid)) == json.loads(valid)


@pytest.mark.parametrize('raw', [
    'Here is a template using {name}: {"name": "x", "instructions": "# T"}',
    'Config was {"unrelated": 1 and then the draft: {"name": "x", "instructions": "# T"}',
])
def test_a_brace_in_the_preamble_does_not_cost_the_draft(extract_json_from_text, raw):
    """The symmetric case to a sign-off containing a brace - the first { is not the object."""
    assert json.loads(extract_json_from_text(raw))['name'] == 'x'


def test_the_search_for_an_opening_brace_is_bounded(utils):
    """A Markdown draft can carry many braces; scanning every one is not worth the failure path."""
    raw = '{ ' * (utils._MAX_OBJECT_STARTS + 3) + '"name": "x"}'

    candidate = utils.extract_json_from_text(raw)

    assert candidate
    with pytest.raises(json.JSONDecodeError):
        json.loads(candidate)


TRUNCATED_WITH_NESTING = {
    'a cut-off dimension list':
        '{"version_id": 1, "dimensions": [{"name": "Politeness", "tier": "agent_adhoc", '
        '"evidence_scope": {"input": true}}, {"name": "Cla',
    'a cut-off draft with a nested object':
        '{"name": "bot", "config": {"a": 1}, "instructions": "# Title\\n\\nDo the thing and then',
    # both faults at once - the plain decode stops at the first raw newline, so only the repaired
    # attempt reaches the truncation, and reporting the plain offset would let the nested object win
    'a cut-off draft that also needs the repair':
        '{"name": "bot", "instructions": "# Title\nStep one.\nStep two.", '
        '"config": {"retries": 3}, "description": "Summarizes the release notes and then',
}


@pytest.mark.parametrize('label', sorted(TRUNCATED_WITH_NESTING))
def test_a_truncated_draft_does_not_yield_one_of_its_own_nested_objects(extract_json_from_text, label):
    """A later brace must explain more of the text than the first did, or a cut-off answer parses
    as its own first dimension - the caller then gets a validation error instead of the truncation
    message, and the parse diagnostic never fires for the case it was built for."""
    candidate = extract_json_from_text(TRUNCATED_WITH_NESTING[label])

    with pytest.raises(json.JSONDecodeError):
        json.loads(candidate)


def test_a_nested_object_in_a_whole_draft_is_still_fine(extract_json_from_text):
    """The guard must not cost a draft that simply contains an object."""
    raw = '{"name": "bot", "config": {"a": 1}, "instructions": "# T"}'

    assert json.loads(extract_json_from_text(raw))['name'] == 'bot'


def test_a_preamble_brace_is_still_passed_when_the_draft_needs_the_repair(extract_json_from_text):
    """The other direction of the same comparison: the first brace breaks immediately, so a later
    one that needs repairing to decode must still be reached."""
    raw = 'Use {name} as the placeholder: {"instructions": "# T\nline two"}'

    assert json.loads(extract_json_from_text(raw))['instructions'] == '# T\nline two'
