"""An assistant answer that happens to be a JSON array must reach the user verbatim.

``chat_message_stream_end`` JSON-parses any content starting with "[" to sniff for
multimodal chunks, then falls back to ``str(content)`` when it isn't multimodal —
which turned a model answer like ``[{"number": 7}]`` into the Python repr
``[{'number': 7}]``. Tool results are now clean JSON, so answers start with "["
routinely and the defect became the common case.
"""

import ast
import json
import pathlib

import pytest

MODULE_PATH = pathlib.Path(__file__).resolve().parents[2] / 'utils' / 'message_stream.py'


def _load_helper():
    """Execute just the helpers, so the test needs no Pylon/DB runtime."""
    tree = ast.parse(MODULE_PATH.read_text())
    wanted = {'parse_content_chunks', '_is_chunk_list', '_is_content_chunk'}
    names = {'CONTENT_CHUNK_TYPES', 'CHUNK_KEYS_BY_TYPE', 'TEXT_ONLY_CHUNK_KEYS'}
    body = [
        node for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in wanted)
        or (isinstance(node, ast.Assign) and getattr(node.targets[0], 'id', '') in names)
    ]
    namespace = {'json': json, 'frozenset': frozenset}
    exec(compile(ast.Module(body=body, type_ignores=[]), str(MODULE_PATH), 'exec'), namespace)
    return namespace['parse_content_chunks']


@pytest.fixture
def parse_content_chunks():
    return _load_helper()


class TestPlainAnswers:
    def test_json_array_answer_is_left_as_written(self, parse_content_chunks):
        answer = '[{"number": 535, "title": "fix(carrier)", "labels": []}]'

        assert parse_content_chunks(answer) == answer

    def test_result_is_not_a_python_repr(self, parse_content_chunks):
        answer = '[{"number": 535, "state": "open"}]'

        rendered = str(parse_content_chunks(answer))

        assert "'number'" not in rendered
        assert json.loads(rendered) == [{"number": 535, "state": "open"}]

    def test_prose_answer_is_untouched(self, parse_content_chunks):
        assert parse_content_chunks('Here are the issues.') == 'Here are the issues.'

    def test_malformed_json_stays_a_string(self, parse_content_chunks):
        assert parse_content_chunks('[{"number": 535') == '[{"number": 535'


class TestMultimodalAnswers:
    def test_content_blocks_are_parsed(self, parse_content_chunks):
        blocks = [{'type': 'text', 'text': 'here'}, {'type': 'image_url', 'image_url': {'url': 'u'}}]

        assert parse_content_chunks(json.dumps(blocks)) == blocks

    def test_text_only_chunks_are_parsed_only_when_allowed(self, parse_content_chunks):
        chunks = json.dumps([{'text': 'child said this'}])

        assert parse_content_chunks(chunks) == chunks
        assert parse_content_chunks(chunks, accept_text_only_chunks=True) == [{'text': 'child said this'}]

    def test_records_carrying_no_chunk_keys_are_not_mistaken_for_chunks(self, parse_content_chunks):
        answer = json.dumps([{'number': 1, 'title': 'a'}])

        assert parse_content_chunks(answer, accept_text_only_chunks=True) == answer


class TestRecordsThatCarryATypeField:
    """A record's own `type` must not be mistaken for a content-chunk type.

    Keying off the presence of `type` made the caller filter these to nothing and
    save no message item, so the user saw a blank answer.
    """

    def test_work_item_records_stay_a_string(self, parse_content_chunks):
        answer = json.dumps([{'id': 4711, 'type': 'Bug', 'title': 'Login fails'}])

        assert parse_content_chunks(answer) == answer

    def test_records_with_type_are_not_chunks_for_child_messages_either(self, parse_content_chunks):
        answer = json.dumps([{'id': 1, 'type': 'Task'}])

        assert parse_content_chunks(answer, accept_text_only_chunks=True) == answer

    def test_real_content_chunks_are_still_parsed(self, parse_content_chunks):
        # Each type carries the key it implies; an "image" block with only a text
        # field is not a shape any emitter produces.
        for blocks in (
            [{'type': 'text', 'text': 'x'}],
            [{'type': 'image', 'source': {'data': 'b64'}}],
            [{'type': 'image_url', 'image_url': {'url': 'https://x/y.png'}}],
            [{'type': 'document', 'source': {'data': 'b64'}}],
            [{'type': 'tool_use', 'id': 'call-1', 'name': 'get_issues'}],
        ):
            assert parse_content_chunks(json.dumps(blocks)) == blocks

    def test_mixed_chunk_types_are_parsed(self, parse_content_chunks):
        blocks = [{'type': 'text', 'text': 'here'}, {'type': 'image_url', 'image_url': {'url': 'u'}}]

        assert parse_content_chunks(json.dumps(blocks)) == blocks


class TestRecordsWhoseTypeValueCollidesWithAChunkType:
    """`type: 'text'` is not enough -- real records use those values too.

    The earlier tests all used types outside the set ('Bug', 'Task'), so they could
    not catch this: a SharePoint page reader emits {'type': 'text', 'content': ...}
    and {'type': 'image', 'description': ..., 'src': ...}, which downstream
    filtering would store as an empty string and a Python dict repr.
    """

    def test_sharepoint_style_records_stay_a_string(self, parse_content_chunks):
        answer = json.dumps([
            {'type': 'text', 'content': 'page text'},
            {'type': 'image', 'description': 'a chart', 'src': 'chart.png'},
        ])

        assert parse_content_chunks(answer) == answer

    def test_search_hits_with_a_text_field_stay_a_string(self, parse_content_chunks):
        answer = json.dumps([{'text': 'match', 'score': 0.9}])

        assert parse_content_chunks(answer, accept_text_only_chunks=True) == answer

    def test_a_real_text_block_is_still_parsed(self, parse_content_chunks):
        blocks = [{'type': 'text', 'text': 'hi'}]

        assert parse_content_chunks(json.dumps(blocks)) == blocks

    def test_a_real_image_block_is_still_parsed(self, parse_content_chunks):
        blocks = [{'type': 'image_url', 'image_url': {'url': 'https://x/y.png'}}]

        assert parse_content_chunks(json.dumps(blocks)) == blocks

    def test_a_bare_text_chunk_is_still_parsed_for_children(self, parse_content_chunks):
        blocks = [{'text': 'child said this'}]

        assert parse_content_chunks(json.dumps(blocks), accept_text_only_chunks=True) == blocks
