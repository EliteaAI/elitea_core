"""EL-7679 review #4: output_schema is a server-side predict argument, never request data."""
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]


def test_llm_chat_request_has_no_output_schema_field():
    source = (ROOT / 'models' / 'pd' / 'chat.py').read_text()

    assert 'output_schema' not in source


def test_payload_builder_does_not_copy_it_from_the_request():
    source = (ROOT / 'utils' / 'predict_utils.py').read_text()

    assert 'output_schema' not in source


def test_predict_sio_llm_takes_it_as_an_argument_and_sets_the_payload():
    source = (ROOT / 'rpc' / 'application.py').read_text()
    start = source.index('    def predict_sio_llm(self,')
    body = source[start:source.index('\n    def ', start + 10)]

    assert 'output_schema: Optional[dict] = None,' in body
    assert "payload['output_schema'] = output_schema" in body
