"""EL-7679: llm_schema — the response model's JSON schema the SDK checks the answer against.

Server-filled fields are removed; length/count bounds are kept so the model sees the real limits
(the SDK checks only the required fields; the endpoint model does full validation).
"""
import pathlib
import sys
import types
from typing import List, Optional

import pytest
from pydantic import BaseModel, Field

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module


class Item(BaseModel):
    id: int
    name: str = Field(max_length=10)


class Draft(BaseModel):
    name: str = Field(min_length=1, max_length=32, description="Agent name")
    description: str = Field(max_length=2304)
    starters: Optional[List[str]] = Field(default=None, max_length=4)
    items: List[Item] = Field(default_factory=list)
    validation_result: dict = Field(description="Filled by the server")


@pytest.fixture(scope='module')
def draft_llm_utils():
    utils_stub = types.ModuleType('utils')
    utils_stub.get_public_project_id = lambda: 1
    return load_utils_module(TESTS_DIR.parent / 'utils', 'draft_llm_utils',
                             extra_stubs={'plugins.elitea_core.utils.utils': utils_stub})


def _walk(node):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for value in node:
            yield from _walk(value)


def test_excluded_fields_are_gone(draft_llm_utils):
    schema = draft_llm_utils.llm_schema(Draft, exclude={'validation_result'})

    assert 'validation_result' not in schema['properties']
    assert 'validation_result' not in schema.get('required', [])
    assert set(schema['required']) == {'name', 'description'}


def test_length_and_count_bounds_are_kept(draft_llm_utils):
    """Review #5: some models reject over-long values with a 422, so the model must see the limits."""
    schema = draft_llm_utils.llm_schema(Draft)

    assert schema['properties']['name']['maxLength'] == 32
    assert schema['properties']['description']['maxLength'] == 2304


def test_types_descriptions_and_nested_defs_are_kept(draft_llm_utils):
    schema = draft_llm_utils.llm_schema(Draft)

    assert schema['properties']['name']['type'] == 'string'
    assert schema['properties']['name']['description'] == 'Agent name'
    assert schema['properties']['name']['minLength'] == 1
    assert '$defs' in schema and 'Item' in schema['$defs']
    assert set(schema['$defs']['Item']['required']) == {'id', 'name'}


def test_model_schema_is_not_mutated(draft_llm_utils):
    before = Draft.model_json_schema()

    draft_llm_utils.llm_schema(Draft, exclude={'validation_result'})

    assert Draft.model_json_schema() == before


# Real Slice-1 models: the schema the endpoints send is valid and leaves server-filled fields out
def test_pipeline_schema_omits_validation_result(draft_llm_utils):
    sys.path.insert(0, str(TESTS_DIR / 'unit' / 'models'))
    from test_7323_generate_pipeline_draft import pd_module as _fixture  # noqa: F401
    pd = _fixture.__wrapped__()

    schema = draft_llm_utils.llm_schema(pd.GeneratePipelineDraftResponse, exclude={'validation_result'})

    assert 'validation_result' not in schema['properties']
    assert 'validation_result' not in schema.get('required', [])
    assert {'name', 'pipeline_yaml'} <= set(schema['required'])
