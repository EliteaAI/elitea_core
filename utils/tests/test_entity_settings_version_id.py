"""Unit tests for entity_settings version_id coercion.

The endpoint stores entity_settings verbatim into JSONB; a string version_id
(e.g. from an MCP/JSON caller) never matches the integer ApplicationVersion.id,
so the pinned version fails to resolve. coerce_version_id fixes this write-side.

The helper imports only stdlib, so it loads in isolation; we add the parent
``utils/`` dir to sys.path and import it directly.

    python3 -m pytest utils/tests/test_entity_settings_version_id.py -v
"""

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from entity_settings_utils import coerce_version_id  # noqa: E402


def test_string_version_id_is_coerced_to_int():
    data = {"version_id": "10"}
    coerce_version_id(data)
    assert data["version_id"] == 10
    assert isinstance(data["version_id"], int)


def test_int_version_id_is_unchanged():
    data = {"version_id": 202}
    coerce_version_id(data)
    assert data["version_id"] == 202


def test_absent_version_id_is_left_untouched():
    data = {"llm_settings": {"model_name": "gpt-4o"}}
    coerce_version_id(data)
    assert "version_id" not in data


def test_none_version_id_is_left_untouched():
    data = {"version_id": None}
    coerce_version_id(data)
    assert data["version_id"] is None


@pytest.mark.parametrize("bad", ["abc", "10.5", "", "1,2"])
def test_non_integer_string_raises(bad):
    with pytest.raises((ValueError, TypeError)):
        coerce_version_id({"version_id": bad})


def test_other_keys_are_preserved():
    data = {"version_id": "7", "variables": [{"name": "lang", "value": "en"}]}
    coerce_version_id(data)
    assert data["version_id"] == 7
    assert data["variables"] == [{"name": "lang", "value": "en"}]
