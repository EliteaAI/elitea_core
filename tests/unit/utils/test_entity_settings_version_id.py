"""Unit tests for entity_settings version_id coercion (utils/entity_settings_utils.py).

The endpoint stores entity_settings verbatim into JSONB; a string version_id
(e.g. from an MCP/JSON caller) never matches the integer ApplicationVersion.id,
so the pinned version fails to resolve. coerce_version_id fixes this write-side.
The helper imports only stdlib, so it loads without stubs.

Run from plugin root:
    python3 tests/run_tests.py unit/utils/test_entity_settings_version_id.py -v
"""
import sys

import pytest


@pytest.fixture(scope="module")
def coerce_version_id(utils_path):
    sys.path.insert(0, str(utils_path))
    try:
        import entity_settings_utils
        return entity_settings_utils.coerce_version_id
    finally:
        sys.path.remove(str(utils_path))


class TestCoerceVersionId:
    def test_string_version_id_is_coerced_to_int(self, coerce_version_id):
        data = {"version_id": "10"}
        coerce_version_id(data)
        assert data["version_id"] == 10
        assert isinstance(data["version_id"], int)

    def test_int_version_id_is_unchanged(self, coerce_version_id):
        data = {"version_id": 202}
        coerce_version_id(data)
        assert data["version_id"] == 202

    def test_absent_version_id_is_left_untouched(self, coerce_version_id):
        data = {"llm_settings": {"model_name": "gpt-4o"}}
        coerce_version_id(data)
        assert "version_id" not in data

    def test_none_version_id_is_left_untouched(self, coerce_version_id):
        data = {"version_id": None}
        coerce_version_id(data)
        assert data["version_id"] is None

    @pytest.mark.parametrize("bad", ["abc", "10.5", "", "1,2"])
    def test_non_integer_string_raises(self, coerce_version_id, bad):
        with pytest.raises((ValueError, TypeError)):
            coerce_version_id({"version_id": bad})

    def test_other_keys_are_preserved(self, coerce_version_id):
        data = {"version_id": "7", "variables": [{"name": "lang", "value": "en"}]}
        coerce_version_id(data)
        assert data["version_id"] == 7
        assert data["variables"] == [{"name": "lang", "value": "en"}]
