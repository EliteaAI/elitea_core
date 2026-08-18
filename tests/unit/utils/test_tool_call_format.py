"""The persisted "Calling tool ..." message carries parameters as JSON — the UI parses
them back to render the request; a repr-style dict literal here re-breaks that."""
import json
import pathlib
import re
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_module_with_stubs

MESSAGE_PATTERN = re.compile(r"^Calling tool '([^']+)' with parameters: (.+)$", re.DOTALL)


@pytest.fixture(scope="module")
def format_tool_call(utils_path):
    module = load_module_with_stubs(
        utils_path / "tool_call_format.py",
        "plugins.elitea_core.utils.tool_call_format",
    )
    return module.format_tool_call_as_user_input


class TestFormatToolCallAsUserInput:
    def test_params_round_trip_through_json(self, format_tool_call):
        params = {
            "index_name": "docs",
            "clean_index": False,
            "progress_step": 10,
            "cql": None,
            "chunking_config": {".md": {"max_tokens": 512, "prompt": ""}},
            "skip_extensions": [],
        }

        message = format_tool_call("index_data", params)
        match = MESSAGE_PATTERN.match(message)

        assert match is not None
        assert match.group(1) == "index_data"
        assert json.loads(match.group(2)) == params

    def test_non_ascii_values_stay_readable(self, format_tool_call):
        message = format_tool_call("search_index", {"query": "статус релиза"})

        assert "статус релиза" in message

    def test_unserializable_values_fall_back_to_str(self, format_tool_call):
        class Opaque:
            def __str__(self):
                return "opaque-value"

        message = format_tool_call("index_data", {"weird": Opaque()})
        parsed = json.loads(MESSAGE_PATTERN.match(message).group(2))

        assert parsed == {"weird": "opaque-value"}

    def test_empty_params_keep_the_no_parameters_wording(self, format_tool_call):
        assert format_tool_call("list_indexes", {}) == "Calling tool 'list_indexes' with no parameters"
