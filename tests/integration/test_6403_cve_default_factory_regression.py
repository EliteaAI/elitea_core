"""Issue #6403 - GHSA-386q-5hp3-95m9 / CVE-2026-54653 regression test.

On datamodel-code-generator <=0.60.1, a JSON Schema property carrying a literal
"default_factory" key gets that value interpolated UNESCAPED into the generated
`Field(default_factory=<value>)` call, executed at class-definition time. Fixed in
0.60.2, which now validates the value (only "dict"/"list"/"set" accepted) and
raises during parsing instead.

This test only calls JsonSchemaParser().parse() - the generated source is never
compile()'d or exec()'d, so the injected payload is never actually run.

Run via:
    python tests/run_tests.py integration/test_6403_cve_default_factory_regression.py -v
"""

import json
from importlib.metadata import version as _pkg_version

import pytest

# Deferred: CI's unit-only job doesn't install datamodel-code-generator, and pytest
# imports every test file at collection time regardless of -m unit marker filtering.
pytest.importorskip("datamodel_code_generator")
from datamodel_code_generator import DataModelType, PythonVersion  # noqa: E402
from datamodel_code_generator.model import get_data_model_types  # noqa: E402
from datamodel_code_generator.parser.jsonschema import JsonSchemaParser  # noqa: E402


MALICIOUS_EXPR = "__import__('os').system('id')"

MALICIOUS_SCHEMA = json.dumps({
    "type": "object",
    "properties": {
        "safe": {"type": "string"},
        "pwned": {
            "type": "string",
            "default_factory": MALICIOUS_EXPR,
        },
    },
    "required": ["safe"],
})


def _version_tuple():
    return tuple(int(part) for part in _pkg_version("datamodel-code-generator").split(".")[:3])


def _make_parser():
    data_model_types = get_data_model_types(
        DataModelType.PydanticV2BaseModel,
        target_python_version=PythonVersion.PY_312,
    )
    return JsonSchemaParser(
        MALICIOUS_SCHEMA,
        data_model_type=data_model_types.data_model,
        data_model_root_type=data_model_types.root_model,
        data_model_field_type=data_model_types.field_model,
        data_type_manager_type=data_model_types.data_type_manager,
        dump_resolve_reference_action=data_model_types.dump_resolve_reference_action,
        target_python_version=PythonVersion.PY_312,
        remove_special_field_name_prefix=True,
        allow_population_by_field_name=True,
    )


class TestCveDefaultFactoryRegression:
    def test_default_factory_injection(self):
        parser = _make_parser()

        if _version_tuple() < (0, 60, 2):
            generated = parser.parse()
            # Documents the still-open vulnerability on the pinned version: the
            # raw expression lands unescaped (no repr/quoting) in the output.
            assert f"default_factory={MALICIOUS_EXPR}" in generated
        else:
            # 0.60.2 rejects any default_factory value that isn't dict/list/set.
            with pytest.raises(Exception):
                parser.parse()
