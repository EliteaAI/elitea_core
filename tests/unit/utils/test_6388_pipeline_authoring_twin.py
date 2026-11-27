"""#6388: utils/pipeline_authoring is a byte-identical copy of elitea-sdk's runtime/pipeline.

pylon_main does not import the SDK, so the Builder's reference and validator run from this
copy. A file changed in one repo but not the other fails here when elitea-sdk is checked out
next to this plugin, the same way the input-guardrail twin is checked.
"""
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PACKAGE_DIR = PLUGIN_ROOT / 'utils' / 'pipeline_authoring'
SDK_RELATIVE = 'elitea_sdk/runtime/pipeline'
# Every file but __init__.py, whose imports and docstring differ by design
TWIN_FILES = sorted(
    p.relative_to(PACKAGE_DIR).as_posix()
    for p in PACKAGE_DIR.rglob('*')
    if p.is_file() and p.name != '__init__.py' and '__pycache__' not in p.parts
)


def _sdk_dirs():
    roots = [PLUGIN_ROOT.parent / name for name in ('elitea-sdk', 'elitea_sdk', 'Alita_SDK')]
    found = []
    for root in roots:
        if root.is_dir():
            found += [p for p in (root / SDK_RELATIVE, *root.glob(f'*/{SDK_RELATIVE}')) if p.is_dir()]
    return found


def load_package(alias='pipeline_authoring_under_test'):
    """The package on its own; it needs nothing from pylon."""
    spec = importlib.util.spec_from_file_location(
        alias, PACKAGE_DIR / '__init__.py', submodule_search_locations=[str(PACKAGE_DIR)])
    module = importlib.util.module_from_spec(spec)
    sys.modules[alias] = module
    spec.loader.exec_module(module)
    return module


def test_twin_has_the_expected_files():
    for name in ('registry.py', 'validator.py', 'knowledge.py', 'builder.py', 'node_registry.yaml',
                 'knowledge/overview.md', 'knowledge/node/llm.md'):
        assert name in TWIN_FILES, name


def test_twin_matches_the_sdk_copy_when_checked_out_alongside():
    sdks = _sdk_dirs()
    if not sdks:
        pytest.skip('elitea-sdk is not checked out next to this plugin')
    for sdk in sdks:
        sdk_files = sorted(
            p.relative_to(sdk).as_posix() for p in sdk.rglob('*')
            if p.is_file() and p.name != '__init__.py' and '__pycache__' not in p.parts
        )
        assert sdk_files == TWIN_FILES, f'files differ from {sdk}'
        for name in TWIN_FILES:
            mine = (PACKAGE_DIR / name).read_bytes().replace(b'\r\n', b'\n')
            theirs = (sdk / name).read_bytes().replace(b'\r\n', b'\n')
            assert mine == theirs, f'{name} differs from {sdk / name}; copy it from the SDK'


def test_package_loads_and_serves_without_pylon():
    package = load_package()
    assert 'llm' in package.node_types('current')
    assert package.builder_reference().startswith('# Pipeline YAML: the shape')
    assert package.validate_for_builder('nodes: [')['valid'] is False
