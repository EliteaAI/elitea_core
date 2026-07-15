# elitea_core Test Suite

Pytest-based test framework for the elitea_core plugin, isolated from Pylon runtime dependencies.

## Quick Start

```bash
# From plugin root (elitea_core/)
python3 tests/run_tests.py -v              # Run all tests
python3 tests/run_tests.py -m unit -v      # Run only unit tests
python3 tests/run_tests.py -m integration  # Run only integration tests
python3 tests/run_tests.py unit/utils/test_tool_call_dedup.py -v  # Run specific file
```

## Directory Structure

```
tests/
├── run_tests.py          # Entry point - installs stubs before pytest
├── pytest.ini            # Pytest configuration and markers
├── conftest.py           # Root fixtures (plugin_root, utils_path, etc.)
├── fixtures/
│   ├── models.py         # Reusable fake ORM objects (FakeTool, FakeVersion, FakeSession)
│   └── helpers.py        # Module loading utilities (load_module_with_stubs)
├── stubs/
│   ├── pylon_runtime.py  # Pylon core stubs (log, web, module)
│   ├── tools.py          # tools package stubs (db, auth, config)
│   └── orm.py            # SQLAlchemy-like stubs
├── unit/
│   ├── conftest.py       # Unit test fixtures
│   └── utils/            # Tests for utils/ modules
│       └── test_*.py
└── integration/
    ├── conftest.py       # Integration test fixtures (fake_db_session)
    └── test_*.py         # Tests requiring module-level stubbing
```

## Test Categories

| Marker | Directory | Description |
|--------|-----------|-------------|
| `unit` | `unit/` | Pure logic tests, no external dependencies |
| `integration` | `integration/` | Tests loading real modules with stubs |

Tests are auto-marked based on their directory location.

## Writing Tests

### Unit Tests

For pure functions with no imports from plugin modules:

```python
# tests/unit/utils/test_my_module.py
import pytest
import pathlib
import sys

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module

@pytest.fixture(scope='module')
def my_module():
    return load_utils_module(
        TESTS_DIR.parent / 'utils',
        'my_module_name'
    )

class TestMyFunction:
    def test_basic_case(self, my_module):
        result = my_module.my_function([1, 2, 3])
        assert result == expected
```

### Integration Tests

For modules with complex imports requiring stubs:

```python
# tests/integration/test_feature.py
import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

@pytest.fixture(scope='module')
def feature_module():
    # Install stubs for dependencies
    models = types.ModuleType("plugins.elitea_core.models.all")
    models.MyModel = type("MyModel", (), {})
    sys.modules["plugins.elitea_core.models.all"] = models
    
    # Load the module under test
    spec = importlib.util.spec_from_file_location(
        "plugins.elitea_core.utils.feature",
        PLUGIN_ROOT / "utils" / "feature.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module

class TestFeature:
    def test_something(self, feature_module):
        assert feature_module.do_something() == expected
```

### Using Fake ORM Objects

```python
from fixtures.models import FakeTool, FakeVersion, FakeSession

def test_with_fake_db():
    registry = {
        1: FakeVersion(1, tools=[FakeTool(2, 2)]),
        2: FakeVersion(2, tools=[]),
    }
    session = FakeSession(registry)
    
    # Your test logic using session.query()
    result = session.query(None).filter(...).first()
```

## How Isolation Works

The test runner (`run_tests.py`) solves the "cascading import" problem:

1. **Stub injection**: Before pytest loads, minimal stubs for `pylon`, `tools`, etc. are injected into `sys.modules`
2. **No `__init__.py`**: The `tests/` directory has no `__init__.py` files, preventing pytest from treating it as part of the `elitea_core` package
3. **importlib mode**: Pytest uses `--import-mode=importlib` to avoid parent package discovery
4. **Per-test stubs**: Each test fixture can add additional stubs for its specific module dependencies

## Adding New Stubs

If a test fails with `ImportError: cannot import name 'X' from 'tools'`:

1. Check what the module under test imports
2. Add the stub to `run_tests.py` in `install_minimal_stubs()`:

```python
tools.new_attribute = types.SimpleNamespace(method=lambda *a: None)
```

Or add module-specific stubs in your test fixture.

## Pytest Markers

```bash
python3 tests/run_tests.py -m unit           # Only unit tests
python3 tests/run_tests.py -m integration    # Only integration tests
python3 tests/run_tests.py -m "not slow"     # Skip slow tests
```

## Troubleshooting

**ModuleNotFoundError: No module named 'pylon'**
- Run tests via `run_tests.py`, not directly with `pytest`

**Test passes locally but fails in CI**
- Check if CI has different Python version
- Verify all stubs are in `run_tests.py`

**Import errors from parent plugin**
- Ensure no `__init__.py` exists in tests/ or subdirectories
- Check `--import-mode=importlib` is set
