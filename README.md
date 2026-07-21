# elitea_core
ELITEA plugin with core platform functionality


## Running Tests

The `elitea_core` plugin has a pytest-based test suite isolated from the Pylon runtime.

```bash
# From the elitea_core plugin directory
cd centry/pylon_main/plugins/elitea_core

python3 tests/run_tests.py -v              # All tests
python3 tests/run_tests.py -m unit -v     # Unit tests only
python3 tests/run_tests.py -m integration # Integration tests only
python3 tests/run_tests.py unit/utils/test_tool_call_dedup.py -v  # Specific file
```

## Test Structure

```
tests/
├── run_tests.py          # Entry point (installs stubs before pytest)
├── fixtures/             # Reusable fake ORM objects
├── unit/                 # Pure logic tests
└── integration/          # Module-level tests with stubs
```

## Adding Tests

- **Unit tests**: `tests/unit/test_<module>.py` — pure functions
- **Integration tests**: `tests/integration/test_<feature>.py` — modules with imports

Use `tests/run_tests.py` (not raw `pytest`) to ensure Pylon stubs are loaded.

See `tests/README.md` for a detailed guide.
