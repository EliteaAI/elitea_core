"""EL-7011 US-17: a HITL pipeline schedule saves with one warning and keeps `enabled` (R1.2, R2.4).

Integration of the real schedule handler and the real risk scan. Only the edges are fake: the
SQLite rows of the CRUD harness, the pipeline version the scan reads, and the owner expansion.

Run via:
    python tests/run_tests.py integration/test_7011_save_warnings.py -v
"""
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs
from unit.test_7011_schedule_crud_api import PACKAGE, PLUGIN_ROOT, pipeline_body
from unit.test_7011_schedule_crud_api import harness as _crud_harness

pytestmark = pytest.mark.integration

harness = _crud_harness  # the CRUD harness fixture, shared


@pytest.fixture
def isolated_sys_modules():
    """The CRUD harness stubs sys.modules; restore it after each test (the unit conftest is not visible here)."""
    original = sys.modules.copy()
    yield sys.modules
    sys.modules.clear()
    sys.modules.update(original)


PIPELINE_YAML = """
nodes:
  - id: Review
    type: hitl
  - id: Think
    type: llm
"""


class FakeScanSession:
    def __init__(self, version):
        self.version = version

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, _model, _version_id):
        return self.version


@pytest.fixture
def real_scan(harness):
    """Swap the harness scan stub for the real module, fed by one HITL pipeline version."""
    version = types.SimpleNamespace(agent_type='pipeline', instructions=PIPELINE_YAML, meta={}, tools=[])
    sys.modules[f'{PACKAGE}.models.all'] = types.SimpleNamespace(ApplicationVersion=type('ApplicationVersion', (), {}))
    sys.modules[f'{PACKAGE}.utils.application_tools'] = types.SimpleNamespace(
        ConfigurationExpandError=Exception, expand_toolkit_settings=lambda *args: {},
    )
    sys.modules['tools'].VaultClient = lambda project_id: types.SimpleNamespace(get_all_secrets=dict)
    sys.modules['tools'].db = types.SimpleNamespace(get_session=lambda project_id: FakeScanSession(version))
    policy = {'toolkit_security': {'sensitive_tools': {'jira': ['delete_issue']}}}
    sys.modules['tools'].this = types.SimpleNamespace(module=types.SimpleNamespace(
        descriptor=types.SimpleNamespace(config=types.SimpleNamespace(get=lambda key, default=None: policy.get(key, default))),
    ))
    load_module_with_stubs(PLUGIN_ROOT / 'utils/toolkit_security.py', f'{PACKAGE}.utils.toolkit_security')
    load_module_with_stubs(PLUGIN_ROOT / 'utils/pipeline_utils.py', f'{PACKAGE}.utils.pipeline_utils')
    sys.modules[f'{PACKAGE}.utils.schedule_risk'] = load_module_with_stubs(
        PLUGIN_ROOT / 'utils/schedule_risk.py', f'{PACKAGE}.utils.schedule_risk',
    )
    harness.scan_version = version
    return harness


# R1.2, R2.4 (TC-US17-01, TC-US17-02, TC-US17-03, TC-US17-04, TC-US17-10, TC-US17-11)
@pytest.mark.parametrize('enabled', [True, False])
def test_7011_r1_2_hitl_pipeline_save_returns_one_warning_and_keeps_enabled(real_scan, enabled):
    body, status = real_scan.call('post', pipeline_body(enabled=enabled))
    assert status == 201
    assert [(w['code'], w['step']) for w in body['warnings']] == [('human_input_step', 'Review')]
    assert 'Review' in body['warnings'][0]['message']
    assert real_scan.row(body['id']).enabled is enabled

    updated, status = real_scan.call('put', {'name': 'Renamed'}, schedule_id=body['id'])
    assert status == 200
    assert [w['step'] for w in updated['warnings']] == ['Review']
    assert real_scan.row(body['id']).enabled is enabled


# R2.3 (TC-US17-05)
def test_el7011_r2_3_toolkit_with_sensitive_tool_saves_with_one_warning_and_keeps_enabled(real_scan):
    real_scan.scan_version.agent_type = 'openai'
    real_scan.scan_version.tools = [types.SimpleNamespace(
        type='jira', name='Jira', meta={}, settings={'selected_tools': ['get_issue', 'delete_issue']},
    )]
    body, status = real_scan.call('post', pipeline_body(enabled=True))
    assert status == 201
    assert [(w['code'], w['toolkit'], w['tool'], w['risk_level']) for w in body['warnings']] == [
        ('sensitive_action', 'Jira', 'delete_issue', 'needs_approval'),
    ]
    assert real_scan.row(body['id']).enabled is True
