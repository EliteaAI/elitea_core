"""EL-7011 US-17 with the owner CR 2026-10-21 (risk review): a HITL pipeline is refused with
`schedule_blocked`; a sensitive tool needs the acknowledgement, then saves with one warning and keeps
`enabled` (R1.2, R2.3, R2.4).

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
    version = types.SimpleNamespace(application_id=2, agent_type='pipeline', instructions=PIPELINE_YAML, meta={}, tools=[])
    sys.modules[f'{PACKAGE}.models.all'] = types.SimpleNamespace(ApplicationVersion=type('ApplicationVersion', (), {}))
    sys.modules[f'{PACKAGE}.utils.application_tools'] = types.SimpleNamespace(
        ConfigurationExpandError=Exception, expand_toolkit_settings=lambda *args: {},
    )
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


def _count(harness):
    with harness.session_factory() as session:
        return session.query(harness.EntitySchedule).count()


# R1.2 with the owner CR (TC-US17-01, TC-US17-02): a human-input step blocks the save
@pytest.mark.parametrize('enabled', [True, False])
def test_el7011_cr_hitl_pipeline_save_is_refused_with_the_step(real_scan, enabled):
    body, status = real_scan.call('post', pipeline_body(enabled=enabled))
    assert status == 409 and body['code'] == 'schedule_blocked'
    assert [(w['code'], w['step'], w['step_label']) for w in body['blocking']] == [
        ('human_input_step', 'Review', 'Review')]
    assert body['blocking'][0]['fix'] == {'entity_type': 'pipeline', 'id': 2, 'version_id': 11}
    assert _count(real_scan) == 0


def _sensitive_agent(harness):
    harness.scan_version.agent_type = 'openai'
    harness.scan_version.tools = [types.SimpleNamespace(
        type='jira', name='Jira', meta={}, settings={'selected_tools': ['get_issue', 'delete_issue']},
    )]


# R2.3, R2.4 with the owner CR (TC-US17-05, TC-US17-10, TC-US17-11)
@pytest.mark.parametrize('enabled', [True, False])
def test_el7011_cr_sensitive_tool_needs_the_ack_then_saves_with_one_warning(real_scan, enabled):
    _sensitive_agent(real_scan)
    refused, status = real_scan.call('post', pipeline_body(enabled=enabled))
    assert status == 409 and refused['code'] == 'risk_confirmation_required'
    assert [(w['toolkit'], w['tool'], w['badge']) for w in refused['sensitive']] == [
        ('Jira', 'delete_issue', 'needs_approval')]
    ack = {'fingerprint': refused['fingerprint'], 'confirmed_name': ''}
    body, status = real_scan.call('post', pipeline_body(enabled=enabled, risk_ack=ack))
    assert status == 201
    assert [(w['code'], w['toolkit'], w['tool'], w['risk_level']) for w in body['warnings']] == [
        ('sensitive_action', 'Jira', 'delete_issue', 'needs_approval'),
    ]
    assert real_scan.row(body['id']).enabled is enabled


# Owner CR: the dry-run check of the real scan answers the same fingerprint the save checks
def test_el7011_cr_check_gives_the_fingerprint_the_save_accepts(real_scan):
    _sensitive_agent(real_scan)
    check, status = real_scan.call('post', {'entity_type': 'pipeline', 'application_id': 2, 'version_id': 11},
                                   suffix='/check')
    assert status == 200 and check['blocking'] == [] and len(check['sensitive']) == 1
    ack = {'fingerprint': check['fingerprint'], 'confirmed_name': 'Nightly'}
    _, status = real_scan.call('post', pipeline_body(risk_ack=ack))
    assert status == 201
