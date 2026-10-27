"""EL-7011 US-17: the save-time risk scan (R1.1-R1.3, R2.1, R2.2) of utils/schedule_risk.py.

Owner CR 2026-10-21 (risk review): `scan_schedule` splits the findings into `blocking`, `sensitive`
and `notices`, and gives the `fingerprint` of the sensitive tools. The module loads alone behind
stand-ins for `tools`, the models and the owner expansion.
"""
import hashlib
import json
import pathlib
import sys
import types

import pytest
from fixtures.helpers import load_module_with_stubs

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PKG = 'plugins.elitea_core'
VERSION_ID = 7
DEFAULT_INFO = {'badge': 'needs_approval', 'effect': 'This tool can change data outside ELITEA.'}


class ConfigurationExpandError(Exception):
    def __init__(self, errors):
        super().__init__(str(errors))
        self.errors = errors


def _error(field, error_type):
    return {'loc': (field,), 'msg': json.dumps({'error_type': error_type})}


class FakeSession:
    """Stands in for the SQLite stub session; the stub ApplicationVersion has no meta or instructions."""

    def __init__(self, version):
        self._version = version

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, _model, version_id):
        return self._version if version_id == VERSION_ID else None


def _tool(type_='jira', name='Jira', settings=None, meta=None):
    return types.SimpleNamespace(type=type_, name=name, settings=settings or {}, meta=meta or {})


def _version(agent_type='openai', meta=None, instructions=None, tools=(), variables=()):
    return types.SimpleNamespace(
        id=VERSION_ID, application_id=3, agent_type=agent_type,
        meta=meta or {}, instructions=instructions, tools=list(tools), variables=list(variables),
    )


def _content_digest(instructions, variables, tools):
    """The sha256 of the run content: instructions, sorted (name, value) variables and sorted raw toolkit settings."""
    content = {
        'instructions': instructions, 'variables': sorted(json.dumps([v.name, v.value], sort_keys=True) for v in variables),
        'toolkits': sorted(json.dumps([t.name, t.type, t.settings], sort_keys=True) for t in tools),
    }
    return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()


@pytest.fixture
def scan(isolated_sys_modules):
    state = types.SimpleNamespace(version=None, expand_errors={}, expanded={}, calls=[], sensitive=set(),
                                  tool_info={})

    def expand_toolkit_settings(type_, settings, project_id, user_id):
        state.calls.append((type_, project_id, user_id))
        if type_ in state.expand_errors:
            raise ConfigurationExpandError(state.expand_errors[type_])
        return {**settings, **state.expanded.get(type_, {})}

    for name in (PKG, f'{PKG}.models', f'{PKG}.utils'):
        sys.modules[name] = types.ModuleType(name)
        sys.modules[name].__path__ = []
    tools = types.ModuleType('tools')
    tools.db = types.SimpleNamespace(get_session=lambda project_id: FakeSession(state.version))
    sys.modules['tools'] = tools
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = types.SimpleNamespace(exception=lambda *a, **k: None, warning=lambda *a, **k: None)
    sys.modules.setdefault('pylon', types.ModuleType('pylon'))
    sys.modules.setdefault('pylon.core', types.ModuleType('pylon.core'))
    sys.modules['pylon.core.tools'] = pylon_tools
    models_all = types.ModuleType(f'{PKG}.models.all')
    models_all.ApplicationVersion = type('ApplicationVersion', (), {})
    sys.modules[models_all.__name__] = models_all
    security = types.ModuleType(f'{PKG}.utils.toolkit_security')
    security.is_tool_sensitive = lambda type_, tool_name: (type_, tool_name) in state.sensitive
    security.sensitive_tool_info = lambda type_, tool_name: state.tool_info.get((type_, tool_name), DEFAULT_INFO)
    sys.modules[security.__name__] = security
    app_tools = types.ModuleType(f'{PKG}.utils.application_tools')
    app_tools.ConfigurationExpandError = ConfigurationExpandError
    app_tools.expand_toolkit_settings = expand_toolkit_settings
    sys.modules[app_tools.__name__] = app_tools
    load_module_with_stubs(PLUGIN_ROOT / 'utils' / 'pipeline_utils.py', f'{PKG}.utils.pipeline_utils')
    module = load_module_with_stubs(PLUGIN_ROOT / 'utils' / 'schedule_risk.py', f'{PKG}.utils.schedule_risk')
    state.review = lambda version_id=VERSION_ID: module.scan_schedule(12, version_id, 42)
    state.run = lambda version_id=VERSION_ID: _flat(state.review(version_id))
    state.module = module
    return state


def _flat(review):
    """Every finding in the order blocking, sensitive, notices; the fix link has its own test."""
    return [{key: value for key, value in item.items() if key != 'fix'}
            for item in review['blocking'] + review['sensitive'] + review['notices']]


def _codes(warnings):
    return [w['code'] for w in warnings]


PIPELINE = """
nodes:
  - id: Review
    type: hitl
  - id: Show
    type: printer
  - id: Think
    type: llm
"""


class TestEL7011R1HumanSteps:
    def test_r1_1_agent_with_ask_user_gets_one_warning(self, scan):
        scan.version = _version(meta={'internal_tools': ['ask_user', 'other']})
        [warning] = scan.run()
        assert warning['code'] == 'human_input_step'

    def test_r1_2_pipeline_gets_one_warning_per_human_node(self, scan):
        scan.version = _version('pipeline', instructions=PIPELINE)
        warnings = scan.run()
        assert _codes(warnings) == ['human_input_step'] * 2
        assert [w['step'] for w in warnings] == ['Review', 'Show']
        assert [w['step_label'] for w in warnings] == ['Review', 'Show']

    def test_r1_1_version_without_human_steps_has_no_warning(self, scan):
        scan.version = _version(meta={'internal_tools': ['python']})
        assert scan.run() == []
        scan.version = _version('pipeline', instructions='nodes:\n  - id: A\n    type: llm\n  - id: B\n    type: tool\n')
        assert scan.run() == []

    @pytest.mark.parametrize('instructions', ['nodes: [unclosed', 'just text', None])
    def test_r1_3_unparsable_pipeline_gives_one_scan_incomplete(self, scan, instructions):
        scan.version = _version('pipeline', instructions=instructions)
        assert _codes(scan.run()) == ['scan_incomplete']

    @pytest.mark.parametrize('version_id', [None, 999])
    def test_el7011_r1_3_missing_version_returns_no_warning(self, scan, version_id):
        scan.version = _version()
        assert scan.run(version_id=version_id) == []

    def test_el7011_r1_3_expansion_error_of_one_toolkit_keeps_the_other_warnings(self, scan):
        scan.version = _version(
            meta={'internal_tools': ['ask_user']},
            tools=[_tool('jira', 'Jira'), _tool('git', 'Git'), _tool('mcp', 'Remote')],
        )
        scan.expand_errors = {'jira': [{'loc': ('x',), 'msg': 'boom'}], 'git': [_error('a', 'credential_not_found')]}
        warnings = scan.run()
        assert _codes(warnings) == ['human_input_step', 'missing_credential', 'personal_signin_required', 'scan_incomplete']
        assert warnings[3]['toolkit'] == 'Jira'


class TestEL7011R2Credentials:
    def test_r2_1_one_warning_per_toolkit_for_each_error_type(self, scan):
        scan.version = _version(tools=[_tool('jira', 'Jira'), _tool('confluence', 'Confluence'), _tool('git', 'Git')])
        scan.expand_errors = {
            'jira': [_error('a', 'credential_not_found'), _error('b', 'credential_not_found')],
            'confluence': [_error('a', 'private_credential_not_found')],
            'git': [_error('a', 'credential_type_mismatch')],
        }
        warnings = scan.run()
        assert _codes(warnings) == ['missing_credential'] * 3
        assert [w['toolkit'] for w in warnings] == ['Jira', 'Confluence', 'Git']

    def test_r2_1_expands_for_the_owner_in_the_project(self, scan):
        scan.version = _version(tools=[_tool()])
        scan.run()
        assert scan.calls == [('jira', 12, 42)]

    def test_r2_1_clean_version_has_no_warning(self, scan):
        scan.version = _version(tools=[_tool()])
        assert scan.run() == []


class TestEL7011R2PersonalToken:
    @pytest.mark.parametrize('tool', [
        _tool('mcp', 'Remote'),
        _tool('mcp_github', 'Hub', meta={'mcp': True}),
        _tool('openapi', 'Api', settings={'oauth_discovery_endpoint': 'https://idp'}),
    ])
    def test_el7011_cr_mcp_and_inline_openapi_endpoint_block_the_save(self, scan, tool):
        scan.version = _version(tools=[tool])
        [warning] = scan.review()['blocking']
        assert warning == {
            'code': 'personal_signin_required', 'message': warning['message'], 'toolkit': tool.name,
            'toolkit_type': tool.type, 'reason': 'needs a personal sign-in',
            'fix': {'entity_type': 'agent', 'id': 3, 'version_id': VERSION_ID},
        }

    @pytest.mark.parametrize('type_', ['sharepoint', 'openapi'])
    def test_r2_2_credential_with_discovery_endpoint_blocks(self, scan, type_):
        scan.version = _version(tools=[_tool(type_, 'Docs', settings={f'{type_}_configuration': {'elitea_title': 'c'}})])
        scan.expanded[type_] = {f'{type_}_configuration': {'oauth_discovery_endpoint': 'https://idp'}}
        assert _codes(scan.run()) == ['personal_signin_required']

    @pytest.mark.parametrize('type_', ['sharepoint', 'openapi'])
    def test_r2_2_app_only_credential_does_not_warn(self, scan, type_):
        scan.version = _version(tools=[_tool(type_, 'Docs', settings={f'{type_}_configuration': {'elitea_title': 'c'}})])
        scan.expanded[type_] = {f'{type_}_configuration': {'client_id': 'x'}}
        assert scan.run() == []

    def test_r2_2_toolkit_with_missing_credential_gets_only_the_credential_warning(self, scan):
        scan.version = _version(tools=[_tool('openapi', 'Api', settings={'oauth_discovery_endpoint': 'u'})])
        scan.expand_errors['openapi'] = [_error('a', 'credential_not_found')]
        assert _codes(scan.run()) == ['missing_credential']

    def test_r2_2_warning_text_holds_no_secret(self, scan):
        scan.version = _version(tools=[_tool('openapi', 'Api', settings={'oauth_discovery_endpoint': 'https://idp', 'token': 's3cret'})])
        assert 's3cret' not in json.dumps(scan.review())


class TestEL7011R2SensitiveActions:
    def test_el7011_r2_3_sensitive_selected_tool_gives_one_needs_approval_warning(self, scan):
        scan.sensitive = {('jira', 'delete_issue')}
        scan.version = _version(tools=[_tool(settings={'selected_tools': ['get_issue', 'delete_issue']})])
        [warning] = scan.run()
        assert warning == {
            'code': 'sensitive_action', 'message': warning['message'], 'toolkit': 'Jira', 'toolkit_type': 'jira',
            'tool': 'delete_issue', 'risk_level': 'needs_approval', **DEFAULT_INFO,
        }

    def test_el7011_r2_3_unlisted_tool_gives_no_warning(self, scan):
        scan.sensitive = {('jira', 'delete_issue')}
        scan.version = _version(tools=[_tool(settings={'selected_tools': ['get_issue']})])
        assert scan.run() == []

    def test_el7011_r2_3_no_policy_gives_no_warning(self, scan):
        scan.version = _version(tools=[_tool(settings={'selected_tools': ['delete_issue']})])
        assert scan.run() == []


def test_el7011_cr_the_scan_no_longer_owns_the_run_without_asking_check(scan):
    """Owner CR 2026-10-21: the project setting replaces the secret; utils/schedule_settings.py owns the check."""
    assert not hasattr(scan.module, 'is_run_without_asking_allowed')
    assert not hasattr(scan.module, 'ALLOW_RUN_WITHOUT_ASKING_SECRET')


class TestEL7011CrRiskReviewSplit:
    """Owner CR 2026-10-21: what blocks a save, what needs a confirmation, what is only a notice."""

    def test_el7011_cr_blocking_findings_carry_the_fix_link_of_the_entity(self, scan):
        scan.version = _version('pipeline', instructions=PIPELINE, tools=[_tool('git', 'Git'), _tool('mcp', 'Remote')])
        scan.expand_errors = {'git': [_error('a', 'credential_not_found')]}
        review = scan.review()
        assert _codes(review['blocking']) == [
            'human_input_step', 'human_input_step', 'missing_credential', 'personal_signin_required']
        assert all(item['fix'] == {'entity_type': 'pipeline', 'id': 3, 'version_id': VERSION_ID}
                   for item in review['blocking'])
        assert review['sensitive'] == [] and review['notices'] == []

    def test_el7011_cr_sensitive_tools_take_badge_and_effect_from_the_policy(self, scan):
        scan.sensitive = {('outlook', 'send_email')}
        scan.tool_info = {('outlook', 'send_email'): {'badge': 'sends_messages', 'effect': 'Sends e-mail'}}
        scan.version = _version(tools=[_tool('outlook', 'Outlook', settings={'selected_tools': ['send_email']})])
        [item] = scan.review()['sensitive']
        assert (item['toolkit_type'], item['tool'], item['badge'], item['effect']) == (
            'outlook', 'send_email', 'sends_messages', 'Sends e-mail')

    def test_el7011_cr_a_scan_failure_is_a_notice_and_the_scan_is_incomplete(self, scan):
        scan.version = _version(tools=[_tool('jira', 'Jira')])
        scan.expand_errors = {'jira': [{'loc': ('x',), 'msg': 'boom'}]}
        review = scan.review()
        assert review['blocking'] == [] and _codes(review['notices']) == ['scan_incomplete']
        assert scan.module.scan_complete(review) is False
        scan.expand_errors = {}
        assert scan.module.scan_complete(scan.review()) is True

    def test_el7011_cr_fingerprint_is_sha256_of_version_and_sorted_sensitive_tools(self, scan):
        scan.sensitive = {('jira', 'delete_issue'), ('jira', 'create_issue')}
        tool = _tool(settings={'selected_tools': ['delete_issue', 'create_issue']})
        scan.version = _version(tools=[tool])
        digest = _content_digest(None, [], [tool])
        expected = hashlib.sha256(json.dumps(
            [VERSION_ID, [['Jira', 'create_issue'], ['Jira', 'delete_issue']], digest]).encode()).hexdigest()
        assert scan.review()['fingerprint'] == expected

    def test_same_name_variables_with_none_and_text_give_a_stable_digest(self, scan):
        first = [types.SimpleNamespace(name='to', value=None), types.SimpleNamespace(name='to', value='a')]
        scan.version = _version(variables=first)
        review = scan.review()
        scan.version = _version(variables=list(reversed(first)))
        assert scan.module.scan_complete(review) is True
        assert scan.review()['fingerprint'] == review['fingerprint']

    def test_el7011_cr_fingerprint_changes_when_a_sensitive_tool_is_added(self, scan):
        scan.sensitive = {('jira', 'delete_issue'), ('jira', 'create_issue')}
        scan.version = _version(tools=[_tool(settings={'selected_tools': ['delete_issue']})])
        before = scan.review()['fingerprint']
        scan.version.tools = [_tool(settings={'selected_tools': ['delete_issue', 'create_issue']})]
        assert scan.review()['fingerprint'] != before

    @pytest.mark.parametrize('change', [
        lambda v: setattr(v, 'instructions', 'Send the report to everyone'),
        lambda v: setattr(v, 'variables', [types.SimpleNamespace(name='to', value='boss@example.com')]),
        lambda v: v.tools[0].settings.update(url='https://evil.example'),
    ], ids=['instructions', 'variables', 'toolkit-settings'])
    def test_the_fingerprint_changes_when_the_run_content_changes(self, scan, change):
        scan.sensitive = {('jira', 'delete_issue')}
        scan.version = _version(instructions='Send the report', tools=[_tool(settings={'selected_tools': ['delete_issue']})],
                                variables=[types.SimpleNamespace(name='to', value='me@example.com')])
        before = scan.review()['fingerprint']
        change(scan.version)
        assert scan.review()['fingerprint'] != before

    def test_the_fingerprint_hashes_the_settings_before_the_secret_expansion(self, scan):
        scan.sensitive = {('jira', 'delete_issue')}
        scan.version = _version(tools=[_tool(settings={'selected_tools': ['delete_issue']})])
        before = scan.review()['fingerprint']
        scan.expanded = {'jira': {'password': 'rotated-secret'}}
        assert scan.review()['fingerprint'] == before

    def test_el7011_cr_missing_version_gives_an_empty_review(self, scan):
        review = scan.review(version_id=None)
        assert (review['blocking'], review['sensitive'], review['notices']) == ([], [], [])
        assert isinstance(review['fingerprint'], str)


def _personal(type_='github', name='GitHub', private=True, **settings):
    return _tool(type_, name, settings={f'{type_}_configuration': {'elitea_title': 'mine', 'private': private},
                                        **settings})


class TestEL7011CrPersonalCredential:
    """Owner decision 2026-10-21: a personal credential is "warn and confirm", not silent and not blocked."""

    @pytest.mark.parametrize('type_, name, label', [
        ('github', 'GitHub', 'GitHub'), ('jira', 'Jira', 'Jira'), ('confluence', 'Docs', 'Confluence')])
    def test_el7011_cr_a_personal_credential_is_a_sensitive_item(self, scan, type_, name, label):
        scan.version = _version(tools=[_personal(type_, name)])
        review = scan.review()
        assert review['blocking'] == [] and review['notices'] == []
        assert review['sensitive'] == [{
            'code': 'personal_credential', 'message': f'Toolkit "{name}" runs with the owner\'s personal {label} credential',
            'toolkit': name, 'toolkit_type': type_, 'tool': None, 'risk_level': 'needs_approval',
            'badge': 'personal_credential', 'effect': f"Runs with the owner's personal {label} credential.",
        }]

    def test_el7011_cr_a_shared_credential_adds_nothing(self, scan):
        scan.version = _version(tools=[_personal(private=False)])
        assert scan.run() == []

    def test_el7011_cr_the_personal_credential_is_in_the_fingerprint_as_toolkit_star(self, scan):
        tool = _personal()
        scan.version = _version(tools=[tool])
        expected = hashlib.sha256(json.dumps(
            [VERSION_ID, [['GitHub', '*']], _content_digest(None, [], [tool])]).encode()).hexdigest()
        assert scan.review()['fingerprint'] == expected

    def test_el7011_cr_personal_credential_and_sensitive_tool_are_both_listed(self, scan):
        scan.sensitive = {('github', 'delete_file')}
        scan.version = _version(tools=[_personal(selected_tools=['delete_file'])])
        assert [(i['code'], i['tool']) for i in scan.review()['sensitive']] == [
            ('personal_credential', None), ('sensitive_action', 'delete_file')]

    def test_el7011_cr_an_interactive_sign_in_still_blocks_and_adds_no_personal_item(self, scan):
        scan.version = _version(tools=[_personal('sharepoint', 'Docs')])
        scan.expanded['sharepoint'] = {'sharepoint_configuration': {'oauth_discovery_endpoint': 'https://idp'}}
        review = scan.review()
        assert _codes(review['blocking']) == ['personal_signin_required'] and review['sensitive'] == []

    def test_el7011_cr_a_missing_personal_credential_blocks_without_a_personal_item(self, scan):
        scan.version = _version(tools=[_personal('jira', 'Jira')])
        scan.expand_errors = {'jira': [_error('a', 'private_credential_not_found')]}
        review = scan.review()
        assert _codes(review['blocking']) == ['missing_credential'] and review['sensitive'] == []
