"""#7383 review fixes: the Builder speaks the real endpoint shapes.

The fake endpoints here validate payloads the way the real models do (ApplicationCreateModel needs a one-version
`versions` list; skill GET nests version content under `version_details`; skill PUT takes skill fields top-level
and version content under `version`; agent rename goes through PUT application, not the version)."""
import pytest

PID = 12


class StrictEndpoints:
    def __init__(self):
        self.writes = []
        self.app = {'id': 1, 'name': 'Daily Summary', 'description': 'Posts issues',
                    'versions': [{'id': 10, 'name': 'base', 'status': 'draft'}], 'meta': {}}
        self.version = {'id': 10, 'application_id': 1, 'name': 'base', 'status': 'draft', 'instructions': 'Be brief.',
                        'llm_settings': {'model_name': 'm'}, 'conversation_starters': [], 'agent_type': 'openai',
                        'instructions_sha256': 'a' * 64}
        self.skill = {'id': 4, 'name': 'incident-triage', 'description': 'Triage incidents', 'versions': [],
                      'version_details': {'id': 8, 'name': 'base', 'instructions': 'Step 1.', 'tags': [],
                                          'run_settings': None, 'meta': {}}}
        self.fail_update_version = False

    def __call__(self, op, args):
        if op == 'get_application':
            return dict(self.app)
        if op == 'get_version':
            return dict(self.version)
        if op == 'get_skill':
            return dict(self.skill)
        self.writes.append((op, args))
        if op == 'create_application':
            versions = args.get('versions')
            assert isinstance(versions, list) and len(versions) == 1, 'versions: Field required'
            assert versions[0]['name'] == 'base'
            return {'id': 7, 'name': args['name'], 'version_details': {'id': 70, 'agent_type': versions[0].get(
                'agent_type', 'openai')}}
        if op == 'create_skill':
            versions = args.get('versions')
            assert versions and versions[0]['name'] == 'base' and versions[0]['instructions']
            assert 'instructions' not in args
            return {'id': 5, 'name': args['name'], 'version_details': {'id': 50}}
        if op == 'update_skill':
            assert 'instructions' not in args, "instructions belong under 'version'"
            if args.get('version'):
                assert 'description' not in args or args.get('version_id') is None
            return {'id': 4, 'name': args.get('name', 'incident-triage')}
        if op == 'update_application':
            assert set(args) <= {'application_id', 'name', 'description'}
            return {'id': 1, 'name': args.get('name')}
        if op == 'update_version':
            assert 'name' not in args, 'a rename must not rename the version'
            if self.fail_update_version:
                return {'error': 'conversation_starters cannot exceed 4'}
            return {'ok': True, 'name': 'base'}
        if op == 'patch_instructions':
            return {'name': 'base', 'mcp_backup_version': {'id': 99, 'name': 'mcp-backup-10-x'}}
        return {'ok': True}


@pytest.fixture
def api():
    return StrictEndpoints()


@pytest.fixture
def service(builder, api):
    return builder.service.BuilderService(project_id=PID, user_id=5, call=api, store=builder.service.MemoryStore(),
                                          catalog=builder.catalog.Catalog([]), can_write=True)


def apply(service, proposal):
    assert proposal['status'] in ('ready', 'needs_confirmation'), proposal
    return service.apply(proposal['changeset_id'])


class TestCreate:

    def test_agent_create_sends_one_base_version(self, service, api):
        result = apply(service, service.propose('agent', 'create', {
            'name': 'Reviewer', 'instructions': 'Review code.', 'llm_settings': {'model_name': 'm'}}))
        assert result['status'] == 'applied'
        (op, args), = api.writes
        assert args['name'] == 'Reviewer'
        assert args['versions'][0]['instructions'] == 'Review code.'
        assert args['versions'][0]['llm_settings'] == {'model_name': 'm'}
        assert result['chip'] == {'type': 'agent', 'id': 7, 'version_id': 70, 'name': 'Reviewer'}

    def test_pipeline_create_sets_the_agent_type_on_the_version(self, service, api):
        result = apply(service, service.propose('pipeline', 'create', {'name': 'P', 'instructions': 'nodes: []'}))
        assert api.writes[0][1]['versions'][0]['agent_type'] == 'pipeline'
        assert result['chip']['type'] == 'pipeline'

    def test_skill_create_nests_instructions_in_the_base_version(self, service, api):
        result = apply(service, service.propose('skill', 'create', {
            'name': 'triage', 'description': 'd', 'instructions': 'Do it.'}))
        assert result['status'] == 'applied'
        assert api.writes[0][1]['versions'][0] == {'name': 'base', 'instructions': 'Do it.'}

    def test_skill_create_takes_the_body_as_its_instructions(self, service, api):
        result = apply(service, service.propose('skill', 'create', {'description': 'd', 'body': 'Do it.'},
                                                entity_name='triage'))
        assert result['status'] == 'applied'
        assert api.writes[0][1]['versions'][0] == {'name': 'base', 'instructions': 'Do it.'}


class TestSkill:

    def test_read_flattens_version_content(self, service):
        state = service.read('skill', 4)['state']
        assert state['instructions'] == 'Step 1.'
        assert state['name'] == 'incident-triage'
        assert state['version_name'] == 'base'

    def test_instruction_patch_goes_under_version(self, service, api):
        apply(service, service.propose('skill', 'update', {'instructions': {'append': 'Step 2.'}}, entity_id=4))
        (op, args), = api.writes
        assert op == 'update_skill'
        assert args['version'] == {'id': 8, 'instructions': 'Step 1.\nStep 2.'}
        assert 'version_id' not in args

    def test_description_change_is_a_skill_level_update(self, service, api):
        apply(service, service.propose('skill', 'update', {'description': 'Better'}, entity_id=4))
        (op, args), = api.writes
        assert args == {'skill_id': 4, 'description': 'Better'}

    def test_rename_renames_the_skill_not_the_version(self, service, api):
        apply(service, service.propose('skill', 'update', {'name': 'triage-v2'}, entity_id=4))
        assert api.writes == [('update_skill', {'skill_id': 4, 'name': 'triage-v2'})]


class TestAgentRename:

    def test_rename_updates_the_application(self, service, api):
        result = apply(service, service.propose('agent', 'update', {'name': 'Weekly Summary'}, entity_id=1))
        assert api.writes == [('update_application', {'application_id': 1, 'name': 'Weekly Summary'})]
        assert 'Weekly Summary' in result['summary']

    def test_rename_with_settings_reports_the_agent_name_not_the_version(self, service, api):
        result = apply(service, service.propose('agent', 'update', {'name': 'Weekly Summary',
                                                                    'conversation_starters': ['Hi']}, entity_id=1))
        assert result['summary'].startswith('**Updated agent "Weekly Summary"**')
        assert result['chip']['name'] == 'Weekly Summary'

    def test_settings_update_reports_the_agent_name(self, service, api):
        result = apply(service, service.propose('agent', 'update', {'conversation_starters': ['Hi']}, entity_id=1))
        assert '"base"' not in result['summary']

    def test_starters_accept_an_append(self, service, api):
        apply(service, service.propose('agent', 'update', {'conversation_starters': {'append': 'Hi'}}, entity_id=1))
        assert api.writes[-1][1]['conversation_starters'][-1] == 'Hi'

    def test_create_resolves_text_patches_and_takes_the_default_model(self, service, api):
        apply(service, service.propose('agent', 'create', {'instructions': {'append': 'Greet users.'}},
                                       entity_name='Greeter'))
        version = api.writes[0][1]['versions'][0]
        assert version['instructions'] == 'Greet users.' and version['llm_settings'] == {}

    def test_state_separates_agent_and_version_names(self, service):
        state = service.read('agent', 1)['state']
        assert state['name'] == 'Daily Summary' and state['version_name'] == 'base'

    def test_version_name_is_not_writable(self, service):
        proposal = service.propose('agent', 'update', {'version_name': 'v2'}, entity_id=1)
        assert proposal['status'] == 'invalid'


class TestPartialFailure:

    def test_settings_are_written_before_instructions(self, service, api):
        apply(service, service.propose('agent', 'update', {
            'instructions': {'append': 'x'}, 'llm_settings': {'temperature': 0}}, entity_id=1))
        assert [op for op, _ in api.writes] == ['update_version', 'patch_instructions']

    def test_failed_settings_write_leaves_instructions_untouched(self, service, api):
        api.fail_update_version = True
        result = apply(service, service.propose('agent', 'update', {
            'instructions': {'append': 'x'}, 'llm_settings': {'temperature': 0}}, entity_id=1))
        assert result['status'] == 'error'
        assert [op for op, _ in api.writes] == ['update_version']
