"""#7383 S2: the Builder service — find/read/propose/apply over typed entities and the catalog, with an in-memory
call() standing in for the existing endpoints (dispatched in-process through the MCP executor in production)."""
import pytest

PID = 12


class Calls:
    """Fake endpoint layer keyed by op_id; records writes."""

    def __init__(self):
        self.writes = []
        self.versions = {
            (1, 10): {'id': 10, 'name': 'base', 'status': 'draft', 'instructions': 'Be brief.\nUse a table.',
                      'llm_settings': {'model_name': 'gpt-5-mini', 'temperature': 0.2},
                      'conversation_starters': [], 'agent_type': 'openai', 'instructions_sha256': 'a' * 64},
            (2, 20): {'id': 20, 'name': 'base', 'status': 'published', 'instructions': 'x',
                      'agent_type': 'openai', 'instructions_sha256': 'b' * 64},
        }
        self.apps = [
            {'id': 1, 'name': 'Daily Summary', 'versions': [{'id': 10, 'name': 'base', 'status': 'draft'}]},
            {'id': 2, 'name': 'Published Bot', 'versions': [{'id': 20, 'name': 'base', 'status': 'published'}]},
            {'id': 3, 'name': 'Twin', 'versions': [{'id': 30, 'name': 'base', 'status': 'draft'}]},
            {'id': 4, 'name': 'Twin', 'versions': [{'id': 40, 'name': 'base', 'status': 'draft'}]},
        ]
        self.project_context = {'content': 'Old facts.', 'enabled': True, 'activation_description': 'When asked.'}

    def __call__(self, op, args):
        if op == 'list_applications':
            q = (args.get('query') or '').lower()
            return {'rows': [a for a in self.apps if q in a['name'].lower()]}
        if op == 'get_application':
            return next(dict(a) for a in self.apps if a['id'] == args['application_id'])
        if op == 'get_version':
            return dict(self.versions[(args['application_id'], args['version_id'])])
        if op == 'get_project_context':
            return dict(self.project_context)
        if op == 'list_toolkits':
            self.toolkit_args = args
            rows = [{'id': 5, 'name': 'Github', 'mcp': False}, {'id': 6, 'name': 'Github MCP', 'mcp': True}]
            mcp = args.get('mcp') == 'true'
            return {'rows': [r for r in rows if r['mcp'] is mcp]}
        if op == 'list_models':
            self.model_args = args
            if args.get('section') == 'embedding':
                return {'total': 1, 'items': [{'name': 'text-embedding-3-large'}]}
            return {'total': 1, 'items': [{'name': 'gpt-5-mini', 'display_name': 'GPT-5 mini'}]}
        self.writes.append((op, args))
        if op == 'patch_instructions':
            return {'mcp_backup_version': {'id': 99, 'name': 'mcp-backup-10-x'}, 'instructions_sha256': 'c' * 64}
        if op == 'create_application':
            return {'id': 7, 'name': args['name'], 'versions': [{'id': 70, 'name': 'base'}]}
        return {'ok': True}


@pytest.fixture
def calls():
    return Calls()


@pytest.fixture
def service(builder, calls):
    store = builder.service.MemoryStore()
    return builder.service.BuilderService(project_id=PID, user_id=5, call=calls, store=store,
                                          catalog=builder.catalog.Catalog([]), can_write=True)


class TestFind:

    def test_finds_models_from_an_items_answer(self, service):
        found = service.find('model', 'embedding')
        assert found['status'] == 'ok'
        assert [c['name'] for c in found['candidates']] == ['text-embedding-3-large']

    def test_lists_shared_llm_models_by_display_name(self, service, calls):
        found = service.find('model', 'gpt-5 mini')
        assert [c['name'] for c in found['candidates']] == ['gpt-5-mini']
        assert calls.model_args == {'section': 'llm', 'include_shared': 'true'}

    def test_finds_agents_by_name(self, service):
        result = service.find('agent', 'daily')
        assert result['candidates'] == [{'id': 1, 'name': 'Daily Summary', 'version_id': 10, 'version': 'base',
                                         'status': 'draft'}]
        assert result['ambiguous'] is False

    def test_exact_duplicate_name_is_ambiguous(self, service):
        assert service.find('agent', 'Twin')['ambiguous'] is True

    def test_unknown_type(self, service):
        assert service.find('folder', 'x')['status'] == 'invalid'

    def test_finds_mcps_with_the_mcp_filter(self, service, calls):
        found = service.find('mcp', 'github')
        assert found['candidates'] == [{'id': 6, 'name': 'Github MCP'}]
        assert calls.toolkit_args == {'query': 'github', 'mcp': 'true'}

    def test_toolkit_search_keeps_mcps_out(self, service, calls):
        found = service.find('toolkit', 'github')
        assert found['candidates'] == [{'id': 5, 'name': 'Github'}]
        assert 'mcp' not in calls.toolkit_args

    def test_mcp_is_a_lookup_type_only(self, service):
        assert service.read('mcp', 6)['status'] == 'invalid'
        assert service.propose('mcp', 'update', {}, entity_id=6)['status'] == 'invalid'


class TestRead:

    def test_returns_state_and_revision(self, service):
        result = service.read('agent', 1, version_id=10)
        assert result['state']['instructions'] == 'Be brief.\nUse a table.'
        assert len(result['revision']) == 64

    def test_a_read_carries_its_type_playbook(self, service):
        assert 'entity="agent"' in service.read('agent', 1, version_id=10)['playbook']


class TestProposeApplyAgent:

    def test_instruction_append_goes_through_the_safe_patch(self, service, calls):
        rev = service.read('agent', 1, version_id=10)['revision']
        proposal = service.propose('agent', 'update', {'instructions': {'append': 'Answer in French.'}},
                                   entity_id=1, version_id=10, base_revision=rev)
        assert proposal['status'] == 'ready'
        assert proposal['diff'] == [{'field': 'instructions', 'added_lines': ['Answer in French.'],
                                     'removed_lines': []}]
        assert calls.writes == []
        result = service.apply(proposal['changeset_id'])
        assert result['status'] == 'applied'
        (op, args), = calls.writes
        assert op == 'patch_instructions'
        assert args['expected_instructions_sha256'] == 'a' * 64
        assert args['old_text'] == 'Be brief.\nUse a table.'
        assert args['replacement'] == 'Be brief.\nUse a table.\nAnswer in French.'
        assert result['backup'] == 'mcp-backup-10-x'
        assert 'Updated agent "Daily Summary"' in result['summary']

    def test_non_instruction_fields_send_only_what_changes(self, service, calls):
        proposal = service.propose('agent', 'update', {'llm_settings': {'temperature': 0.0}}, entity_id=1,
                                   version_id=10)
        service.apply(proposal['changeset_id'])
        (op, args), = calls.writes
        assert op == 'update_version'
        assert args == {'application_id': 1, 'version_id': 10,
                        'llm_settings': {'model_name': 'gpt-5-mini', 'temperature': 0.0}}

    def test_published_version_offers_a_draft(self, service, calls):
        proposal = service.propose('agent', 'update', {'name': 'x'}, entity_id=2, version_id=20)
        assert proposal['status'] == 'invalid'
        assert 'published' in proposal['errors'][0]['message']
        assert proposal['errors'][0]['suggestion'] == 'create a new draft version with this change'
        assert calls.writes == []

    def test_stale_revision_is_a_conflict(self, service):
        proposal = service.propose('agent', 'update', {'name': 'x'}, entity_id=1, version_id=10,
                                   base_revision='0' * 64)
        assert proposal['status'] == 'conflict'

    def test_limit_errors_block_the_proposal(self, service):
        proposal = service.propose('agent', 'update', {'conversation_starters': list('abcde')}, entity_id=1,
                                   version_id=10)
        assert proposal['status'] == 'invalid'
        assert proposal['errors'][0]['limit'] == 4

    def test_create_needs_a_name(self, service):
        proposal = service.propose('agent', 'create', {'instructions': 'x'})
        assert proposal['status'] == 'needs_input'
        assert proposal['questions'][0]['header'] == 'Name'

    def test_create_takes_the_top_level_name(self, service):
        proposal = service.propose('agent', 'create', {'instructions': 'x'}, entity_name='GitHub PR Reviewer')
        assert proposal['status'] == 'ready'
        assert 'GitHub PR Reviewer' in proposal['summary']

    def test_create_applies_without_confirmation(self, service, calls):
        proposal = service.propose('agent', 'create', {'name': 'Helper', 'instructions': 'Help.'})
        assert proposal['status'] == 'ready'
        result = service.apply(proposal['changeset_id'])
        assert result['status'] == 'applied'
        assert result['chip'] == {'type': 'agent', 'id': 7, 'version_id': 70, 'name': 'Helper'}

    def test_ambiguous_target_asks_which_one(self, service):
        proposal = service.propose('agent', 'update', {'name': 'x'}, entity_name='Twin')
        assert proposal['status'] == 'needs_input'
        options = proposal['questions'][0]['options']
        assert [o['label'] for o in options] == ['Twin (id 3)', 'Twin (id 4)']


class TestDestructive:

    def test_delete_needs_confirmation_then_applies(self, service, calls):
        proposal = service.propose('agent', 'delete', {}, entity_id=1)
        assert proposal['status'] == 'needs_confirmation'
        refused = service.apply(proposal['changeset_id'])
        assert refused['status'] == 'forbidden'
        assert calls.writes == []
        service.confirm(proposal['changeset_id'], user_id=5)
        assert service.apply(proposal['changeset_id'])['status'] == 'applied'
        assert calls.writes[0][0] == 'delete_application'

    def test_confirmation_by_another_user_is_refused(self, service):
        proposal = service.propose('agent', 'delete', {}, entity_id=1)
        assert service.confirm(proposal['changeset_id'], user_id=6) is False

    def test_a_changeset_applies_once(self, service, calls):
        proposal = service.propose('agent', 'create', {'name': 'Once', 'instructions': 'x'})
        service.apply(proposal['changeset_id'])
        assert service.apply(proposal['changeset_id'])['status'] == 'invalid'
        assert len(calls.writes) == 1


class TestProjectContext:

    def test_update_keeps_untouched_fields(self, service, calls):
        proposal = service.propose('project_context', 'update', {'content': {'append': 'New fact.'}})
        service.apply(proposal['changeset_id'])
        (op, args), = calls.writes
        assert op == 'put_project_context'
        assert args == {'content': 'Old facts.\nNew fact.', 'enabled': True, 'activation_description': 'When asked.'}

    def test_over_limit(self, service):
        proposal = service.propose('project_context', 'update', {'content': {'replace': 'x' * 2600}})
        assert proposal['status'] == 'invalid'
        assert '100 characters over' in proposal['errors'][0]['message']


class TestPermissions:

    def test_viewer_cannot_propose_writes(self, builder, calls):
        viewer = builder.service.BuilderService(project_id=PID, user_id=5, call=calls,
                                                store=builder.service.MemoryStore(),
                                                catalog=builder.catalog.Catalog([]), can_write=False)
        assert viewer.find('agent', 'daily')['candidates']
        proposal = viewer.propose('agent', 'update', {'name': 'x'}, entity_id=1, version_id=10)
        assert proposal['status'] == 'forbidden'
        assert 'permission' in proposal['message']


class TestCatalogOperation:

    TOOLS = [{'value': 'delete_artifacts_mode_project_id_bucket', 'method': 'DELETE',
              'path': '/api/v2/artifacts/artifacts/{mode}/{project_id}/{bucket}', 'tags': ['artifacts'],
              'description': 'Delete Artifacts.',
              'args_schema': {'type': 'object', 'properties': {'project_id': {'type': 'integer'},
                                                               'bucket': {'type': 'string'}},
                              'required': ['project_id', 'bucket']},
              'parameters': []}]

    def test_destructive_operation_needs_confirmation_and_gets_the_session_project(self, builder, calls):
        service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls,
                                                 store=builder.service.MemoryStore(),
                                                 catalog=builder.catalog.Catalog(self.TOOLS), can_write=True)
        proposal = service.propose('operation', 'call', {'bucket': 'tmp'},
                                   entity_id='delete_artifacts_mode_project_id_bucket')
        assert proposal['status'] == 'needs_confirmation'
        service.confirm(proposal['changeset_id'], user_id=5)
        service.apply(proposal['changeset_id'])
        assert calls.writes == [('op:delete_artifacts_mode_project_id_bucket', {'bucket': 'tmp', 'project_id': PID})]

    def test_invalid_args_call_nothing(self, builder, calls):
        service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls,
                                                 store=builder.service.MemoryStore(),
                                                 catalog=builder.catalog.Catalog(self.TOOLS), can_write=True)
        proposal = service.propose('operation', 'call', {}, entity_id='delete_artifacts_mode_project_id_bucket')
        assert proposal['status'] == 'invalid'
        assert proposal['errors'] == [{'field': 'bucket', 'message': 'required'}]


class TestResumeReproposes:
    """On a HITL resume the SDK re-issues the same propose call; it must get the same changeset back (review fix)."""

    def test_identical_pending_proposal_returns_the_same_changeset(self, service):
        first = service.propose('agent', 'delete', {}, entity_id=1)
        second = service.propose('agent', 'delete', {}, entity_id=1)
        assert second['changeset_id'] == first['changeset_id']

    def test_confirm_then_repropose_then_apply_works(self, service, calls):
        first = service.propose('agent', 'delete', {}, entity_id=1)
        service.confirm(first['changeset_id'], user_id=5)
        again = service.propose('agent', 'delete', {}, entity_id=1)
        assert again['changeset_id'] == first['changeset_id']
        assert service.apply(again['changeset_id'])['status'] == 'applied'
        assert [c[0] for c in calls.writes] == ['delete_application']

    def test_a_different_change_gets_a_new_changeset(self, service):
        first = service.propose('agent', 'update', {'name': 'A'}, entity_id=1, version_id=10)
        second = service.propose('agent', 'update', {'name': 'B'}, entity_id=1, version_id=10)
        assert first['changeset_id'] != second['changeset_id']

    def test_an_applied_changeset_is_not_reused(self, service):
        first = service.propose('agent', 'create', {'name': 'Once', 'instructions': 'x'})
        service.apply(first['changeset_id'])
        second = service.propose('agent', 'create', {'name': 'Once', 'instructions': 'x'})
        assert second['changeset_id'] != first['changeset_id']


def test_operation_arguments_shown_to_the_chat_mask_secret_values(builder, calls):
    tools = [{'value': 'put_secrets_secret', 'method': 'PUT', 'path': '/api/v2/secrets/secret/{mode}/{project_id}/{secret}',
              'description': 'Update a secret.',
              'args_schema': {'type': 'object', 'properties': {'project_id': {'type': 'integer'},
                                                               'secret': {'type': 'string'},
                                                               'value': {'type': 'string'}},
                              'required': ['project_id', 'secret', 'value']}}]
    service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls, store=builder.service.MemoryStore(),
                                             catalog=builder.catalog.Catalog(tools), can_write=True)
    proposal = service.propose('operation', 'call', {'secret': 's', 'value': 'v-123'}, entity_id='put_secrets_secret')
    assert proposal['status'] == 'needs_confirmation'
    assert proposal['arguments'] == {'secret': 's', 'value': '****', 'project_id': PID}
    service.confirm(proposal['changeset_id'], user_id=5)
    service.apply(proposal['changeset_id'])
    assert calls.writes[-1] == ('op:put_secrets_secret', {'secret': 's', 'value': 'v-123', 'project_id': PID})


def test_a_secrets_operation_shows_only_the_secret_name(builder, calls):
    """E2E: the model put the new value into the `secret` path argument; it is masked all the same."""
    tools = [{'value': 'put_secrets_secret', 'method': 'PUT', 'path': '/api/v2/secrets/secret/{mode}/{project_id}/{secret}',
              'args_schema': {'type': 'object', 'properties': {'project_id': {'type': 'integer'},
                                                               'secret': {'type': 'string'},
                                                               'name': {'type': 'string'}}}}]
    service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls, store=builder.service.MemoryStore(),
                                             catalog=builder.catalog.Catalog(tools), can_write=True)
    proposal = service.propose('operation', 'call', {'name': 'tmp', 'secret': 'e2e-value-789'},
                               entity_id='put_secrets_secret')
    assert proposal['arguments'] == {'name': 'tmp', 'secret': '****', 'project_id': PID}


def test_toolkits_are_not_an_agent_field(service):
    proposal = service.propose('agent', 'update', {'tools': [{'id': 3}]}, entity_id=1)
    assert proposal['status'] == 'invalid' and 'tools' in proposal['message']


def test_linking_a_toolkit_returns_the_agent_chip(builder, calls):
    tools = [{'value': 'patch_elitea_core_tool', 'method': 'PATCH', 'path': '/api/v2/elitea_core/tool/{mode}/{project_id}/{tool_id}',
              'args_schema': {'type': 'object', 'properties': {
                  'project_id': {'type': 'integer'}, 'tool_id': {'type': 'integer'}, 'entity_id': {'type': 'integer'},
                  'entity_version_id': {'type': 'integer'}, 'entity_type': {'type': 'string'},
                  'has_relation': {'type': 'boolean'}}}}]
    service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls, store=builder.service.MemoryStore(),
                                             catalog=builder.catalog.Catalog(tools), can_write=True)
    proposal = service.propose('operation', 'call', {'tool_id': 9, 'entity_id': 1, 'entity_version_id': 10,
                                                     'entity_type': 'agent', 'has_relation': True},
                               entity_id='patch_elitea_core_tool')
    result = service.apply(proposal['changeset_id'])
    assert result['chip'] == {'type': 'agent', 'id': 1, 'version_id': 10, 'name': 'Daily Summary'}


def test_linking_with_a_version_id_as_the_agent_id_is_refused(builder, calls):
    """E2E: the model sent the version id (819) as entity_id; the link would have gone to no agent."""
    tools = [{'value': 'patch_elitea_core_tool', 'method': 'PATCH', 'path': '/api/v2/elitea_core/tool/{mode}/{project_id}/{tool_id}',
              'args_schema': {'type': 'object', 'properties': {
                  'project_id': {'type': 'integer'}, 'tool_id': {'type': 'integer'}, 'entity_id': {'type': 'integer'},
                  'entity_version_id': {'type': 'integer'}, 'entity_type': {'type': 'string'}}}}]
    service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls, store=builder.service.MemoryStore(),
                                             catalog=builder.catalog.Catalog(tools), can_write=True)
    proposal = service.propose('operation', 'call', {'tool_id': 9, 'entity_id': 1, 'entity_version_id': 20,
                                                     'entity_type': 'agent'}, entity_id='patch_elitea_core_tool')
    assert proposal['status'] == 'invalid' and 'does not belong' in proposal['message']


def test_a_secret_nested_in_an_operation_argument_is_masked(builder, calls):
    tools = [{'value': 'post_configurations', 'method': 'POST', 'path': '/api/v2/configurations/configurations/{project_id}',
              'args_schema': {'type': 'object', 'properties': {'project_id': {'type': 'integer'},
                                                               'type': {'type': 'string'},
                                                               'data': {'type': 'object'}}}}]
    service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls, store=builder.service.MemoryStore(),
                                             catalog=builder.catalog.Catalog(tools), can_write=True)
    proposal = service.propose('operation', 'call', {'type': 'openai', 'data': {
        'api_key': 'made-up-value', 'headers': [{'auth_token': 'made-up-2'}], 'ref_key': '{{secret.openai}}',
        'url': 'https://x'}}, entity_id='post_configurations')
    assert proposal['arguments']['data'] == {'api_key': '****', 'headers': [{'auth_token': '****'}],
                                             'ref_key': '{{secret.openai}}', 'url': 'https://x'}
    assert 'made-up' not in str(proposal)


def test_a_create_appends_to_list_fields_as_lists(service, calls):
    proposal = service.propose('agent', 'create', {'name': 'QA', 'tags': {'append': 'qa'},
                                                   'conversation_starters': {'append': 'Hi'}})
    if proposal['status'] == 'needs_confirmation':
        service.confirm(proposal['changeset_id'], user_id=5)
    service.apply(proposal['changeset_id'])
    version = calls.writes[0][1]['versions'][0]
    assert version['tags'] == ['qa'] and version['conversation_starters'] == ['Hi']
