"""#7383 x #7228: a credential value never goes through the Builder — material routes are not offered to a run, a
material proposal is refused before anything is stored, and an apply keeps what the run's write filed."""
PID = 12


class Calls:
    """Fake endpoint layer; records writes."""

    def __init__(self):
        self.writes = []

    def __call__(self, op, args):
        self.writes.append((op, args))
        return {'ok': True}

SECRET_TOOL = {'value': 'post_secrets_secret', 'method': 'POST', 'path': '/api/v2/secrets/secret/{mode}/{project_id}',
               'tags': ['secrets'], 'description': 'Create Secret.',
               'args_schema': {'type': 'object', 'properties': {'project_id': {'type': 'integer'},
                                                                'name': {'type': 'string'},
                                                                'value': {'type': 'string'}}}}
TOOLKIT_TOOL = {'value': 'put_elitea_core_tool', 'method': 'PUT', 'path': '/api/v2/elitea_core/tool/{mode}/{project_id}/{tool_id}',
                'tags': ['elitea_core/toolkits'], 'description': 'Update Toolkit.',
                'args_schema': {'type': 'object', 'properties': {'project_id': {'type': 'integer'},
                                                                 'tool_id': {'type': 'integer'},
                                                                 'settings': {'type': 'object'}}}}


def _service(builder, calls, material=None, listed=None):
    catalog = builder.catalog.Catalog([SECRET_TOOL, TOOLKIT_TOOL], listed=listed)
    store = builder.service.MemoryStore()
    service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls, store=store, catalog=catalog,
                                             can_write=True, material=material)
    return service, store


def test_a_route_the_run_may_not_list_is_not_in_the_catalog(builder):
    catalog = builder.catalog.Catalog([SECRET_TOOL, TOOLKIT_TOOL], listed=lambda tool: '/secrets/' not in tool['path'])
    assert [c['op_id'] for c in catalog.find('secret toolkit')] == ['put_elitea_core_tool']


def test_without_a_filter_every_tagged_route_stays(builder):
    catalog = builder.catalog.Catalog([SECRET_TOOL, TOOLKIT_TOOL])
    assert {c['op_id'] for c in catalog.find('secret toolkit')} == {'post_secrets_secret', 'put_elitea_core_tool'}


def test_a_material_proposal_is_refused_and_nothing_is_stored(builder):
    calls, seen = Calls(), []

    def material(tool, args):
        seen.append((tool['value'], args))
        return True

    service, store = _service(builder, calls, material=material)
    proposal = service.propose('operation', 'call', {'tool_id': 3, 'settings': {'api_key': 'made-up-value'}},
                               entity_id='put_elitea_core_tool')
    assert proposal['status'] == 'refused' and proposal['code'] == 'CREDENTIALS_VIA_FORM'
    assert 'made-up-value' not in str(proposal)
    assert store._rows == {} and calls.writes == []
    assert seen == [('put_elitea_core_tool', {'tool_id': 3, 'settings': {'api_key': 'made-up-value'},
                                               'project_id': PID})]


def test_a_proposal_without_material_is_stored_as_before(builder):
    service, store = _service(builder, Calls(), material=lambda tool, args: False)
    proposal = service.propose('operation', 'call', {'tool_id': 3, 'settings': {'api_key': '{{secret.jira}}'}},
                               entity_id='put_elitea_core_tool')
    assert proposal['status'] in ('ready', 'needs_confirmation') and len(store._rows) == 1


def test_invalid_arguments_are_reported_before_the_material_check(builder):
    service, _ = _service(builder, Calls(), material=lambda tool, args: (_ for _ in ()).throw(AssertionError))
    proposal = service.propose('operation', 'call', {'tool_id': 'x'}, entity_id='put_elitea_core_tool')
    assert proposal['status'] == 'invalid'


def test_apply_passes_on_what_the_write_filed(builder):
    filed = {'requests': [{'kind': 'toolkit', 'title': 'Jira', 'state': 'pending'}], 'next': 'An admin approves it.'}

    class Filing(Calls):
        def __call__(self, op, args):
            super().__call__(op, args)
            return {'ok': True, 'governance_requests': filed}

    service, _ = _service(builder, Filing())
    proposal = service.propose('operation', 'call', {'tool_id': 3, 'settings': {}}, entity_id='put_elitea_core_tool')
    if proposal['status'] == 'needs_confirmation':
        service.confirm(proposal['changeset_id'], user_id=5)
    result = service.apply(proposal['changeset_id'])
    assert result['status'] == 'applied' and result['governance_requests'] == filed
