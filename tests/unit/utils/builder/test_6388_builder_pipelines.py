"""#6388: the Elitea Builder serves the SDK pipeline knowledge pack and refuses a pipeline that would not run."""
import pathlib

import pytest
import yaml

PID = 12
PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[4]

GOOD = yaml.safe_dump({'entry_point': 'a', 'nodes': [
    {'id': 'a', 'type': 'llm', 'input_mapping': {'task': {'type': 'fixed', 'value': 'Say hi'}}, 'transition': 'END'}]})
# Tool node with only `name` (the runtime reads `tool`) and a transition to a node that does not exist
BROKEN = 'entry_point: a\nnodes:\n  - {id: a, type: toolkit, name: Jira, transition: ghost}\n'


# ---- reference: pipeline:<topics> serves the knowledge pack, one entry per page -------------------------------

def test_overview(builder):
    found = builder.pipelines.reference_pages('pipeline:overview')
    assert list(found['pages']) == ['overview']
    assert found['pages']['overview'].startswith('# Pipeline YAML: the shape')


def test_several_pages_in_one_call(builder):
    found = builder.pipelines.reference_pages('pipeline:overview,node:toolkit,pattern:loop')
    assert list(found['pages']) == ['overview', 'node:toolkit', 'pattern:loop']
    assert '**Required:** `tool`' in found['pages']['node:toolkit']
    assert found['unknown'] == [] and found['not_loaded'] == []


def test_repeated_prefix_and_duplicates_are_accepted(builder):
    found = builder.pipelines.reference_pages('pipeline:node:llm,pipeline:node:router,node:llm')
    assert list(found['pages']) == ['node:llm', 'node:router']


def test_unknown_page_is_reported(builder):
    found = builder.pipelines.reference_pages('pipeline:node:printr,node:llm')
    assert found['unknown'] == ['node:printr'] and list(found['pages']) == ['node:llm']


def test_bare_prefix_returns_the_overview(builder):
    assert list(builder.pipelines.reference_pages('pipeline:')['pages']) == ['overview']


def test_pages_over_the_limit_are_named_not_loaded(builder):
    limit = builder.pipelines.MAX_TOPICS_PER_CALL
    keys = ['overview', 'design', 'constraints', 'troubleshooting', 'node:llm', 'node:code', 'node:router']
    found = builder.pipelines.reference_pages('pipeline:' + ','.join(keys))
    assert len(found['pages']) == limit and found['not_loaded'] == keys[limit:]


def test_topics_carry_the_prefix(builder):
    topics = builder.pipelines.topics()
    assert 'pipeline:overview' in topics and 'pipeline:node:llm' in topics


def test_runtime_no_longer_reads_the_service_prompt():
    source = (PLUGIN_ROOT / 'utils' / 'builder' / 'runtime.py').read_text()
    assert 'pipeline_authoring_reference' not in source and 'get_service_prompt' not in source


def test_playbook_asks_for_node_pages(builder):
    text = builder.prompts.playbook('pipeline')
    assert 'node:llm' in text and 'propose refuses' in text


# ---- propose: a pipeline that would not run is refused before the Changes card ----------------------------------

class Calls:
    """The endpoints propose reads, for one draft pipeline version."""

    def __init__(self, instructions):
        self.writes = []
        self.version = {'id': 10, 'name': 'base', 'status': 'draft', 'instructions': instructions,
                        'agent_type': 'pipeline', 'llm_settings': {}, 'instructions_sha256': 'a' * 64}

    def __call__(self, op, args):
        if op == 'get_application':
            return {'id': 1, 'name': 'Flow', 'versions': [{'id': 10, 'name': 'base', 'status': 'draft'}]}
        if op == 'get_version':
            return dict(self.version)
        if op == 'list_applications':
            return {'rows': [{'id': 1, 'name': 'Flow', 'versions': [{'id': 10, 'name': 'base', 'status': 'draft'}]}]}
        self.writes.append((op, args))
        return {'ok': True}


def _service(builder, calls):
    return builder.service.BuilderService(project_id=PID, user_id=5, call=calls,
                                          store=builder.service.MemoryStore(),
                                          catalog=builder.catalog.Catalog([]), can_write=True)


def _replace(text):
    return {'instructions': {'replace': text}}


def test_create_with_a_broken_pipeline_is_refused(builder):
    result = _service(builder, Calls(GOOD)).propose('pipeline', 'create', {'name': 'Flow', 'instructions': BROKEN})
    assert result['status'] == 'invalid'
    rules = {e['message'].split(']')[0].strip('[') for e in result['errors']}
    assert {'R24', 'R2'} <= rules
    assert any('reads `tool`, not `name`' in e['suggestion'] for e in result['errors'])
    assert result['errors'][0]['field'].startswith('instructions')


def test_create_with_a_working_pipeline_is_proposed(builder):
    result = _service(builder, Calls(GOOD)).propose('pipeline', 'create', {'name': 'Flow', 'instructions': GOOD})
    assert result['status'] != 'invalid', result


def test_update_that_breaks_the_pipeline_is_refused(builder):
    service = _service(builder, Calls(GOOD))
    result = service.propose('pipeline', 'update', _replace(BROKEN), entity_id=1, version_id=10)
    assert result['status'] == 'invalid'


def test_update_of_a_pipeline_that_already_had_errors_is_allowed(builder):
    old = BROKEN.replace('transition: ghost', 'transition: END')  # only the old `name`-only node error
    service = _service(builder, Calls(old))
    edited = old.replace('name: Jira', 'name: Jira Cloud')
    result = service.propose('pipeline', 'update', _replace(edited), entity_id=1, version_id=10)
    assert result['status'] != 'invalid', result


def test_update_without_instructions_is_not_validated(builder):
    service = _service(builder, Calls(BROKEN))
    result = service.propose('pipeline', 'update', {'description': 'New text'}, entity_id=1, version_id=10)
    assert result['status'] != 'invalid', result


def test_agents_are_not_validated_as_pipelines(builder):
    result = _service(builder, Calls(GOOD)).propose('agent', 'create', {'name': 'Bot', 'instructions': BROKEN})
    assert result['status'] != 'invalid', result


@pytest.mark.parametrize('before', [None, '', 'nodes: ['])
def test_text_that_is_still_not_a_pipeline_is_refused(builder, before):
    assert builder.pipelines.new_errors(before, 'entry_point: [')


# ---- the check knows the pipeline's trigger and its attached toolkits' parameters --------------------------------

JIRA_NODE = ('entry_point: t\nnodes:\n  - {id: t, type: toolkit, toolkit_name: My Jira, tool: get_issue, '
             'transition: END, input_mapping: {%s: {type: fixed, value: PROJ-1}}}\n')
JIRA_SCHEMAS = {'jira': {'get_issue': {'properties': {'issue_key': {'type': 'string'}}, 'required': ['issue_key']}}}


def _attached_calls(instructions, trigger=None):
    calls = Calls(instructions)
    calls.version['tools'] = [{'name': 'My Jira', 'type': 'jira', 'settings': {'selected_tools': ['get_issue']}}]
    if trigger:
        calls.version['pipeline_settings'] = {'trigger': {'type': trigger}}
    return calls


def _with_schemas(builder, calls, schemas=JIRA_SCHEMAS):
    asked = []

    def lookup(types_):
        asked.append(types_)
        return schemas
    service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls,
                                             store=builder.service.MemoryStore(),
                                             catalog=builder.catalog.Catalog([]), can_write=True, tool_schemas=lookup)
    return service, asked


def test_a_misspelled_required_parameter_is_refused(builder):
    service, asked = _with_schemas(builder, _attached_calls(JIRA_NODE % 'issue_key'))
    result = service.propose('pipeline', 'update', _replace(JIRA_NODE % 'issuekey'), entity_id=1, version_id=10)
    assert result['status'] == 'invalid' and asked == [['jira']]
    assert any(e['message'].startswith('[R36]') and 'issue_key' in e['message'] for e in result['errors'])


def test_the_right_parameter_passes(builder):
    service, _ = _with_schemas(builder, _attached_calls(GOOD))
    result = service.propose('pipeline', 'update', _replace(JIRA_NODE % 'issue_key'), entity_id=1, version_id=10)
    assert result['status'] != 'invalid', result


def test_a_schema_lookup_failure_never_blocks_a_change(builder):
    calls = _attached_calls(GOOD)

    def broken(types_):
        raise RuntimeError('provider hub is down')
    service = builder.service.BuilderService(project_id=PID, user_id=5, call=calls,
                                             store=builder.service.MemoryStore(),
                                             catalog=builder.catalog.Catalog([]), can_write=True, tool_schemas=broken)
    result = service.propose('pipeline', 'update', _replace(JIRA_NODE % 'issuekey'), entity_id=1, version_id=10)
    assert result['status'] != 'invalid', result


HITL_FLOW = yaml.safe_dump({'entry_point': 'a', 'nodes': [
    {'id': 'a', 'type': 'llm', 'input_mapping': {'task': {'type': 'fixed', 'value': 'x'}}, 'transition': 'h'},
    {'id': 'h', 'type': 'hitl', 'user_message': {'type': 'fixed', 'value': 'ok?'}, 'routes': {'approve': 'END'}}]})


def test_an_approval_step_in_a_scheduled_pipeline_is_refused(builder):
    service = _service(builder, _attached_calls(GOOD, trigger='schedule'))
    result = service.propose('pipeline', 'update', _replace(HITL_FLOW), entity_id=1, version_id=10)
    assert result['status'] == 'invalid'
    assert any(e['message'].startswith('[R31]') for e in result['errors'])


def test_an_approval_step_in_a_chat_pipeline_is_fine(builder):
    service = _service(builder, _attached_calls(GOOD))
    result = service.propose('pipeline', 'update', _replace(HITL_FLOW), entity_id=1, version_id=10)
    assert result['status'] != 'invalid', result


# ---- read shows what is already wrong ----------------------------------------------------------------------------

def test_read_reports_existing_errors(builder):
    result = _service(builder, Calls(BROKEN)).read('pipeline', 1, 10)
    check = result['pipeline_check']
    assert result['status'] == 'ok' and check['errors']
    assert any('reads `tool`, not `name`' in e['suggestion'] for e in check['errors'])


def test_read_of_a_clean_pipeline_reports_nothing(builder):
    check = _service(builder, Calls(GOOD)).read('pipeline', 1, 10)['pipeline_check']
    assert check == {'errors': [], 'warnings': []}


def test_read_of_an_agent_has_no_pipeline_check(builder):
    calls = Calls('Be brief.')
    calls.version['agent_type'] = 'openai'
    assert 'pipeline_check' not in _service(builder, calls).read('agent', 1, 10)
