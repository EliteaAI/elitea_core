"""#6388, review of elitea_core#774: three ways a pipeline that never finishes or fails could still be proposed."""
import yaml

PID = 12

HITL_FLOW = yaml.safe_dump({'entry_point': 'a', 'nodes': [
    {'id': 'a', 'type': 'llm', 'input_mapping': {'task': {'type': 'fixed', 'value': 'x'}}, 'transition': 'h'},
    {'id': 'h', 'type': 'hitl', 'user_message': {'type': 'fixed', 'value': 'ok?'}, 'routes': {'approve': 'END'}}]})
HITL_FLOW_EDITED = HITL_FLOW.replace("value: x", "value: a better prompt")


class Calls:
    def __init__(self, instructions, trigger=None, tools=None):
        self.version = {'id': 10, 'name': 'base', 'status': 'draft', 'instructions': instructions,
                        'agent_type': 'pipeline', 'llm_settings': {}, 'instructions_sha256': 'a' * 64,
                        'tools': tools or []}
        if trigger:
            self.version['pipeline_settings'] = {'trigger': {'type': trigger}}

    def __call__(self, op, args):
        if op == 'get_application':
            return {'id': 1, 'name': 'Flow', 'versions': [{'id': 10, 'name': 'base', 'status': 'draft'}]}
        if op == 'get_version':
            return dict(self.version)
        return {'ok': True}


def _service(builder, calls, tool_schemas=None):
    return builder.service.BuilderService(project_id=PID, user_id=5, call=calls, store=builder.service.MemoryStore(),
                                          catalog=builder.catalog.Catalog([]), can_write=True,
                                          tool_schemas=tool_schemas)


def _rules(result):
    return {e['message'].split(']')[0].strip('[') for e in result.get('errors', [])}


# ---- 1. a trigger-only change is checked -------------------------------------------------------------------------

def test_switching_a_hitl_pipeline_to_a_webhook_trigger_is_refused(builder):
    service = _service(builder, Calls(HITL_FLOW))
    result = service.propose('pipeline', 'update', {'pipeline_settings': {'trigger': {'type': 'webhook'}}},
                             entity_id=1, version_id=10)
    assert result['status'] == 'invalid' and 'R31' in _rules(result)


def test_a_trigger_change_on_a_pipeline_without_a_person_step_is_fine(builder):
    plain = HITL_FLOW.replace("transition: h", "transition: END")
    plain = yaml.safe_dump({**yaml.safe_load(plain), 'nodes': yaml.safe_load(plain)['nodes'][:1]})
    result = _service(builder, Calls(plain)).propose(
        'pipeline', 'update', {'pipeline_settings': {'trigger': {'type': 'schedule'}}}, entity_id=1, version_id=10)
    assert result['status'] != 'invalid', result


# ---- 2. the old YAML is checked with the old trigger ---------------------------------------------------------------

def test_moving_to_a_schedule_while_editing_the_yaml_is_refused(builder):
    service = _service(builder, Calls(HITL_FLOW))  # chat trigger: the HITL node is fine today
    changes = {'instructions': {'replace': HITL_FLOW_EDITED}, 'pipeline_settings': {'trigger': {'type': 'schedule'}}}
    result = service.propose('pipeline', 'update', changes, entity_id=1, version_id=10)
    assert result['status'] == 'invalid' and 'R31' in _rules(result)


def test_an_existing_schedule_problem_does_not_block_an_unrelated_edit(builder):
    service = _service(builder, Calls(HITL_FLOW, trigger='schedule'))  # already broken before this change
    result = service.propose('pipeline', 'update', {'instructions': {'replace': HITL_FLOW_EDITED}},
                             entity_id=1, version_id=10)
    assert result['status'] != 'invalid', result


# ---- 3. a new missing parameter is not hidden by an old one on the same node --------------------------------------

SCHEMAS = {'github': {'create_branch': {'properties': {'repo_name': {}, 'branch': {}, 'base': {}},
                                        'required': ['repo_name', 'branch']}}}
TOOLS = [{'name': 'My GitHub', 'type': 'github'}]


def _github(*mapped):
    mapping = {k: {'type': 'fixed', 'value': 'x'} for k in mapped}
    return yaml.safe_dump({'entry_point': 'n1', 'nodes': [
        {'id': 'n1', 'type': 'toolkit', 'toolkit_name': 'My GitHub', 'tool': 'create_branch',
         'input_mapping': mapping, 'transition': 'END'}]})


def test_a_second_missing_required_parameter_is_refused(builder):
    service = _service(builder, Calls(_github('repo_name'), tools=TOOLS), tool_schemas=lambda types_: SCHEMAS)
    result = service.propose('pipeline', 'update', {'instructions': {'replace': _github('base')}},
                             entity_id=1, version_id=10)
    assert result['status'] == 'invalid'
    assert any('repo_name' in e['message'] for e in result['errors'])
    assert not any('`branch`' in e['message'] for e in result['errors'])  # the old one is not held against it


def test_an_old_error_whose_message_changes_still_counts_as_old(builder):
    # R12's message names the output count; adding a third output to a state_modifier that already had two must not
    # look like a new error. Only R36 compares messages.
    def modifier(*outputs):
        return yaml.safe_dump({'entry_point': 's', 'state': {'messages': {'type': 'list'},
                                                            **{o: {'type': 'str'} for o in outputs}},
                               'nodes': [{'id': 's', 'type': 'state_modifier', 'template': 'x',
                                          'output': list(outputs), 'transition': 'END'}]})
    assert builder.pipelines.new_errors(modifier('a', 'b'), modifier('a', 'b', 'c')) == []


def test_the_same_missing_parameter_is_still_not_held_against_an_edit(builder):
    service = _service(builder, Calls(_github('repo_name'), tools=TOOLS), tool_schemas=lambda types_: SCHEMAS)
    result = service.propose('pipeline', 'update', {'instructions': {'replace': _github('repo_name', 'base')}},
                             entity_id=1, version_id=10)
    assert result['status'] != 'invalid', result
