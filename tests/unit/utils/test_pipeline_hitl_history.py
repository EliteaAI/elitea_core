import ast
import pathlib
from types import SimpleNamespace


ROOT = pathlib.Path(__file__).resolve().parents[3]
SOURCE_PATH = ROOT / 'utils' / 'pipeline_hitl_history.py'


def _load_contract_functions():
    tree = ast.parse(SOURCE_PATH.read_text())
    names = {
        'PIPELINE_HITL_INTERACTION_TYPE',
        'PIPELINE_HITL_HISTORY_CONTRACT_VERSION',
        'get_direct_pipeline_hitl_interrupt',
        'get_pipeline_hitl_interrupt',
        'pipeline_hitl_decision_text',
    }
    selected = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            assigned = {target.id for target in node.targets if isinstance(target, ast.Name)}
            if assigned & names:
                selected.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in names:
            selected.append(node)
    namespace = {}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(SOURCE_PATH), 'exec'), namespace)
    return namespace


def test_only_versioned_pipeline_node_interrupts_use_chat_segmentation():
    contract = _load_contract_functions()
    detect = contract['get_pipeline_hitl_interrupt']
    interrupt = {
        'interaction_type': 'pipeline_hitl_node',
        'history_contract_version': 1,
        'interrupt_id': 'hitl-occurrence-1',
        'node_name': 'Review',
    }

    assert detect({'hitl_interrupt': interrupt})['interrupt_id'] == 'hitl-occurrence-1'
    assert detect({'hitl_interrupt': {**interrupt, 'history_contract_version': '1'}})
    assert detect({'hitl_interrupt': {**interrupt, 'interrupt_id': ''}}) is None
    assert detect({'hitl_interrupt': {**interrupt, 'history_contract_version': 'invalid'}}) is None
    assert detect({'hitl_interrupt': {**interrupt, 'interaction_type': 'sensitive_tool'}}) is None


def test_chat_segmentation_is_limited_to_direct_pipeline_execution():
    contract = _load_contract_functions()
    detect = contract['get_direct_pipeline_hitl_interrupt']
    interrupt = {
        'interaction_type': 'pipeline_hitl_node',
        'history_contract_version': 1,
        'interrupt_id': 'hitl-occurrence-1',
    }
    direct_pipeline = SimpleNamespace(author_participant=SimpleNamespace(
        entity_name='application', meta={'agent_type': 'pipeline'},
    ))
    parent_agent = SimpleNamespace(author_participant=SimpleNamespace(
        entity_name='application', meta={'agent_type': 'openai'},
    ))

    assert detect(direct_pipeline, {'hitl_interrupt': interrupt}) == interrupt
    assert detect(parent_agent, {'hitl_interrupt': interrupt}) is None


def test_nested_and_parallel_pipeline_interrupts_do_not_segment_chat_history():
    detect = _load_contract_functions()['get_direct_pipeline_hitl_interrupt']
    pipeline = SimpleNamespace(author_participant=SimpleNamespace(
        entity_name='application', meta={'agent_type': 'pipeline'},
    ))
    interrupt = {
        'interaction_type': 'pipeline_hitl_node',
        'history_contract_version': 1,
        'interrupt_id': 'hitl-occurrence-1',
    }

    for nested in (
        {'parent_agent_name': 'Coordinator'},
        {'parent_agent_call_id': 'call-1'},
        {'parent_agent_path': [{'name': 'Coordinator', 'call_id': 'call-1'}]},
        {'child_thread_id': 'child-1'},
        {'resume_strategy': 'aggregate_child'},
        {'resume_strategy': 'supervised_child'},
    ):
        assert detect(pipeline, {'hitl_interrupt': {**interrupt, **nested}}) is None

    second = {**interrupt, 'interrupt_id': 'hitl-occurrence-2'}
    assert detect(pipeline, {
        'hitl_interrupt': interrupt,
        'hitl_interrupts': [interrupt, second],
    }) is None


def test_decisions_become_canonical_user_chat_content():
    render = _load_contract_functions()['pipeline_hitl_decision_text']

    assert render('approve', '') == 'Approved'
    assert render('reject', '') == 'Rejected'
    assert render('edit', 'updated joke') == 'updated joke'


def test_continue_flow_redirects_resume_to_new_message_segments():
    source = (ROOT / 'rpc' / 'chat_all.py').read_text()
    continue_flow = source[source.index('def continue_predict_sio('):]

    assert 'create_pipeline_hitl_resume_segments(' in continue_flow
    assert "'type': 'chat_user_message'" in continue_flow
    assert "event=SioEvents.chat_message_sync" in continue_flow
    assert "payload['message_id'] = str(response_msg.uuid)" in continue_flow


def test_continuation_replies_to_the_persisted_user_decision():
    source = SOURCE_PATH.read_text()
    create_segments = source[source.index('def create_pipeline_hitl_resume_segments('):]

    decision_flush = create_segments.index('session.flush()')
    continuation_create = create_segments.index('continuation_group = ConversationMessageGroup(')
    assert decision_flush < continuation_create
    assert 'reply_to_id=decision_group.id' in create_segments
    assert 'reply_to=decision_group' not in create_segments


def test_pause_flow_persists_the_rendered_prompt_before_interrupt_meta():
    source = (ROOT / 'events' / 'message_stream.py').read_text()
    pause = source[source.index('def chat_message_stream_pause('):]

    persist_at = pause.index('persist_pipeline_hitl_prompt(')
    merge_at = pause.index('merged = merge_interrupts(')
    assert persist_at < merge_at
