"""Focused contract tests for durable nested parallel HITL metadata."""

import importlib.util
import pathlib


_SPEC = importlib.util.spec_from_file_location(
    'parallel_hitl', pathlib.Path(__file__).resolve().parents[1] / 'parallel_hitl.py',
)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
decisions_for_child = _MODULE.decisions_for_child
begin_execution_generation = _MODULE.begin_execution_generation
is_current_execution = _MODULE.is_current_execution
merge_interrupts = _MODULE.merge_interrupts
normalize_interrupts = _MODULE.normalize_interrupts
pending_interrupts = _MODULE.pending_interrupts
requires_plural_persistence = _MODULE.requires_plural_persistence
retire_child_interrupts = _MODULE.retire_child_interrupts
retire_interrupts = _MODULE.retire_interrupts
retire_all_interrupts = _MODULE.retire_all_interrupts
validate_child_decisions = _MODULE.validate_child_decisions


def test_pause_merge_preserves_sibling_children_and_adds_identity():
    meta = {
        'hitl_interrupts': [{
            'interrupt_id': 'i-1', 'child_thread_id': 'child-1',
            'tool_call_id': 'tool-1',
        }],
    }
    response = {
        'thread_id': 'child-2',
        'hitl_interrupts': [{'tool_call_id': 'tool-2', 'message': 'approve?'}],
        'metadata': {
            'child_thread_id': 'child-2',
            'parent_agent_call_id': 'call-b2',
            'sibling_ordinal': 2,
        },
    }

    merged = merge_interrupts(meta, response)

    assert [item['child_thread_id'] for item in merged] == ['child-1', 'child-2']
    assert 'interrupt_id' not in merged[1]
    assert merged[1]['resume_strategy'] == 'aggregate_child'
    assert merged[1]['parent_agent_call_id'] == 'call-b2'


def test_root_singular_pause_keeps_legacy_scalar_shape():
    response = {
        'thread_id': 'root-thread',
        'hitl_interrupt': {'tool_call_id': 'tool-root'},
    }
    normalized = normalize_interrupts(response)
    assert normalized == [{
        'tool_call_id': 'tool-root', 'resume_strategy': 'single',
    }]
    assert requires_plural_persistence(normalized, response) is False


def test_incremental_root_pauses_promote_to_plural_without_losing_tombstones():
    """Concurrent roots arrive as separate scalar callbacks under the row lock."""
    tombstones = ['old-1', 'old-2', 'old-3', 'old-4']

    for first_id, second_id in (('new-1', 'new-2'), ('new-2', 'new-1')):
        meta = {'resolved_hitl_interrupt_ids': tombstones}
        first_response = {
            'hitl_interrupt': {
                'interrupt_id': first_id,
                'tool_call_id': f'tool-{first_id}',
            },
        }
        first = merge_interrupts(meta, first_response)
        meta['hitl_interrupt'] = first[0]
        if requires_plural_persistence(first, first_response):
            meta['hitl_interrupts'] = first

        assert 'hitl_interrupts' not in meta

        second_response = {
            'hitl_interrupt': {
                'interrupt_id': second_id,
                'tool_call_id': f'tool-{second_id}',
            },
        }
        merged = merge_interrupts(meta, second_response)
        meta['hitl_interrupt'] = merged[0]
        if requires_plural_persistence(merged, second_response):
            meta['hitl_interrupts'] = merged

        assert [item['interrupt_id'] for item in pending_interrupts(meta)] == [
            first_id, second_id,
        ]
        assert meta['hitl_interrupt']['interrupt_id'] == first_id
        assert meta['resolved_hitl_interrupt_ids'] == tombstones


def test_one_durable_child_pause_requires_routed_plural_shape():
    response = {
        'thread_id': 'root-stream-thread',
        'hitl_interrupt': {'tool_call_id': 'leaf-tool'},
        'metadata': {'child_thread_id': 'durable-child'},
    }
    normalized = normalize_interrupts(response)
    assert normalized[0]['child_thread_id'] == 'durable-child'
    assert normalized[0]['resume_strategy'] == 'aggregate_child'
    assert requires_plural_persistence(normalized, response) is True


def test_two_interrupts_in_one_durable_child_preserve_both():
    response = {
        'hitl_interrupts': [
            {'tool_call_id': 'leaf-1'}, {'tool_call_id': 'leaf-2'},
        ],
        'metadata': {'child_thread_id': 'durable-child'},
    }
    normalized = normalize_interrupts(response)
    assert [item['tool_call_id'] for item in normalized] == ['leaf-1', 'leaf-2']
    assert all(item['child_thread_id'] == 'durable-child' for item in normalized)
    assert requires_plural_persistence(normalized, response) is True


def test_interrupt_lineage_prefixes_outer_root_and_drops_replayed_self_hop():
    response = {
        'hitl_interrupt': {
            'tool_call_id': 'sensitive-leaf',
            'parent_agent_path': [
                {'name': 'Full Name resolver', 'call_id': 'replay-b'},
                {'name': 'Name Resolver', 'call_id': 'call-c'},
            ],
        },
        'metadata': {
            'child_thread_id': 'durable-b',
            'parent_agent_path': [
                {'name': 'Full Name resolver', 'call_id': 'stable-b', 'sibling_ordinal': 2},
            ],
        },
    }

    normalized = normalize_interrupts(response)

    assert normalized[0]['parent_agent_path'] == [
        {'name': 'Full Name resolver', 'call_id': 'stable-b', 'sibling_ordinal': 2},
        {'name': 'Name Resolver', 'call_id': 'call-c'},
    ]


def test_resume_forwards_all_unscoped_leaf_decisions_to_target_child():
    decisions = [
        {'interrupt_id': 'leaf-1', 'action': 'approve'},
        {'interrupt_id': 'leaf-2', 'action': 'reject'},
    ]
    assert decisions_for_child(decisions, 'child-1', 'tool-b1') == decisions


def test_leaf_thread_ids_do_not_collapse_aggregate_child_decisions():
    decisions = [
        {'thread_id': 'leaf-1', 'action': 'approve'},
        {'thread_id': 'leaf-2', 'action': 'reject'},
    ]
    assert decisions_for_child(decisions, 'durable-child', 'tool-b1') == decisions


def test_resume_retires_only_owned_child_interrupts():
    meta = {
        'hitl_interrupt': {'interrupt_id': 'i-1', 'child_thread_id': 'child-1'},
        'hitl_interrupts': [
            {'interrupt_id': 'i-1', 'child_thread_id': 'child-1'},
            {'interrupt_id': 'i-2', 'child_thread_id': 'child-2'},
        ],
    }

    updated = retire_child_interrupts(meta, 'child-1', ['i-1'])

    assert updated['hitl_interrupts'] == [
        {'interrupt_id': 'i-2', 'child_thread_id': 'child-2'},
    ]
    assert updated['hitl_interrupt']['interrupt_id'] == 'i-2'
    assert updated['resolved_hitl_interrupt_ids'] == ['i-1']


def test_late_pause_for_resolved_interrupt_is_not_resurrected():
    meta = {
        'hitl_interrupt': {
            'interrupt_id': 'i-resolved',
            'child_thread_id': 'child-1',
            'tool_call_id': 'tool-1',
        },
    }
    retired = retire_child_interrupts(meta, 'child-1', ['i-resolved'])

    merged = merge_interrupts(retired, {
        'hitl_interrupt': {
            'interrupt_id': 'i-resolved',
            'tool_call_id': 'tool-1',
        },
        'metadata': {'child_thread_id': 'child-1'},
    })

    assert merged == []
    assert retired['resolved_hitl_interrupt_ids'] == ['i-resolved']


def test_root_resume_retires_only_decided_interrupts_and_blocks_late_pause():
    meta = {
        'hitl_interrupt': {'interrupt_id': 'root-1', 'tool_call_id': 'leaf-1'},
        'hitl_interrupts': [
            {'interrupt_id': 'root-1', 'tool_call_id': 'leaf-1'},
            {'interrupt_id': 'root-2', 'tool_call_id': 'leaf-2'},
        ],
    }

    retired = retire_interrupts(meta, ['root-1'])

    assert retired['hitl_interrupts'] == [
        {'interrupt_id': 'root-2', 'tool_call_id': 'leaf-2'},
    ]
    assert retired['hitl_interrupt']['interrupt_id'] == 'root-2'
    assert retired['resolved_hitl_interrupt_ids'] == ['root-1']
    assert merge_interrupts(retired, {
        'hitl_interrupt': {'interrupt_id': 'root-1', 'tool_call_id': 'leaf-1'},
    }) == [
        {'interrupt_id': 'root-2', 'tool_call_id': 'leaf-2'},
    ]


def test_regenerate_generation_allows_reused_id_and_rejects_old_callbacks():
    old = retire_all_interrupts({
        'execution_generation': 'old-run',
        'hitl_interrupt': {'interrupt_id': 'stable-id'},
    })
    fresh = begin_execution_generation(old, 'new-run')

    assert fresh['execution_generation'] == 'new-run'
    assert 'resolved_hitl_interrupt_ids' not in fresh
    assert is_current_execution(fresh, {'execution_generation': 'new-run'})
    assert not is_current_execution(fresh, {'execution_generation': 'old-run'})
    assert not is_current_execution(fresh, {})
    assert merge_interrupts(fresh, {
        'hitl_interrupt': {'interrupt_id': 'stable-id'},
    }) == [{'interrupt_id': 'stable-id', 'resume_strategy': 'single'}]


def test_new_interrupt_after_resume_is_persisted_and_tombstones_are_bounded():
    meta = {'resolved_hitl_interrupt_ids': [f'i-{index}' for index in range(300)]}
    merged = merge_interrupts(meta, {
        'hitl_interrupt': {'interrupt_id': 'i-new', 'tool_call_id': 'tool-2'},
        'metadata': {'child_thread_id': 'child-1'},
    })
    retired = retire_all_interrupts({**meta, 'hitl_interrupt': merged[0]})

    assert [item['interrupt_id'] for item in merged] == ['i-new']
    assert len(retired['resolved_hitl_interrupt_ids']) == 256
    assert retired['resolved_hitl_interrupt_ids'][-1] == 'i-new'


def test_child_decisions_require_exact_unique_identities_and_valid_actions():
    pending = [
        {'interrupt_id': 'i-1', 'available_actions': ['approve', 'reject']},
        {'interrupt_id': 'i-2', 'available_actions': ['approve', 'reject']},
    ]
    validate_child_decisions(pending, [
        {'interrupt_id': 'i-1', 'action': 'approve'},
        {'interrupt_id': 'i-2', 'action': 'reject'},
    ])

    invalid = [
        [
            {'interrupt_id': 'i-1', 'action': 'approve'},
            {'interrupt_id': 'i-1', 'action': 'reject'},
        ],
        [{'interrupt_id': 'i-1', 'action': 'approve'}],
        [
            {'interrupt_id': 'i-1', 'action': 'approve'},
            {'interrupt_id': 'unknown', 'action': 'reject'},
        ],
        [
            {'interrupt_id': 'i-1', 'action': 'edit'},
            {'interrupt_id': 'i-2', 'action': 'approve'},
        ],
    ]
    for decisions in invalid:
        try:
            validate_child_decisions(pending, decisions)
        except ValueError:
            continue
        raise AssertionError(f'expected invalid decisions to fail: {decisions}')


def test_regenerate_clears_stopped_flag_but_continue_does_not():
    plugin_root = pathlib.Path(__file__).resolve().parents[2]
    regenerate_source = (plugin_root / 'api' / 'v2' / 'regenerate.py').read_text()
    continue_source = (plugin_root / 'rpc' / 'chat_all.py').read_text()
    authorization_index = regenerate_source.index('auth.current_user().get("id") not in')
    clear_index = regenerate_source.index('clear_chat_run_stopped')
    assert clear_index > authorization_index
    child_resume = continue_source[
        continue_source.index('def _continue_child_resume'):
        continue_source.index("@web.rpc(f'chat_predict_summary_content'")
    ]
    assert 'clear_chat_run_stopped' not in child_resume


def test_all_hitl_jsonb_mutations_lock_the_message_row_and_stop_clears_cards():
    plugin_root = pathlib.Path(__file__).resolve().parents[2]
    event_source = (plugin_root / 'events' / 'message_stream.py').read_text()
    continue_source = (plugin_root / 'rpc' / 'chat_all.py').read_text()
    stop_source = (plugin_root / 'api' / 'v2' / 'task.py').read_text()
    regenerate_source = (plugin_root / 'api' / 'v2' / 'regenerate.py').read_text()
    chat_models_source = (plugin_root / 'models' / 'pd' / 'chat.py').read_text()
    predict_source = (plugin_root / 'utils' / 'predict_utils.py').read_text()

    pause_handler = event_source[event_source.index('def chat_message_stream_pause'):]
    child_resume = continue_source[
        continue_source.index('def _continue_child_resume'):
        continue_source.index("@web.rpc(f'chat_predict_summary_content'")
    ]
    root_resume = continue_source[
        continue_source.index('def continue_predict_sio'):
        continue_source.index('def _continue_child_resume')
    ]
    assert '.with_for_update(of=ConversationMessageGroup)' in root_resume
    assert 'retire_interrupts(' in root_resume
    assert '.with_for_update(of=ConversationMessageGroup).first()' in pause_handler
    assert '.with_for_update(of=ConversationMessageGroup).first()' in child_resume
    assert 'This sub-orchestrator approval expired' in continue_source
    assert '.with_for_update(of=ConversationMessageGroup).first()' in stop_source
    assert 'retire_all_interrupts(msg_group.meta)' in stop_source
    assert 'retire_all_interrupts(msg_group.meta)' in event_source
    assert 'retire_all_interrupts(msg_group.meta)' in regenerate_source
    assert 'begin_execution_generation(' in regenerate_source
    assert 'is_current_execution(msg_group.meta, payload)' in event_source
    assert chat_models_source.count('execution_generation: Optional[str]') >= 2
    assert "'execution_generation': getattr(parsed, 'execution_generation', None)" in predict_source
    assert '.with_for_update(of=ConversationMessageGroup)' in regenerate_source
