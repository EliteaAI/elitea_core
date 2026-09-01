"""Focused contract tests for durable nested parallel HITL metadata."""

import importlib.util
import pathlib


_SPEC = importlib.util.spec_from_file_location(
    'parallel_hitl',
    pathlib.Path(__file__).resolve().parents[3] / 'utils' / 'parallel_hitl.py',
)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
decisions_for_child = _MODULE.decisions_for_child
begin_execution_generation = _MODULE.begin_execution_generation
claim_supervisor_decision_phase = _MODULE.claim_supervisor_decision_phase
is_current_execution = _MODULE.is_current_execution
merge_interrupts = _MODULE.merge_interrupts
merge_supervisor_roster = _MODULE.merge_supervisor_roster
normalize_interrupts = _MODULE.normalize_interrupts
pending_interrupts = _MODULE.pending_interrupts
pending_supervisor_decisions = _MODULE.pending_supervisor_decisions
persist_supervisor_decision = _MODULE.persist_supervisor_decision
partition_root_hitl_decisions = _MODULE.partition_root_hitl_decisions
requires_plural_persistence = _MODULE.requires_plural_persistence
retire_child_interrupts = _MODULE.retire_child_interrupts
retire_interrupts = _MODULE.retire_interrupts
retire_all_interrupts = _MODULE.retire_all_interrupts
validate_child_decisions = _MODULE.validate_child_decisions


def test_supervised_pause_preserves_live_resume_strategy_without_worker_child_lineage():
    response = {
        'resume_strategy': 'supervised_child',
        'root_thread_id': 'root-1',
        'hitl_interrupt': {
            'interrupt_id': 'i-live',
            'tool_call_id': 'call-live',
        },
    }

    normalized = normalize_interrupts(response)

    assert normalized[0]['resume_strategy'] == 'supervised_child'


def test_supervisor_decision_and_roster_are_bounded_durable_message_state():
    meta = persist_supervisor_decision({}, {
        'decision_id': 'd-1',
        'interrupt_id': 'i-1',
        'action': 'approve',
    })
    meta = merge_supervisor_roster(meta, {
        'root_thread_id': 'root-1',
        'tool_call_id': 'call-a',
        'state': 'paused',
    })

    assert pending_supervisor_decisions(meta) == [{
        'decision_id': 'd-1',
        'interrupt_id': 'i-1',
        'action': 'approve',
        'phase': 'queued',
    }]
    assert meta['parallel_hitl_roster']['children']['call-a']['state'] == 'paused'

    retired = retire_all_interrupts(meta)
    assert 'parallel_hitl_decisions' not in retired
    assert 'parallel_hitl_roster' not in retired


def test_supervised_oauth_tokens_are_transport_only_not_durable_metadata():
    meta = persist_supervisor_decision({}, {
        'decision_id': 'd-auth',
        'interrupt_id': 'i-auth',
        'action': 'authorize',
        '_mcp_tokens': {
            'https://mcp.example.test': {'access_token': 'test-secret'},
        },
    })

    persisted = pending_supervisor_decisions(meta)[0]
    assert persisted['decision_id'] == 'd-auth'
    assert '_mcp_tokens' not in persisted


def test_supervisor_fallback_has_one_phase_owner():
    meta = persist_supervisor_decision({}, {
        'decision_id': 'd-fallback',
        'interrupt_id': 'i-fallback',
        'pending_interrupt': {'interrupt_id': 'i-fallback'},
    }, phase='fallback_pending')

    claimed_meta, claimed = claim_supervisor_decision_phase(
        meta, 'd-fallback', {'fallback_pending'}, 'fallback_starting',
    )
    losing_meta, claimed_again = claim_supervisor_decision_phase(
        claimed_meta, 'd-fallback', {'fallback_pending'}, 'recovering',
    )

    assert claimed is True
    assert claimed_again is False
    assert pending_supervisor_decisions(losing_meta)[0]['phase'] == 'fallback_starting'


def test_mixed_root_resume_partitions_sensitive_and_authorization_decisions():
    sensitive = {
        'interrupt_id': 'hitl-delete',
        'available_actions': ['approve', 'reject'],
    }
    authorization = {
        'interrupt_id': 'hitl-auth',
        'available_actions': ['authorize', 'skip'],
    }

    sensitive_ids, authorization_ids = partition_root_hitl_decisions(
        [sensitive],
        [authorization],
        [
            {'interrupt_id': 'hitl-auth', 'action': 'skip'},
            {'interrupt_id': 'hitl-delete', 'action': 'reject'},
        ],
    )

    assert sensitive_ids == ['hitl-delete']
    assert authorization_ids == ['hitl-auth']


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


def test_nested_leaf_threads_keep_durable_child_as_resume_route():
    response = {
        'hitl_interrupts': [
            {
                'interrupt_id': 'leaf-1',
                'child_thread_id': 'leaf-thread-1',
                'thread_id': 'leaf-thread-1',
                'tool_call_id': 'leaf-tool-1',
            },
            {
                'interrupt_id': 'leaf-2',
                'child_thread_id': 'leaf-thread-2',
                'thread_id': 'leaf-thread-2',
                'tool_call_id': 'leaf-tool-2',
            },
        ],
        'metadata': {'child_thread_id': 'durable-child'},
    }

    normalized = normalize_interrupts(response)

    assert [item['child_thread_id'] for item in normalized] == [
        'durable-child', 'durable-child',
    ]
    assert [item['thread_id'] for item in normalized] == [
        'leaf-thread-1', 'leaf-thread-2',
    ]
    assert all(item['resume_strategy'] == 'aggregate_child' for item in normalized)


def test_nested_in_process_leaf_threads_resume_the_root_worker():
    response = {
        'thread_id': 'root-worker-thread',
        'hitl_interrupts': [
            {
                'interrupt_id': 'leaf-1',
                'child_thread_id': 'sdk-leaf-thread-1',
                'thread_id': 'sdk-leaf-thread-1',
                'resume_strategy': 'aggregate_child',
            },
            {
                'interrupt_id': 'leaf-2',
                'child_thread_id': 'sdk-leaf-thread-2',
                'thread_id': 'sdk-leaf-thread-2',
                'resume_strategy': 'aggregate_child',
            },
        ],
    }

    normalized = normalize_interrupts(response)

    assert [item['child_thread_id'] for item in normalized] == [
        'sdk-leaf-thread-1', 'sdk-leaf-thread-2',
    ]
    assert all(item['resume_strategy'] == 'single' for item in normalized)
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
        'continuation_error': {'code': 'output_continuation_exhausted'},
        'budget_error_code': 'member_budget_exceeded',
        'is_error': True,
        'error': 'old failure',
    })
    fresh = begin_execution_generation(old, 'new-run')

    assert fresh['execution_generation'] == 'new-run'
    assert 'resolved_hitl_interrupt_ids' not in fresh
    assert 'continuation_error' not in fresh
    assert 'budget_error_code' not in fresh
    assert 'is_error' not in fresh
    assert 'error' not in fresh
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


def test_root_decisions_allow_one_pending_interrupt_at_a_time():
    pending = [
        {'interrupt_id': 'i-1', 'available_actions': ['approve', 'reject']},
        {'interrupt_id': 'i-2', 'available_actions': ['approve', 'reject']},
    ]

    validate_child_decisions(
        pending,
        [{'interrupt_id': 'i-1', 'action': 'approve'}],
        require_all=False,
    )

    for decisions in (
        [],
        [{'interrupt_id': 'unknown', 'action': 'approve'}],
        [{'interrupt_id': 'i-1', 'action': 'edit'}],
    ):
        try:
            validate_child_decisions(
                pending, decisions, require_all=False,
            )
        except ValueError:
            continue
        raise AssertionError(f'expected invalid partial decisions to fail: {decisions}')


def test_regenerate_clears_stopped_flag_but_continue_does_not():
    plugin_root = pathlib.Path(__file__).resolve().parents[3]
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
    plugin_root = pathlib.Path(__file__).resolve().parents[3]
    event_source = (plugin_root / 'events' / 'message_stream.py').read_text()
    continue_source = (plugin_root / 'rpc' / 'chat_all.py').read_text()
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
    # Stop logic is now in chat_stop_task RPC (chat_all.py), called by api/v2/task.py
    stop_rpc = continue_source[
        continue_source.index('def chat_stop_task('):
    ]
    assert '.with_for_update(of=ConversationMessageGroup)' in root_resume
    assert 'retire_interrupts(' in root_resume
    assert '.with_for_update(of=ConversationMessageGroup).first()' in pause_handler
    assert '.with_for_update(of=ConversationMessageGroup).first()' in child_resume
    assert 'This sub-orchestrator approval expired' in continue_source
    assert '.with_for_update(of=ConversationMessageGroup).first()' in stop_rpc
    assert 'retire_all_interrupts(msg_group.meta)' in stop_rpc
    assert 'retire_all_interrupts(msg_group.meta)' in event_source
    assert 'retire_all_interrupts(msg_group.meta)' in regenerate_source
    assert 'begin_execution_generation(' in regenerate_source
    assert 'is_current_execution(msg_group.meta, payload)' in event_source
    assert chat_models_source.count('execution_generation: Optional[str]') >= 2
    assert "'execution_generation': getattr(parsed, 'execution_generation', None)" in predict_source
    assert '.with_for_update(of=ConversationMessageGroup)' in regenerate_source


def test_supervised_fallback_cannot_launch_a_competing_root_worker():
    plugin_root = pathlib.Path(__file__).resolve().parents[3]
    continue_source = (plugin_root / 'rpc' / 'chat_all.py').read_text()
    callback_source = (plugin_root / 'methods' / 'task_callbacks.py').read_text()

    offer = continue_source[
        continue_source.index('def _offer_supervised_decision'):
        continue_source.index('@web.rpc("chat_continue_predict_sio"')
    ]
    claim = continue_source[
        continue_source.index('def _claim_stopped_supervisor_fallback'):
        continue_source.index('def _offer_supervised_decision')
    ]
    recovery = callback_source[
        callback_source.index('def _maybe_recover_supervised_hitl'):
        callback_source.index('def _chat_stream_already_closed')
    ]

    assert "task_status != 'stopped'" in claim
    assert "{'fallback_pending'}" in claim
    assert 'return None' in offer
    assert 'ConversationMessageGroup.task_id == task_id' in recovery
    assert 'self.chat_continue_predict_sio(' in recovery
    assert 'self.continue_predict_sio(' not in recovery
    assert '_internal_token=INTERNAL_CONTINUE_TOKEN' in recovery


def test_live_supervised_mcp_resume_forwards_fresh_tokens_only_on_event_bus():
    plugin_root = pathlib.Path(__file__).resolve().parents[3]
    continue_source = (plugin_root / 'rpc' / 'chat_all.py').read_text()
    offer = continue_source[
        continue_source.index('def _offer_supervised_decision'):
        continue_source.index('@web.rpc("chat_continue_predict_sio"')
    ]

    persist_index = offer.index('persist_supervisor_decision(')
    transport_index = offer.index("transport_decision['_mcp_tokens']")
    emit_index = offer.index("'decision': transport_decision")
    assert persist_index < transport_index < emit_index
    assert "parsed.mcp_auth_resume and parsed.mcp_tokens" in offer


def test_repeated_resolved_supervised_decision_cannot_replay_the_root():
    plugin_root = pathlib.Path(__file__).resolve().parents[3]
    continue_source = (plugin_root / 'rpc' / 'chat_all.py').read_text()

    resolved_guard = continue_source[
        continue_source.index('def _resolved_resume_result'):
        continue_source.index('def _claim_stopped_supervisor_fallback')
    ]
    offer = continue_source[
        continue_source.index('def _offer_supervised_decision'):
        continue_source.index('@web.rpc("chat_continue_predict_sio"')
    ]

    assert "meta.get('resolved_hitl_interrupt_ids')" in resolved_guard
    assert "meta.get('resolved_authorization_request_ids')" in resolved_guard
    assert "'already_resolved': True" in resolved_guard
    assert 'return self._resolved_resume_result(response_msg, parsed)' in offer


def test_resolved_resume_identity_helper_is_registered_on_pylon_module():
    """Root MCP resumes reach this helper after the supervised fast path misses."""
    plugin_root = pathlib.Path(__file__).resolve().parents[3]
    continue_source = (plugin_root / 'rpc' / 'chat_all.py').read_text()

    helper = continue_source[
        continue_source.index('def _explicit_resume_interrupt_ids') - 40:
        continue_source.index('def _resolved_resume_result')
    ]

    assert '@web.method()' in helper
    assert 'def _explicit_resume_interrupt_ids(self, parsed)' in helper
