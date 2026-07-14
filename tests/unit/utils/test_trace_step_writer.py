import importlib.util
import pathlib
import sys
import types


PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


def _load_module(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


for package_name in (
    'plugins',
    'plugins.elitea_core',
    'plugins.elitea_core.models',
    'plugins.elitea_core.utils',
):
    package = types.ModuleType(package_name)
    package.__path__ = []
    sys.modules.setdefault(package_name, package)


class _MessageTraceStep:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


message_trace_step = types.ModuleType('plugins.elitea_core.models.message_trace_step')
message_trace_step.MessageTraceStep = _MessageTraceStep
sys.modules['plugins.elitea_core.models.message_trace_step'] = message_trace_step

_load_module(
    'plugins.elitea_core.utils.tool_call_dedup',
    'utils/tool_call_dedup.py',
)
trace_step_writer = _load_module(
    'plugins.elitea_core.utils.trace_step_writer',
    'utils/trace_step_writer.py',
)

_merge_thinking_steps = trace_step_writer._merge_thinking_steps
_row_key = trace_step_writer._row_key
thinking_step_to_row = trace_step_writer.thinking_step_to_row
tool_call_to_row = trace_step_writer.tool_call_to_row


def test_tool_attrs_keep_display_lineage_and_drop_arbitrary_metadata():
    row = tool_call_to_row(1, 'run-1', {
        'run_id': 'run-1',
        'tool_name': 'read_file',
        'metadata': {
            'parent_agent_name': 'Name Resolver',
            'parent_agent_call_id': 'leaf-1',
            'parent_agent_path': [
                {'name': 'Full Name resolver', 'call_id': 'root-1', 'sibling_ordinal': 1},
            ],
            'toolkit_type': 'github',
            'mcp_session_id': 'must-not-persist',
            'authorization': 'must-not-persist',
        },
        'tool_meta': {
            'name': 'read_file',
            'description': 'not needed by the resting chip',
            'metadata': {'display_name': 'Repository'},
        },
    })

    assert row.parent_agent_call_id == 'leaf-1'
    assert row.attrs['metadata']['parent_agent_path'][0]['call_id'] == 'root-1'
    assert 'mcp_session_id' not in row.attrs['metadata']
    assert 'authorization' not in row.attrs['metadata']
    assert 'description' not in row.attrs['tool_meta']


def test_thinking_row_round_trips_in_process_hierarchy_and_visibility():
    path = [{'name': 'Full Name resolver', 'call_id': 'root-1', 'sibling_ordinal': 2}]
    row = thinking_step_to_row(1, {
        'tool_run_id': 'llm-1',
        'parent_agent_name': 'Name Resolver',
        'parent_agent_call_id': 'leaf-1',
        'parent_agent_path': path,
        'text': '',
        'thinking': '',
        'message': {'response_metadata': {'tool_name': 'Name LLM', 'secret': 'drop'}},
    })

    assert row.has_visible_content is True
    assert row.attrs['parent_agent_path'] == path
    assert row.attrs['response_metadata'] == {
        'tool_name': 'Name LLM',
        'metadata': {
            'parent_agent_name': 'Name Resolver',
            'parent_agent_call_id': 'leaf-1',
            'parent_agent_path': path,
        },
    }


def test_blank_root_transition_is_not_visible():
    row = thinking_step_to_row(1, {
        'tool_run_id': 'llm-transition',
        'text': '  ',
        'thinking': '',
        'message': {'response_metadata': {}},
    })

    assert row.has_visible_content is False


def test_parallel_thinking_steps_with_equal_timestamps_keep_distinct_run_ids():
    timestamp = '2026-07-14T10:00:00+00:00'
    merged = _merge_thinking_steps([], [
        {'tool_run_id': 'llm-1', 'timestamp_start': timestamp},
        {'tool_run_id': 'llm-2', 'timestamp_start': timestamp},
    ])

    assert [step['tool_run_id'] for step in merged] == ['llm-1', 'llm-2']
    rows = [thinking_step_to_row(1, step) for step in merged]
    assert _row_key(rows[0]) != _row_key(rows[1])


def test_thinking_delta_replaces_same_run_without_changing_order():
    merged = _merge_thinking_steps(
        [{'tool_run_id': 'llm-1', 'text': 'partial'}],
        [{'tool_run_id': 'llm-1', 'text': 'complete'}],
    )

    assert merged == [{'tool_run_id': 'llm-1', 'text': 'complete'}]
