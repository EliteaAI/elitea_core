"""#7460: recent turns' tool calls are replayed as compact tool_call/tool_result stubs.

Background: Elitea replays only each assistant turn's final text. After a few
"File saved successfully" replies with no visible tool calls, Claude Sonnet 5
learns to write that reply without calling create_file. Replaying short stub
pairs for the last N turns restores the evidence that saving required a call.
"""
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module  # noqa: E402


@pytest.fixture(scope='module')
def stubs():
    return load_utils_module(TESTS_DIR.parent / 'utils', 'history_tool_stubs')


@pytest.fixture(scope='module')
def chat_history_module():
    """chat_history with the same minimal stubs as test_chat_history.py."""
    role = types.SimpleNamespace(
        user=types.SimpleNamespace(value='user'),
        assistant=types.SimpleNamespace(value='assistant'),
    )

    class FakeChatHistory:
        def __init__(self, role, content, additional_kwargs=None):
            self.role, self.content = role, content

        def dict(self, **kwargs):
            return {'role': self.role, 'content': self.content}

    def item_cls(identity):
        return type(identity, (), {'__mapper_args__': {'polymorphic_identity': identity}})

    enums_mod = types.ModuleType('enums')
    enums_mod.ChatHistoryRole = role
    enums_mod.ParticipantTypes = types.SimpleNamespace(user='user')
    mods = {
        'plugins.elitea_core.models.enums.all': enums_mod,
        'plugins.elitea_core.models.pd.chat': types.SimpleNamespace(ChatHistory=FakeChatHistory),
        'plugins.elitea_core.models.message_group': types.SimpleNamespace(ConversationMessageGroup=object),
        'plugins.elitea_core.models.message_items.attachment': types.SimpleNamespace(
            AttachmentMessageItem=item_cls('attachment')),
        'plugins.elitea_core.models.message_items.context': types.SimpleNamespace(
            ContextMessageItem=item_cls('context')),
        'plugins.elitea_core.models.message_items.text': types.SimpleNamespace(TextMessageItem=item_cls('text')),
        'plugins.elitea_core.models.message_items.canvas': types.SimpleNamespace(
            CanvasMessageItem=item_cls('canvas'), CanvasVersionItem=object),
    }
    return load_utils_module(TESTS_DIR.parent / 'utils', 'chat_history', extra_stubs=mods)


class Item:
    def __init__(self, content, group_id):
        self.item_type, self.content, self.message_group_id = 'text', content, group_id


class Group:
    def __init__(self, gid, author, text):
        self.id = gid
        self.author_participant = types.SimpleNamespace(entity_name=author)
        self.message_items = [Item(text, gid)] if text is not None else []


def _step(step_id, name='create_file', inputs=None, output='ok', is_error=False):
    return {'id': step_id, 'tool_name': name, 'tool_inputs': inputs or {}, 'tool_output': output,
            'is_error': is_error}


class TestStubSettings:

    def test_defaults_on_with_three_turns(self, stubs):
        assert stubs.stub_settings({}) == {'enabled': True, 'last_n': 3}

    def test_reads_admin_values(self, stubs):
        cfg = {'chat_history': {'tool_call_stubs': {'enabled': False, 'last_n': 5}}}
        assert stubs.stub_settings(cfg) == {'enabled': False, 'last_n': 5}

    def test_garbage_last_n_falls_back(self, stubs):
        cfg = {'chat_history': {'tool_call_stubs': {'last_n': 'many'}}}
        assert stubs.stub_settings(cfg)['last_n'] == 3


class TestBuildToolStubMessages:

    def test_no_steps_no_messages(self, stubs):
        assert stubs.build_tool_stub_messages([]) == []

    def test_pair_shape_matches_langchain_dicts(self, stubs):
        msgs = stubs.build_tool_stub_messages([_step(7, inputs={'filename': 'a.md'})])
        assert msgs[0] == {'role': 'assistant', 'content': '', 'tool_calls': [
            {'id': 'hist_7', 'name': 'create_file', 'args': {'filename': 'a.md'}, 'type': 'tool_call'}]}
        assert msgs[1] == {'role': 'tool', 'content': 'ok', 'tool_call_id': 'hist_7'}

    def test_large_file_body_is_truncated(self, stubs):
        # The story body is the expensive part; the model only needs to see that a call happened
        msgs = stubs.build_tool_stub_messages([_step(1, inputs={'filedata': 'x' * 5000}, output='y' * 5000)])
        args = msgs[0]['tool_calls'][0]['args']
        assert len(args['filedata']) < 300 and args['filedata'].endswith(stubs.TRUNCATED)
        assert len(msgs[1]['content']) < 300

    def test_non_string_args_kept_small(self, stubs):
        msgs = stubs.build_tool_stub_messages([_step(1, inputs={'n': 3, 'flag': True, 'rows': list(range(500))})])
        args = msgs[0]['tool_calls'][0]['args']
        assert args['n'] == 3 and args['flag'] is True
        assert isinstance(args['rows'], str) and len(args['rows']) < 300

    def test_error_result_is_marked(self, stubs):
        # A failed save must not read as a success in history
        msgs = stubs.build_tool_stub_messages([_step(1, output='bucket missing', is_error=True)])
        assert msgs[1]['content'] == 'error: bucket missing'

    def test_empty_output_reads_ok(self, stubs):
        assert stubs.build_tool_stub_messages([_step(1, output='')])[1]['content'] == 'ok'

    def test_several_calls_share_one_assistant_message(self, stubs):
        msgs = stubs.build_tool_stub_messages([_step(1), _step(2, name='list_files')])
        assert [c['id'] for c in msgs[0]['tool_calls']] == ['hist_1', 'hist_2']
        assert [m['tool_call_id'] for m in msgs[1:]] == ['hist_1', 'hist_2']

    def test_calls_over_cap_are_counted_not_replayed(self, stubs):
        cap = stubs.MAX_CALLS_PER_TURN
        msgs = stubs.build_tool_stub_messages([_step(i) for i in range(cap + 4)])
        assert len(msgs[0]['tool_calls']) == cap
        assert len(msgs) == cap + 1
        assert '+4 more tool calls' in msgs[-1]['content']


class TestSelectStubGroupIds:

    def test_last_n_assistant_groups_only(self, stubs):
        groups = [types.SimpleNamespace(id=i, a=i % 2 == 0) for i in range(1, 11)]
        assert stubs.select_stub_group_ids(groups, lambda g: g.a, 3) == [6, 8, 10]

    def test_zero_disables(self, stubs):
        assert stubs.select_stub_group_ids([types.SimpleNamespace(id=1)], lambda g: True, 0) == []


class TestSummarizedGroupIds:
    """summarized_count counts LangChain messages; groups now emit one message per stub plus the text."""

    def test_plain_history_unchanged(self, stubs):
        assert stubs.summarized_group_ids_from_messages([1, 2, 3, 4], 2) == [1, 2]

    def test_stubbed_group_counts_once(self, stubs):
        # user 1, assistant 2 (tool_calls + result + text), user 3, assistant 4
        ids = [1, 2, 2, 2, 3, 4]
        assert stubs.summarized_group_ids_from_messages(ids, 4) == [1, 2]

    def test_old_slice_would_overshoot(self, stubs):
        # Regression guard: slicing group ids by message count marked unsummarized turns
        ids = [1, 2, 2, 2, 3, 4, 5, 6]
        groups = [1, 2, 3, 4, 5, 6]
        assert groups[:4] == [1, 2, 3, 4]
        assert stubs.summarized_group_ids_from_messages(ids, 4) == [1, 2]

    def test_group_split_by_cutoff_stays(self, stubs):
        # The cutoff fell between group 2's stub and its text: group 2 is still in history
        ids = [1, 2, 2, 2, 3]
        assert stubs.summarized_group_ids_from_messages(ids, 2) == [1]

    def test_everything_summarized(self, stubs):
        assert stubs.summarized_group_ids_from_messages([1, 2, 2], 3) == [1, 2]

    def test_zero_or_missing_count(self, stubs):
        assert stubs.summarized_group_ids_from_messages([1, 2], 0) == []
        assert stubs.summarized_group_ids_from_messages([1, 2], None) == []


class TestGenerateChatHistoryWithStubs:

    def _groups(self):
        return [Group(1, 'user', 'write a tale'), Group(2, 'application', 'Saved a.md'),
                Group(3, 'user', 'another'), Group(4, 'application', 'Here is the draft')]

    def test_no_stubs_is_unchanged(self, chat_history_module):
        ids = []
        out = chat_history_module.generate_chat_history(self._groups(), message_group_ids=ids)
        assert [m['role'] for m in out] == ['user', 'assistant', 'user', 'assistant']
        assert ids == [1, 2, 3, 4]

    def test_stubs_go_before_the_final_text(self, chat_history_module, stubs):
        pair = stubs.build_tool_stub_messages([_step(9, inputs={'filename': 'a.md'})])
        ids = []
        out = chat_history_module.generate_chat_history(self._groups(), tool_stubs={2: pair},
                                                        message_group_ids=ids)
        assert [m['role'] for m in out] == ['user', 'assistant', 'tool', 'assistant', 'user', 'assistant']
        assert out[1]['tool_calls'][0]['id'] == out[2]['tool_call_id'] == 'hist_9'
        assert out[3]['content'][0]['text'] == 'Saved a.md'
        assert ids == [1, 2, 2, 2, 3, 4]

    def test_summary_row_has_no_group_id(self, chat_history_module):
        # The summary row is not a group; ids must still align with emitted messages after it
        ids = []
        out = chat_history_module.generate_chat_history(
            self._groups(), summaries=[{'summary_content': 'earlier'}], message_group_ids=ids)
        assert len(out) == 5 and ids == [1, 2, 3, 4]

    def test_empty_group_emits_no_stubs(self, chat_history_module, stubs):
        # A text-less group is dropped; its stubs must not leak in as dangling tool calls
        groups = [Group(1, 'user', 'hi'), Group(2, 'application', None)]
        ids = []
        out = chat_history_module.generate_chat_history(
            groups, tool_stubs={2: stubs.build_tool_stub_messages([_step(1)])}, message_group_ids=ids)
        assert [m['role'] for m in out] == ['user'] and ids == [1]

    def test_routing_projection_stays_aligned(self, chat_history_module, stubs):
        # The indexer zips routing history with chat history and raises on any length/role mismatch
        projection = {}
        out = chat_history_module.generate_chat_history(
            self._groups(), tool_stubs={2: stubs.build_tool_stub_messages([_step(1)])},
            routing_projection=projection)
        assert [m['role'] for m in projection['history']] == [m['role'] for m in out]


class TestPayloadHasTools:
    """Regression: agent toolkits sit under version_details, so a top-level-only check never fired."""

    def test_agent_toolkits_in_version_details(self, stubs):
        assert stubs.payload_has_tools({'tools': [], 'version_details': {'tools': [{'id': 1}]}})

    def test_llm_chat_top_level_tools(self, stubs):
        assert stubs.payload_has_tools({'tools': [{'id': 1}]})

    def test_internal_tools_only(self, stubs):
        assert stubs.payload_has_tools({'internal_tools': ['swarm']})

    def test_no_tools_anywhere(self, stubs):
        assert not stubs.payload_has_tools({'tools': [], 'version_details': {'tools': []}, 'internal_tools': []})
