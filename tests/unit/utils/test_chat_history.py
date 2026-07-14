"""Unit tests for chat_history.py pure functions."""
import pytest
import pathlib
import sys

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module


@pytest.fixture(scope='module')
def chat_history_module():
    """Load chat_history module with minimal stubs."""
    import types

    # Stub ChatHistoryRole enum
    chat_history_role = types.SimpleNamespace(
        user=types.SimpleNamespace(value='user'),
        assistant=types.SimpleNamespace(value='assistant'),
    )

    # Stub ChatHistory pydantic model
    class FakeChatHistory:
        def __init__(self, role, content, additional_kwargs=None):
            self.role = role
            self.content = content
            self.additional_kwargs = additional_kwargs

        def dict(self, exclude_none=True):
            result = {'role': self.role, 'content': self.content}
            if self.additional_kwargs:
                result['additional_kwargs'] = self.additional_kwargs
            return result

    # Fake ORM classes
    class FakeConversationMessageGroup:
        pass

    class FakeAttachmentMessageItem:
        __mapper_args__ = {'polymorphic_identity': 'attachment'}

    class FakeContextMessageItem:
        __mapper_args__ = {'polymorphic_identity': 'context'}

    class FakeTextMessageItem:
        __mapper_args__ = {'polymorphic_identity': 'text'}

    class FakeCanvasMessageItem:
        __mapper_args__ = {'polymorphic_identity': 'canvas'}

    class FakeCanvasVersionItem:
        pass

    # Build stub modules
    enums_mod = types.ModuleType('enums')
    enums_mod.ChatHistoryRole = chat_history_role
    enums_mod.ParticipantTypes = types.SimpleNamespace(user='user')

    chat_mod = types.ModuleType('chat')
    chat_mod.ChatHistory = FakeChatHistory

    message_group_mod = types.ModuleType('message_group')
    message_group_mod.ConversationMessageGroup = FakeConversationMessageGroup

    attachment_mod = types.ModuleType('attachment')
    attachment_mod.AttachmentMessageItem = FakeAttachmentMessageItem

    context_mod = types.ModuleType('context')
    context_mod.ContextMessageItem = FakeContextMessageItem

    text_mod = types.ModuleType('text')
    text_mod.TextMessageItem = FakeTextMessageItem

    canvas_mod = types.ModuleType('canvas')
    canvas_mod.CanvasMessageItem = FakeCanvasMessageItem
    canvas_mod.CanvasVersionItem = FakeCanvasVersionItem

    extra_stubs = {
        'plugins.elitea_core.models.enums.all': enums_mod,
        'plugins.elitea_core.models.pd.chat': chat_mod,
        'plugins.elitea_core.models.message_group': message_group_mod,
        'plugins.elitea_core.models.message_items.attachment': attachment_mod,
        'plugins.elitea_core.models.message_items.context': context_mod,
        'plugins.elitea_core.models.message_items.text': text_mod,
        'plugins.elitea_core.models.message_items.canvas': canvas_mod,
    }

    return load_utils_module(
        TESTS_DIR.parent / 'utils',
        'chat_history',
        extra_stubs=extra_stubs,
    )


class TestFormatContextForLLM:
    """Tests for format_context_for_llm function."""

    def test_empty_context(self, chat_history_module):
        result = chat_history_module.format_context_for_llm({})
        assert result == ""

    def test_none_context(self, chat_history_module):
        result = chat_history_module.format_context_for_llm(None)
        assert result == ""

    def test_user_id_only(self, chat_history_module):
        result = chat_history_module.format_context_for_llm({'user_id': 42})
        assert '<user_id>42</user_id>' in result
        assert '<runtime_context>' in result
        assert '</runtime_context>' in result

    def test_full_context(self, chat_history_module):
        context = {
            'user_id': 1,
            'project_id': 2,
            'assistant_name': 'TestBot',
            'assistant_version': 'v1',
            'project_name': 'MyProject',
            'current_page': '/chat',
            'current_entity_id': 100,
            'current_entity_type': 'agent',
            'current_entity_name': 'CodeReviewer',
            'selected_provider': 'openai',
            'selected_model': 'gpt-4',
            'meta': {'key': 'value'},
        }
        result = chat_history_module.format_context_for_llm(context)

        assert '<user_id>1</user_id>' in result
        assert '<project_id>2</project_id>' in result
        assert '<assistant>TestBot</assistant>' in result
        assert '<assistant_version>v1</assistant_version>' in result
        assert '<project>MyProject</project>' in result
        assert '<current_page>/chat</current_page>' in result
        assert '<current_entity_id>100</current_entity_id>' in result
        assert "<working_on>agent 'CodeReviewer'</working_on>" in result
        assert '<provider>openai</provider>' in result
        assert '<model>gpt-4</model>' in result
        assert '<meta>{"key": "value"}</meta>' in result

    def test_entity_type_without_name(self, chat_history_module):
        result = chat_history_module.format_context_for_llm({
            'current_entity_type': 'pipeline'
        })
        assert '<working_on>pipeline</working_on>' in result


class TestExcludeImageBase64Content:
    """Tests for exclude_image_base64_content function."""

    def test_string_content_unchanged(self, chat_history_module):
        result = chat_history_module.exclude_image_base64_content("Hello world")
        assert result == "Hello world"

    def test_text_items_preserved(self, chat_history_module):
        content = [
            {'type': 'text', 'text': 'Hello'},
            {'type': 'text', 'text': 'World'},
        ]
        result = chat_history_module.exclude_image_base64_content(content)
        assert result == content

    def test_image_url_filtered(self, chat_history_module):
        content = [
            {'type': 'text', 'text': 'Hello'},
            {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,...'}},
            {'type': 'text', 'text': 'World'},
        ]
        result = chat_history_module.exclude_image_base64_content(content)
        assert len(result) == 2
        assert all(item['type'] == 'text' for item in result)

    def test_all_images_returns_placeholder(self, chat_history_module):
        content = [
            {'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,...'}},
        ]
        result = chat_history_module.exclude_image_base64_content(content)
        assert result == '[Image content removed - model does not support vision]'

    def test_non_list_non_string_unchanged(self, chat_history_module):
        result = chat_history_module.exclude_image_base64_content(123)
        assert result == 123


class TestExcludeImageBase64ContentFromChatHistory:
    """Tests for exclude_image_base64_content_from_chat_history function."""

    def test_filters_images_from_messages(self, chat_history_module):
        chat_history = [
            {
                'role': 'user',
                'content': [
                    {'type': 'text', 'text': 'What is this?'},
                    {'type': 'image_url', 'image_url': {'url': 'base64...'}},
                ],
            },
            {
                'role': 'assistant',
                'content': 'I see an image.',
            },
        ]
        result = chat_history_module.exclude_image_base64_content_from_chat_history(chat_history)

        assert len(result) == 2
        assert len(result[0]['content']) == 1
        assert result[0]['content'][0]['type'] == 'text'
        assert result[1]['content'] == 'I see an image.'

    def test_preserves_non_dict_messages(self, chat_history_module):
        chat_history = ['not a dict', {'role': 'user', 'content': 'text'}]
        result = chat_history_module.exclude_image_base64_content_from_chat_history(chat_history)
        assert result[0] == 'not a dict'

    def test_empty_history(self, chat_history_module):
        result = chat_history_module.exclude_image_base64_content_from_chat_history([])
        assert result == []


class TestGenerateChatHistoryFromSummaries:
    """Tests for generate_chat_history_from_summaries function."""

    def test_empty_summaries(self, chat_history_module):
        result = chat_history_module.generate_chat_history_from_summaries(None)
        assert result['role'] == 'user'
        assert result['content'][0]['text'] == ''

    def test_single_summary(self, chat_history_module):
        summaries = [{'summary_content': 'User asked about weather.'}]
        result = chat_history_module.generate_chat_history_from_summaries(summaries)

        assert result['role'] == 'user'
        assert 'Here is a summary of the conversation to date:' in result['content'][0]['text']
        assert 'User asked about weather.' in result['content'][0]['text']
        assert result['additional_kwargs'] == {'lc_source': 'summarization'}

    def test_multiple_summaries_joined(self, chat_history_module):
        summaries = [
            {'summary_content': 'First topic.'},
            {'summary_content': 'Second topic.'},
        ]
        result = chat_history_module.generate_chat_history_from_summaries(summaries)

        text = result['content'][0]['text']
        assert 'First topic.' in text
        assert 'Second topic.' in text
        assert '\n\n' in text  # joined with double newline

    def test_empty_summary_content_skipped(self, chat_history_module):
        summaries = [
            {'summary_content': '  '},  # whitespace only
            {'summary_content': 'Valid summary.'},
        ]
        result = chat_history_module.generate_chat_history_from_summaries(summaries)

        text = result['content'][0]['text']
        assert 'Valid summary.' in text
