"""#7383 AC-16, AC-20, AC-24, AC-25: field-level merge, revision check, diff and the confirmation policy."""
import pytest


AGENT = {
    'name': 'Daily Issue Summary Agent',
    'instructions': 'You summarise issues.\nAlways use a table.',
    'llm_settings': {'model_name': 'gpt-5-mini', 'temperature': 0.2},
    'tools': [{'id': 3}],
    'conversation_starters': ['Show today'],
}


class TestRevision:

    def test_is_stable_and_order_independent(self, builder):
        a = builder.changeset.revision({'b': 1, 'a': [1, 2]})
        b = builder.changeset.revision({'a': [1, 2], 'b': 1})
        assert a == b and len(a) == 64

    def test_changes_with_content(self, builder):
        assert builder.changeset.revision({'a': 1}) != builder.changeset.revision({'a': 2})


class TestApplyChanges:

    def test_untouched_fields_are_identical(self, builder):
        merged = builder.changeset.apply_changes(AGENT, {'llm_settings': {'temperature': 0.0}})
        assert merged['llm_settings'] == {'model_name': 'gpt-5-mini', 'temperature': 0.0}
        for key in ('name', 'instructions', 'tools', 'conversation_starters'):
            assert merged[key] == AGENT[key]
        assert AGENT['llm_settings']['temperature'] == 0.2  # input not mutated

    def test_text_patch_replaces_one_exact_occurrence(self, builder):
        merged = builder.changeset.apply_changes(AGENT, {'instructions': {
            'old_text': 'Always use a table.', 'new_text': 'Always use a table.\nAlways answer in French.',
        }})
        assert merged['instructions'] == 'You summarise issues.\nAlways use a table.\nAlways answer in French.'

    def test_text_patch_append(self, builder):
        merged = builder.changeset.apply_changes(AGENT, {'instructions': {'append': 'Answer in French.'}})
        assert merged['instructions'].endswith('\nAnswer in French.')

    @pytest.mark.parametrize('patch, reason', [
        ({'old_text': 'missing', 'new_text': 'x'}, 'not found'),
        ({'old_text': 'a', 'new_text': 'x'}, 'more than once'),
    ])
    def test_text_patch_errors(self, builder, patch, reason):
        with pytest.raises(builder.changeset.ChangeError, match=reason):
            builder.changeset.apply_changes({'instructions': 'a and a'}, {'instructions': patch})

    def test_lists_are_replaced_whole(self, builder):
        merged = builder.changeset.apply_changes(AGENT, {'conversation_starters': ['A', 'B']})
        assert merged['conversation_starters'] == ['A', 'B']

    def test_unknown_field_is_refused(self, builder):
        with pytest.raises(builder.changeset.ChangeError, match='unknown field'):
            builder.changeset.apply_changes(AGENT, {'colour': 'red'}, allowed_fields=set(AGENT))


class TestDiff:

    def test_lists_only_changed_paths(self, builder):
        after = builder.changeset.apply_changes(AGENT, {'llm_settings': {'temperature': 0.0}, 'name': 'X'})
        diff = builder.changeset.diff(AGENT, after)
        assert diff == [
            {'field': 'llm_settings.temperature', 'before': 0.2, 'after': 0.0},
            {'field': 'name', 'before': 'Daily Issue Summary Agent', 'after': 'X'},
        ]

    def test_long_text_is_summarised(self, builder):
        after = builder.changeset.apply_changes(AGENT, {'instructions': {'append': 'Answer in French.'}})
        (entry,) = builder.changeset.diff(AGENT, after)
        assert entry['field'] == 'instructions'
        assert entry['added_lines'] == ['Answer in French.']
        assert entry['removed_lines'] == []


class TestPolicy:

    @pytest.mark.parametrize('op, destructive, status, expected', [
        ('create', False, 'draft', False),
        ('update', False, 'draft', False),
        ('delete', True, 'draft', True),
        ('operation', True, None, True),
        ('operation', False, None, False),
    ])
    def test_only_destructive_needs_confirmation(self, builder, op, destructive, status, expected):
        assert builder.changeset.requires_confirmation(op, destructive) is expected

    @pytest.mark.parametrize('status', ['published', 'embedded'])
    def test_published_versions_are_not_updatable(self, builder, status):
        assert builder.changeset.is_locked_version(status) is True

    def test_draft_is_updatable(self, builder):
        assert builder.changeset.is_locked_version('draft') is False
