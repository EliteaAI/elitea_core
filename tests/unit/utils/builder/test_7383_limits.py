"""#7383 AC-19: a write that breaks a platform limit is refused with the limit, its number and a valid suggestion."""


def errors_by_field(errors):
    return {e['field']: e for e in errors}


class TestSkillName:

    def test_valid_name_passes(self, builder):
        assert builder.limits.check('skill', {'name': 'incident-triage', 'description': 'x'}) == []

    def test_invalid_characters_get_a_slug_suggestion(self, builder):
        err = errors_by_field(builder.limits.check('skill', {'name': 'My Skill!'}))['name']
        assert err['limit'] == 64
        assert err['suggestion'] == 'my-skill'
        assert 'lowercase letters, digits and single hyphens' in err['message']

    def test_too_long_is_cut_to_64_on_a_word_boundary(self, builder):
        name = 'a-' * 40 + 'b'
        err = errors_by_field(builder.limits.check('skill', {'name': name}))['name']
        assert len(err['suggestion']) <= 64
        assert not err['suggestion'].endswith('-')
        assert f'{len(name)} characters' in err['message']

    def test_reserved_word(self, builder):
        err = errors_by_field(builder.limits.check('skill', {'name': 'claude-helper'}))['name']
        assert err['suggestion'] == 'helper'

    def test_description_over_limit(self, builder):
        err = errors_by_field(builder.limits.check('skill', {'description': 'x' * 2305}))['description']
        assert err['limit'] == 2304
        assert '1 character over' in err['message']


class TestProjectContext:

    def test_content_over_limit_says_how_many(self, builder):
        err = errors_by_field(builder.limits.check('project_context', {'content': 'x' * 2600}))['content']
        assert err['limit'] == 2500
        assert '100 characters over' in err['message']

    def test_activation_description_over_limit(self, builder):
        errs = errors_by_field(builder.limits.check('project_context', {'activation_description': 'y' * 301}))
        assert errs['activation_description']['limit'] == 300

    def test_within_limits(self, builder):
        assert builder.limits.check('project_context', {'content': 'x' * 2500, 'activation_description': 'y'}) == []


class TestAgentStarters:

    def test_five_starters_over_default_four(self, builder):
        err = errors_by_field(
            builder.limits.check('agent', {'conversation_starters': ['a', 'b', 'c', 'd', 'e']})
        )['conversation_starters']
        assert err['limit'] == 4
        assert 'at most 4' in err['message']

    def test_blank_starters_do_not_count(self, builder):
        assert builder.limits.check('agent', {'conversation_starters': ['a', ' ', 'b', '', 'c', 'd']}) == []

    def test_configured_limit_is_used(self, builder):
        err = builder.limits.check('pipeline', {'conversation_starters': ['a', 'b', 'c']}, max_starters=2)
        assert err[0]['limit'] == 2

    def test_starter_over_768_rejected(self, builder):
        err = errors_by_field(
            builder.limits.check('agent', {'conversation_starters': ['a' * 768, 'b' * 770]})
        )['conversation_starters']
        assert err['limit'] == 768
        assert '2 characters over the 768-character limit' in err['message']

    def test_unchanged_saved_over_length_starter_allowed(self, builder):
        assert builder.limits.check('agent', {'conversation_starters': ['a' * 900, 'new']},
                                    previous_starters=['a' * 900]) == []

    def test_edited_over_length_starter_rejected(self, builder):
        errs = builder.limits.check('agent', {'conversation_starters': ['a' * 901]},
                                    previous_starters=['a' * 900])
        assert errors_by_field(errs)['conversation_starters']['limit'] == 768

    def test_name_required_non_empty(self, builder):
        assert errors_by_field(builder.limits.check('agent', {'name': '  '}))['name']['limit'] == 1
