"""Unit tests for per-persona instruction resolution (#5392).

conversation_utils.resolve_persona_instructions selects the instructions that apply to the
currently-selected persona, with a legacy fallback for unmigrated rows. The function is pure;
conversation_utils imports ORM models at import time, so the logic is mirrored here (per the
plugin test guide for modules with relative imports) and kept byte-identical to the source.

Run from tests/ directory:
    pytest unit/utils/test_persona_instructions.py -v
"""


def resolve_persona_instructions(user_personalization: dict, persona: str) -> str:
    # Mirror of utils/conversation_utils.py::resolve_persona_instructions — keep in sync.
    if not user_personalization:
        return ''
    instructions_map = user_personalization.get('personality_instructions')
    if isinstance(instructions_map, dict):
        return instructions_map.get(persona) or '' if persona else ''
    return user_personalization.get('default_instructions') or ''


def test_dict_entry_present_returns_that_persona():
    p = {'persona': 'qa', 'personality_instructions': {'qa': 'be precise', 'generic': 'balanced'}}
    assert resolve_persona_instructions(p, 'qa') == 'be precise'


def test_empty_entry_is_no_override_not_fallback_to_generic():
    # The actual bug fix: qa is empty -> '' (no override), must NOT leak generic's text.
    p = {'persona': 'qa', 'personality_instructions': {'qa': '', 'generic': 'balanced'}}
    assert resolve_persona_instructions(p, 'qa') == ''


def test_missing_entry_is_no_override():
    p = {'persona': 'nerdy', 'personality_instructions': {'generic': 'balanced'}}
    assert resolve_persona_instructions(p, 'nerdy') == ''


def test_legacy_row_without_dict_falls_back_to_flat_instructions():
    # Pre-migration row: no personality_instructions key -> use flat default_instructions.
    p = {'persona': 'qa', 'default_instructions': 'legacy shared text'}
    assert resolve_persona_instructions(p, 'qa') == 'legacy shared text'


def test_malformed_dict_treated_as_absent_and_falls_back():
    p = {'persona': 'qa', 'personality_instructions': 'not-a-dict', 'default_instructions': 'legacy'}
    assert resolve_persona_instructions(p, 'qa') == 'legacy'


def test_empty_personalization_returns_empty():
    assert resolve_persona_instructions({}, 'qa') == ''
    assert resolve_persona_instructions(None, 'qa') == ''


def test_no_persona_selected_returns_empty_when_dict_present():
    p = {'personality_instructions': {'generic': 'balanced'}}
    assert resolve_persona_instructions(p, None) == ''


def test_none_and_bare_personas_use_own_empty_slot():
    p = {'persona': 'bare', 'personality_instructions': {'bare': '', 'generic': 'balanced'}}
    assert resolve_persona_instructions(p, 'bare') == ''
    assert resolve_persona_instructions(p, 'none') == ''
