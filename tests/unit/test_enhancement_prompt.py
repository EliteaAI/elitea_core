"""Unit tests for the "Enhance with AI" prompt assembly (ENH-3, §3.2/§3.3).

The module is dependency-free, so it is imported by path. What matters here is not wording but the
handful of properties that decide whether the model is asked a truthful question: verbatim rubrics,
a stated absence of ground truth, an admitted sample, and byte-stability.
"""
import importlib.util
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture(scope='module')
def prompt():
    path = PLUGIN_ROOT / 'utils' / 'enhancement_prompt.py'
    spec = importlib.util.spec_from_file_location('enhancement_prompt_under_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TEMPLATE = (
    'AGENT {application_name}\n'
    'INSTRUCTIONS\n{instructions}\n'
    'CONTEXT\n{agent_context}\n'
    'COVERAGE\n{coverage}\n'
    'GAPS\n{gaps}\n'
)


def _gap(**overrides):
    gap = {
        'dimension_id': 12,
        'name': 'Accuracy',
        'engine': 'ai',
        'rubric': 'Score 0-100. Deduct for any policy window not stated in the docs.',
        'scale': {'type': 'continuous', 'min': 0, 'max': 100, 'polarity': 'higher_better'},
        'target': 85,
        'target_operator': '>=',
        'weight': 2.0,
        'scored_count': 20,
        'missed_count': 12,
        'mean_shortfall': 0.3,
        'cases': [{
            'case_id': 2,
            'input': 'Refund for order #8842',
            'output': 'Refunds are only possible within 7 days.',
            'expected_output': '30-day refund window.',
            'native_score': 55,
            'shortfall': 0.3,
            'reasoning': 'cites wrong policy window',
        }],
    }
    gap.update(overrides)
    return gap


# ---------------------------------------------------------------------------
# instructions
# ---------------------------------------------------------------------------

def test_instructions_are_passed_through_untouched(prompt):
    text = 'Line one\n\n  indented\tspan'
    assert prompt.truncate_instructions(text) == text


def test_empty_instructions_are_stated_not_blank(prompt):
    assert prompt.truncate_instructions(None) == '(no instructions set)'
    assert prompt.truncate_instructions('   ') == '(no instructions set)'


def test_truncation_is_marked_in_the_text_the_model_reads(prompt):
    """An edit anchored in a span that was cut is a patch that cannot apply, so the model has to
    know the tail is missing."""
    out = prompt.truncate_instructions('x' * (prompt.MAX_INSTRUCTIONS_CHARS + 500))
    assert len(out) < prompt.MAX_INSTRUCTIONS_CHARS + 500
    assert out.endswith('[instructions truncated for analysis]')


# ---------------------------------------------------------------------------
# case + gap rendering
# ---------------------------------------------------------------------------

def test_case_renders_evidence_with_score_and_rationale(prompt):
    out = prompt.render_case(_gap()['cases'][0])
    assert 'case #2' in out
    assert 'Refunds are only possible within 7 days.' in out
    assert '30-day refund window.' in out
    assert 'cites wrong policy window' in out


def test_reference_free_case_says_so_explicitly(prompt):
    """Absent ground truth must be stated. Left as a missing line it is indistinguishable from a
    rendering failure, and the model invents an expected answer to diagnose against."""
    case = dict(_gap()['cases'][0], expected_output=None)
    out = prompt.render_case(case)
    assert 'reference-free' in out
    assert 'do not assume a ground-truth answer' in out


def test_rubric_is_quoted_verbatim(prompt):
    """An eval_fix critiques this exact text, so it must be the text the judge saw — not a
    paraphrase."""
    rubric = 'Score 0-100. Deduct for any policy window not stated in the docs.'
    assert rubric in prompt.render_gap(_gap())


def test_gap_header_names_the_dimension_id_the_model_must_cite(prompt):
    """Citations are required by id and grounding drops an item citing an id that is not in the run.
    A brief showing only the dimension name therefore asks the model to guess, and it guesses 0."""
    assert 'dimension #12' in prompt.render_gap(_gap())


def test_gap_header_carries_target_operator_and_miss_statistics(prompt):
    out = prompt.render_gap(_gap())
    assert 'target >= 85' in out
    assert 'missed 12/20' in out
    assert 'weight 2' in out


def test_missing_rubric_is_reported_not_omitted(prompt):
    out = prompt.render_gap(_gap(rubric=None))
    assert 'none recorded in the run snapshot' in out


def test_code_engine_gap_renders_the_traceback_as_the_judge_line(prompt):
    gap = _gap(engine='code', cases=[{
        'case_id': 2, 'input': 'q', 'output': 'a', 'expected_output': None,
        'native_score': 0, 'shortfall': 1.0, 'reasoning': "KeyError: 'refund_id'",
    }])
    assert "KeyError: 'refund_id'" in prompt.render_gap(gap)


def test_no_gaps_is_stated_rather_than_left_empty(prompt):
    assert prompt.render_gaps([]) == 'No dimension missed its target in this run.'


# ---------------------------------------------------------------------------
# coverage
# ---------------------------------------------------------------------------

def test_coverage_tells_the_model_the_brief_is_a_sample(prompt):
    """An AI that believes it saw every failure writes an absolute diagnosis about a sample."""
    out = prompt.render_coverage({
        'total_cases': 20, 'targeted_bindings': 7, 'missed_bindings': 7,
        'gap_dimensions_total': 7, 'gap_dimensions_returned': 5, 'max_cases_per_dimension': 3,
    })
    assert 'Showing the 5 highest-impact of 7' in out
    assert 'do not describe your diagnosis as exhaustive' in out
    assert 'At most 3 worst cases' in out


def test_coverage_omits_the_sample_warning_when_nothing_was_dropped(prompt):
    out = prompt.render_coverage({
        'total_cases': 5, 'gap_dimensions_total': 2, 'gap_dimensions_returned': 2,
    })
    assert 'highest-impact' not in out


def test_coverage_explains_that_errored_results_are_not_evidence(prompt):
    out = prompt.render_coverage({'excluded_error_results': 3, 'excluded_pending_human': 2})
    assert '3 result(s) errored' in out
    assert 'not evidence of anything' in out
    assert '2 result(s) await human scoring' in out


# ---------------------------------------------------------------------------
# agent context
# ---------------------------------------------------------------------------

def test_agent_context_is_marked_read_only(prompt):
    out = prompt.render_agent_context({'model_name': 'gpt-5', 'toolkit_names': ['orders']})
    assert 'gpt-5' in out and 'orders' in out
    assert 'cannot propose changes to the model or toolkits' in out


def test_agent_context_handles_a_bare_agent(prompt):
    out = prompt.render_agent_context(None)
    assert '(unknown)' in out and '(none)' in out


# ---------------------------------------------------------------------------
# build_enhance_system_prompt
# ---------------------------------------------------------------------------

def test_build_fills_every_placeholder(prompt):
    out = prompt.build_enhance_system_prompt(
        TEMPLATE, 'Support Bot', 'Help the customer.', [_gap()],
        coverage={'total_cases': 20}, agent_context={'model_name': 'gpt-5'},
    )
    assert '{' not in out.replace('{{', '')
    assert 'Support Bot' in out
    assert 'Help the customer.' in out
    assert 'Accuracy' in out


def test_build_is_byte_stable_for_the_same_input(prompt):
    """Identical input must produce an identical prompt, or a prompt regression is
    indistinguishable from ordinary model variance."""
    args = (TEMPLATE, 'Support Bot', 'Help the customer.', [_gap(), _gap(name='Tone')])
    assert prompt.build_enhance_system_prompt(*args) == prompt.build_enhance_system_prompt(*args)


def test_build_raises_on_a_malformed_template(prompt):
    """A template with an unknown placeholder must fail loudly rather than ship a prompt with a
    literal {brace} in it."""
    with pytest.raises(prompt.EnhancePromptTemplateError):
        prompt.build_enhance_system_prompt('{application_name} {unknown_field}', 'a', 'b', [])


def test_build_handles_an_unnamed_agent_and_no_gaps(prompt):
    out = prompt.build_enhance_system_prompt(TEMPLATE, '', None, [])
    assert '(unnamed agent)' in out
    assert 'No dimension missed its target' in out
