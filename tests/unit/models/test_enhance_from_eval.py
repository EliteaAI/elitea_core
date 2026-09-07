"""Unit tests for the "Enhance with AI" request/response contract (ENH-2, §4/§5.1).

The module is loaded under a stubbed sibling package because it imports ``.predict_llm``
relatively; only ``LLMSettingsRequest`` is needed, so a permissive model stands in for it.

These cases are the ones that decide what reaches a one-click "accept" button and from there a
call that rewrites agent instructions.
"""
import sys
import types

import pytest
from pydantic import BaseModel, ValidationError


@pytest.fixture(scope='module')
def pd_enh(models_path):
    def _pkg(name):
        mod = types.ModuleType(name)
        mod.__path__ = []
        return mod

    for name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.models',
                 'plugins.elitea_core.models.pd'):
        sys.modules.setdefault(name, _pkg(name))

    predict_llm = types.ModuleType('plugins.elitea_core.models.pd.predict_llm')

    class LLMSettingsRequest(BaseModel):
        model_config = {'extra': 'allow'}

    predict_llm.LLMSettingsRequest = LLMSettingsRequest
    sys.modules['plugins.elitea_core.models.pd.predict_llm'] = predict_llm

    from fixtures.helpers import load_module_with_stubs
    return load_module_with_stubs(
        models_path / 'pd' / 'enhance_from_eval.py',
        'plugins.elitea_core.models.pd.enhance_from_eval',
    )


# ---------------------------------------------------------------------------
# request
# ---------------------------------------------------------------------------

def test_request_needs_only_a_run_id(pd_enh):
    request = pd_enh.EnhanceFromEvalRequest(run_id=42)
    assert request.dimension_ids is None
    assert request.llm_settings is None


def test_request_rejects_missing_run_id(pd_enh):
    with pytest.raises(ValidationError):
        pd_enh.EnhanceFromEvalRequest()


# ---------------------------------------------------------------------------
# happy-path envelope (§4)
# ---------------------------------------------------------------------------

def test_response_accepts_the_documented_envelope(pd_enh):
    response = pd_enh.EnhanceFromEvalResponse(
        run_id=42,
        version_id=7,
        instructions_sha256='a' * 64,
        diagnosis='Citations are missing because the instructions never ask for them.',
        coverage={'total_cases': 10, 'gap_dimensions_total': 2, 'gap_dimensions_returned': 2},
        agent_fixes=[{
            'old_text': 'Answer the question.',
            'replacement': 'Answer the question and cite every source.',
            'rationale': 'Cases 10 and 11 lost points for uncited claims.',
            'cited_dimension_ids': [1],
            'cited_case_ids': [10, 11],
        }],
        eval_fixes=[{
            'kind': 'dimension_target',
            'target_id': 1,
            'target_name': 'Helpfulness',
            'current_value': '95',
            'proposed_value': '80',
            'rationale': 'A 95 target on a 0-100 judge scale is not reachable in practice.',
        }],
    )

    assert response.agent_fixes[0].replace_all is False
    assert response.eval_fixes[0].kind == pd_enh.EvalFixKind.dimension_target
    assert response.coverage.discarded_agent_fixes == 0


def test_response_defaults_to_an_empty_proposal(pd_enh):
    response = pd_enh.EnhanceFromEvalResponse()
    assert response.agent_fixes == [] and response.eval_fixes == []
    assert response.diagnosis == ''
    assert response.coverage.total_cases == 0


# ---------------------------------------------------------------------------
# AgentFixItem
# ---------------------------------------------------------------------------

def test_agent_fix_with_no_anchor_requires_replace_all(pd_enh):
    """``apply_instructions_patch`` refuses a null old_text unless replace_all is set, so an item
    like that must never reach the review UI — accepting it would fail with a conflict error that
    reads as a platform bug."""
    with pytest.raises(ValidationError, match='requires old_text'):
        pd_enh.AgentFixItem(replacement='Always cite sources.', rationale='r')


def test_agent_fix_may_rewrite_wholesale_with_replace_all(pd_enh):
    """replace_all discards the existing instructions entirely — old_text is then irrelevant."""
    fix = pd_enh.AgentFixItem(replacement='Whole new brief.', replace_all=True, rationale='r')
    assert fix.old_text is None and fix.replace_all is True


def test_agent_fix_rejects_empty_replacement_and_empty_anchor(pd_enh):
    with pytest.raises(ValidationError, match='must specify a replacement'):
        pd_enh.AgentFixItem(old_text='   ', replacement='  ', rationale='r')


def test_agent_fix_rejects_a_noop(pd_enh):
    """A replacement identical to its anchor would fail at apply time with a confusing
    conflict, after the user had already accepted it."""
    with pytest.raises(ValidationError, match='no-op'):
        pd_enh.AgentFixItem(old_text='same', replacement='same', rationale='r')


def test_agent_fix_deleting_text_is_allowed(pd_enh):
    """An empty replacement with a real anchor is a deletion — a legitimate edit."""
    fix = pd_enh.AgentFixItem(old_text='Never cite sources.', replacement='', rationale='r')
    assert fix.replacement == ''


def test_agent_fix_requires_a_rationale(pd_enh):
    with pytest.raises(ValidationError):
        pd_enh.AgentFixItem(replacement='x')


# ---------------------------------------------------------------------------
# EvalFixItem
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('kind', [
    'dimension_rubric', 'dimension_target', 'dataset_case_expected', 'dataset_coverage_gap',
])
def test_eval_fix_accepts_every_known_kind(pd_enh, kind):
    fix = pd_enh.EvalFixItem(kind=kind, target_id=1, proposed_value='v', rationale='r')
    assert fix.kind == kind


def test_eval_fix_rejects_unknown_kind(pd_enh):
    with pytest.raises(ValidationError, match='unknown eval fix kind'):
        pd_enh.EvalFixItem(kind='rewrite_toolkits', target_id=1, proposed_value='v', rationale='r')


@pytest.mark.parametrize('kind', [
    'dimension_rubric', 'dimension_target', 'dataset_case_expected',
])
def test_eval_fix_requires_target_id_for_edits(pd_enh, kind):
    """Every kind but a new case edits something that already exists; without an id there is
    nothing to apply the change to."""
    with pytest.raises(ValidationError, match='requires target_id'):
        pd_enh.EvalFixItem(kind=kind, proposed_value='v', rationale='r')


def test_eval_fix_coverage_gap_needs_no_target(pd_enh):
    fix = pd_enh.EvalFixItem(
        kind='dataset_coverage_gap',
        proposed_value='Ask a question with no answer in the corpus.',
        rationale='No case covers the refusal path.',
    )
    assert fix.target_id is None and fix.current_value is None


# ---------------------------------------------------------------------------
# caps
# ---------------------------------------------------------------------------

def _agent_fix(index):
    return {'old_text': f'anchor {index}', 'replacement': f'fix {index}', 'rationale': 'r'}


def _eval_fix(index):
    return {'kind': 'dimension_target', 'target_id': index, 'proposed_value': '1', 'rationale': 'r'}


def test_agent_fixes_are_capped(pd_enh):
    """An over-long list means the prompt or the model went off the rails; keeping the first 8
    silently would hide that."""
    at_cap = [_agent_fix(i) for i in range(pd_enh.MAX_AGENT_FIXES)]
    assert len(pd_enh.EnhanceFromEvalResponse(agent_fixes=at_cap).agent_fixes) == pd_enh.MAX_AGENT_FIXES

    with pytest.raises(ValidationError, match='too many agent fixes'):
        pd_enh.EnhanceFromEvalResponse(agent_fixes=at_cap + [_agent_fix(99)])


def test_eval_fixes_are_capped(pd_enh):
    at_cap = [_eval_fix(i) for i in range(pd_enh.MAX_EVAL_FIXES)]
    assert len(pd_enh.EnhanceFromEvalResponse(eval_fixes=at_cap).eval_fixes) == pd_enh.MAX_EVAL_FIXES

    with pytest.raises(ValidationError, match='too many eval fixes'):
        pd_enh.EnhanceFromEvalResponse(eval_fixes=at_cap + [_eval_fix(99)])
