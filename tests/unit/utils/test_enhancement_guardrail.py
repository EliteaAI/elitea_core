"""Unit tests for enhancement_guardrail.py's admin-policy filter.

``apply_kind_guardrail`` runs after grounding and strips whatever fix kinds an admin has turned
off, reporting the counts on ``coverage.blocked_*`` — a separate signal from grounding's
``discarded_*`` so a deliberate admin choice is never mistaken for a quality regression. The module
has no imports, so it is loaded directly by path.
"""
import importlib.util
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]


@pytest.fixture(scope='module')
def guardrail():
    path = PLUGIN_ROOT / 'utils' / 'enhancement_guardrail.py'
    spec = importlib.util.spec_from_file_location('enhancement_guardrail_under_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _AgentFix:
    pass


class _EvalFix:
    def __init__(self, kind):
        self.kind = kind


class _Coverage:
    blocked_agent_fixes = 0
    blocked_eval_fixes = 0


class _Proposal:
    def __init__(self, agent_fixes, eval_fixes):
        self.agent_fixes = agent_fixes
        self.eval_fixes = eval_fixes
        self.coverage = _Coverage()


def test_everything_allowed_leaves_proposal_untouched(guardrail):
    proposal = _Proposal(
        agent_fixes=[_AgentFix()],
        eval_fixes=[_EvalFix('dimension_rubric'), _EvalFix('dataset_coverage_gap')],
    )

    result = guardrail.apply_kind_guardrail(
        proposal, agent_fixes_enabled=True,
        allowed_eval_fix_kinds={'dimension_rubric', 'dimension_target',
                                 'dataset_case_expected', 'dataset_coverage_gap'},
    )

    assert result == {'blocked_agent_fixes': 0, 'blocked_eval_fixes': 0}
    assert len(proposal.agent_fixes) == 1
    assert len(proposal.eval_fixes) == 2
    assert proposal.coverage.blocked_agent_fixes == 0
    assert proposal.coverage.blocked_eval_fixes == 0


def test_disabling_agent_fixes_empties_them_regardless_of_count(guardrail):
    proposal = _Proposal(agent_fixes=[_AgentFix(), _AgentFix(), _AgentFix()], eval_fixes=[])

    result = guardrail.apply_kind_guardrail(
        proposal, agent_fixes_enabled=False, allowed_eval_fix_kinds=set())

    assert proposal.agent_fixes == []
    assert result['blocked_agent_fixes'] == 3
    assert proposal.coverage.blocked_agent_fixes == 3


def test_enabled_with_no_agent_fixes_blocks_nothing(guardrail):
    """An admin turning agent fixes off should never manufacture a nonzero blocked count on a
    proposal that had none to begin with — that would misreport intent as impact."""
    proposal = _Proposal(agent_fixes=[], eval_fixes=[])

    result = guardrail.apply_kind_guardrail(
        proposal, agent_fixes_enabled=False, allowed_eval_fix_kinds=set())

    assert result['blocked_agent_fixes'] == 0


def test_disallowed_eval_kind_is_stripped_others_kept(guardrail):
    kept = _EvalFix('dimension_rubric')
    dropped = _EvalFix('dataset_coverage_gap')
    proposal = _Proposal(agent_fixes=[], eval_fixes=[kept, dropped])

    result = guardrail.apply_kind_guardrail(
        proposal, agent_fixes_enabled=True, allowed_eval_fix_kinds={'dimension_rubric'})

    assert proposal.eval_fixes == [kept]
    assert result == {'blocked_agent_fixes': 0, 'blocked_eval_fixes': 1}


def test_disabling_every_kind_empties_the_whole_proposal(guardrail):
    proposal = _Proposal(
        agent_fixes=[_AgentFix()],
        eval_fixes=[_EvalFix('dimension_rubric'), _EvalFix('dimension_target')],
    )

    result = guardrail.apply_kind_guardrail(
        proposal, agent_fixes_enabled=False, allowed_eval_fix_kinds=set())

    assert proposal.agent_fixes == []
    assert proposal.eval_fixes == []
    assert result == {'blocked_agent_fixes': 1, 'blocked_eval_fixes': 2}


def test_eval_fix_order_is_preserved_among_survivors(guardrail):
    first = _EvalFix('dimension_rubric')
    second = _EvalFix('dataset_coverage_gap')
    third = _EvalFix('dimension_target')
    proposal = _Proposal(agent_fixes=[], eval_fixes=[first, second, third])

    guardrail.apply_kind_guardrail(
        proposal, agent_fixes_enabled=True,
        allowed_eval_fix_kinds={'dimension_rubric', 'dimension_target'})

    assert proposal.eval_fixes == [first, third]


def test_return_value_matches_the_coverage_fields_it_wrote(guardrail):
    proposal = _Proposal(agent_fixes=[_AgentFix()], eval_fixes=[_EvalFix('dimension_rubric')])

    result = guardrail.apply_kind_guardrail(
        proposal, agent_fixes_enabled=False, allowed_eval_fix_kinds=set())

    assert result['blocked_agent_fixes'] == proposal.coverage.blocked_agent_fixes
    assert result['blocked_eval_fixes'] == proposal.coverage.blocked_eval_fixes
