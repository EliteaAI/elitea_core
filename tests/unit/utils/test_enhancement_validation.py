"""Unit tests for the grounding checks on an AI proposal (ENH-5, §5.1).

Each case here is an item that passes type validation and would still fail — or silently
mis-apply — at accept time. The module is loaded by path with a real ``mcp_versioning`` sibling,
because the dry-run agreeing with the writer is the property under test; stubbing the patch
function would test the stub.
"""
import importlib
import pathlib
import sys
import types

import pytest

UTILS = pathlib.Path(__file__).resolve().parents[3] / 'utils'


@pytest.fixture(scope='module')
def validation():
    pkg = types.ModuleType('enh_validation_pkg')
    pkg.__path__ = [str(UTILS)]
    sys.modules['enh_validation_pkg'] = pkg
    try:
        return importlib.import_module('enh_validation_pkg.enhancement_validation')
    finally:
        pass


class _AgentFix:
    def __init__(self, old_text=None, replacement='new', replace_all=False,
                 cited_dimension_ids=(), cited_case_ids=()):
        self.old_text = old_text
        self.replacement = replacement
        self.replace_all = replace_all
        self.cited_dimension_ids = list(cited_dimension_ids)
        self.cited_case_ids = list(cited_case_ids)


class _EvalFix:
    def __init__(self, kind, target_id=None, cited_dimension_ids=(), cited_case_ids=()):
        self.kind = kind
        self.target_id = target_id
        self.cited_dimension_ids = list(cited_dimension_ids)
        self.cited_case_ids = list(cited_case_ids)


SNAPSHOT = {
    'dimensions': {'11': {'name': 'Groundedness'}, '12': {'name': 'Politeness'}},
    'bindings': [{'dimension_id': 11}],
    'cases': [{'id': 100}, {'id': 101}],
}

INSTRUCTIONS = 'Answer support tickets politely. Never cite sources.'


@pytest.fixture
def known(validation):
    return validation.collect_known_ids(SNAPSHOT, [
        {'dimension_id': 11, 'dataset_case_id': 100},
        {'dimension_id': 13, 'dataset_case_id': 102},
    ])


# ---------------------------------------------------------------------------
# known ids
# ---------------------------------------------------------------------------

def test_known_ids_come_from_the_snapshot_and_the_result_rows(known):
    """A case that ranking happened not to sample is still a real case; dropping a citation of it
    would punish the model for the server's own cap."""
    assert known['dimension_ids'] == {11, 12, 13}
    assert known['case_ids'] == {100, 101, 102}


def test_known_ids_tolerate_an_empty_run(validation):
    empty = validation.collect_known_ids({}, [])
    assert empty == {'dimension_ids': set(), 'case_ids': set()}


def test_non_numeric_dimension_keys_are_ignored(validation):
    known = validation.collect_known_ids({'dimensions': {'not-an-id': {}}}, [])
    assert known['dimension_ids'] == set()


# ---------------------------------------------------------------------------
# agent fixes
# ---------------------------------------------------------------------------

def test_a_grounded_agent_fix_survives(validation, known):
    fix = _AgentFix(old_text='Never cite sources.', replacement='Always cite sources.',
                    cited_dimension_ids=[11], cited_case_ids=[100])

    kept, dropped = validation.validate_agent_fixes(
        [fix], instructions=INSTRUCTIONS, known=known)

    assert kept == [fix] and dropped == 0


def test_an_absent_anchor_is_dropped(validation, known):
    fix = _AgentFix(old_text='Text that was never in the instructions.', replacement='x')

    kept, dropped = validation.validate_agent_fixes(
        [fix], instructions=INSTRUCTIONS, known=known)

    assert kept == [] and dropped == 1


def test_an_ambiguous_anchor_is_dropped(validation, known):
    """Two matches with replace_all off is a 409 at apply time, so it must not be offered."""
    fix = _AgentFix(old_text='cite sources.', replacement='always cite sources.')

    kept, dropped = validation.validate_agent_fixes(
        [fix], instructions='Never cite sources. Really: never cite sources.', known=known)

    assert kept == [] and dropped == 1


def test_a_no_op_patch_is_dropped(validation, known):
    fix = _AgentFix(old_text='politely', replacement='politely')

    kept, dropped = validation.validate_agent_fixes(
        [fix], instructions=INSTRUCTIONS, known=known)

    assert kept == [] and dropped == 1


def test_a_wholesale_rewrite_to_empty_is_dropped(validation, known):
    fix = _AgentFix(replacement='   ', replace_all=True)

    kept, dropped = validation.validate_agent_fixes(
        [fix], instructions=INSTRUCTIONS, known=known)

    assert kept == [] and dropped == 1


def test_a_replace_all_rewrite_survives(validation, known):
    fix = _AgentFix(replacement='You are a concise support assistant.', replace_all=True)

    kept, dropped = validation.validate_agent_fixes(
        [fix], instructions=INSTRUCTIONS, known=known)

    assert kept == [fix] and dropped == 0


def test_an_invented_case_citation_drops_the_item(validation, known):
    fix = _AgentFix(old_text='Never cite sources.', replacement='Always cite sources.',
                    cited_case_ids=[999])

    kept, dropped = validation.validate_agent_fixes(
        [fix], instructions=INSTRUCTIONS, known=known)

    assert kept == [] and dropped == 1


def test_an_invented_dimension_citation_drops_the_item(validation, known):
    fix = _AgentFix(old_text='Never cite sources.', replacement='Always cite sources.',
                   cited_dimension_ids=[999])

    kept, dropped = validation.validate_agent_fixes(
        [fix], instructions=INSTRUCTIONS, known=known)

    assert kept == [] and dropped == 1


def test_an_uncited_item_is_not_dropped_for_that_alone(validation, known):
    """Citations are required by the prompt, not by grounding: an otherwise-applicable edge-case fix
    is still worth showing, and the review UI can flag the missing evidence itself."""
    fix = _AgentFix(old_text='Never cite sources.', replacement='Always cite sources.')

    kept, dropped = validation.validate_agent_fixes(
        [fix], instructions=INSTRUCTIONS, known=known)

    assert kept == [fix] and dropped == 0


def test_good_items_survive_alongside_bad_ones(validation, known):
    """The point of dropping rather than failing: one hallucinated item must not discard the rest."""
    good = _AgentFix(old_text='Never cite sources.', replacement='Always cite sources.')
    bad = _AgentFix(old_text='absent', replacement='x')

    kept, dropped = validation.validate_agent_fixes(
        [bad, good, bad], instructions=INSTRUCTIONS, known=known)

    assert kept == [good] and dropped == 2


def test_agent_fixes_are_validated_against_the_analysed_text_not_a_later_edit(validation, known):
    """The anchor is checked against the instructions the run was pinned to. Validating against
    text that has since changed would drop correct proposals and keep stale ones."""
    fix = _AgentFix(old_text='Never cite sources.', replacement='Always cite sources.')

    kept, _dropped = validation.validate_agent_fixes(
        [fix], instructions='Some completely different current draft.', known=known)

    assert kept == []


# ---------------------------------------------------------------------------
# eval fixes
# ---------------------------------------------------------------------------

def test_dimension_kinds_require_a_known_dimension(validation, known):
    good = _EvalFix('dimension_target', target_id=11)
    bad = _EvalFix('dimension_rubric', target_id=999)

    kept, dropped = validation.validate_eval_fixes([good, bad], known=known)

    assert kept == [good] and dropped == 1


def test_a_rubric_fix_aimed_at_a_case_id_is_dropped(validation, known):
    """Case 100 and dimension 100 are different things; applied blind, this would rewrite whichever
    dimension happened to share the number."""
    fix = _EvalFix('dimension_rubric', target_id=100)

    kept, dropped = validation.validate_eval_fixes([fix], known=known)

    assert kept == [] and dropped == 1


def test_case_kind_requires_a_known_case(validation, known):
    good = _EvalFix('dataset_case_expected', target_id=101)
    bad = _EvalFix('dataset_case_expected', target_id=11)

    kept, dropped = validation.validate_eval_fixes([good, bad], known=known)

    assert kept == [good] and dropped == 1


def test_a_coverage_gap_needs_no_target(validation, known):
    fix = _EvalFix('dataset_coverage_gap')

    kept, dropped = validation.validate_eval_fixes([fix], known=known)

    assert kept == [fix] and dropped == 0


def test_an_eval_fix_with_invented_citations_is_dropped(validation, known):
    fix = _EvalFix('dimension_target', target_id=11, cited_case_ids=[999])

    kept, dropped = validation.validate_eval_fixes([fix], known=known)

    assert kept == [] and dropped == 1


# ---------------------------------------------------------------------------
# whole proposal
# ---------------------------------------------------------------------------

class _Coverage:
    discarded_agent_fixes = 0
    discarded_eval_fixes = 0


class _Proposal:
    def __init__(self, agent_fixes, eval_fixes):
        self.agent_fixes = agent_fixes
        self.eval_fixes = eval_fixes
        self.coverage = _Coverage()


def test_grounding_reports_its_drops_on_the_coverage_block(validation):
    """A prompt regression that yields eight ungroundable items looks like a clean run unless the
    count reaches the response."""
    proposal = _Proposal(
        agent_fixes=[_AgentFix(old_text='absent', replacement='x'),
                     _AgentFix(old_text='politely', replacement='courteously')],
        eval_fixes=[_EvalFix('dimension_target', target_id=999)],
    )

    dropped = validation.ground_proposal(
        proposal, instructions=INSTRUCTIONS, snapshot=SNAPSHOT, results=[])

    assert dropped == {'discarded_agent_fixes': 1, 'discarded_eval_fixes': 1}
    assert proposal.coverage.discarded_agent_fixes == 1
    assert proposal.coverage.discarded_eval_fixes == 1
    assert len(proposal.agent_fixes) == 1 and proposal.eval_fixes == []


def test_dropping_everything_is_a_valid_outcome(validation):
    proposal = _Proposal(agent_fixes=[_AgentFix(old_text='absent', replacement='x')],
                         eval_fixes=[])

    dropped = validation.ground_proposal(
        proposal, instructions=INSTRUCTIONS, snapshot=SNAPSHOT, results=[])

    assert proposal.agent_fixes == []
    assert dropped['discarded_agent_fixes'] == 1


def test_a_clean_proposal_is_left_untouched(validation):
    fix = _AgentFix(old_text='Never cite sources.', replacement='Always cite sources.',
                    cited_dimension_ids=[11], cited_case_ids=[100])
    proposal = _Proposal(agent_fixes=[fix], eval_fixes=[_EvalFix('dimension_target', target_id=12)])

    dropped = validation.ground_proposal(
        proposal, instructions=INSTRUCTIONS, snapshot=SNAPSHOT, results=[])

    assert dropped == {'discarded_agent_fixes': 0, 'discarded_eval_fixes': 0}
    assert proposal.agent_fixes == [fix] and len(proposal.eval_fixes) == 1
