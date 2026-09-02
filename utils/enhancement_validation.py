"""Grounding checks on the AI's proposal (ENH-5, §5.1).

Type validation stops malformed items; it does not stop invented ones. An ``agent_fix`` whose
anchor does not appear in the instructions, or one citing a case from some other run, is worse than
no proposal: it costs review time and, once accepted, fails at 409 in front of a user who has
already decided it was correct.

So every item is checked against the run it claims to come from and dropped if it cannot be
grounded. Two rules make that trustworthy:

* **The dry-run calls the same function the apply path calls.** A private re-implementation of
  "does this anchor match exactly once" is a validator that can disagree with the writer, which is
  the one failure this whole module exists to prevent.
* **Drops are counted, never silent.** A prompt regression that yields eight ungroundable items
  looks identical to a clean run unless the count reaches the response.

Pure functions over plain data, so the rules are unit-testable without a run or an LLM.
"""

from typing import Iterable

from .mcp_versioning import InstructionsPatchConflictError, apply_instructions_patch, instructions_sha256

# Kind vocabulary mirrors models.pd.enhance_from_eval, inlined so this module stays importable
# without pydantic (same convention as evaluation_scoring / enhancement_gap_selection).
KIND_DIMENSION_RUBRIC = 'dimension_rubric'
KIND_DIMENSION_TARGET = 'dimension_target'
KIND_DATASET_CASE_EXPECTED = 'dataset_case_expected'
KIND_DATASET_COVERAGE_GAP = 'dataset_coverage_gap'

_DIMENSION_TARGET_KINDS = {KIND_DIMENSION_RUBRIC, KIND_DIMENSION_TARGET}
_CASE_TARGET_KINDS = {KIND_DATASET_CASE_EXPECTED}


def collect_known_ids(snapshot: dict, results: Iterable[dict] = ()) -> dict:
    """Dimension and case ids reachable from this run.

    Read from the snapshot *and* the result rows rather than from the gap payload the model was
    shown: a citation of a real case that ranking happened not to sample is accurate, and dropping
    it would punish the model for the server's own cap.
    """
    snapshot = snapshot or {}
    dimension_ids = set()
    case_ids = set()

    for key in (snapshot.get('dimensions') or {}):
        try:
            dimension_ids.add(int(key))
        except (TypeError, ValueError):
            continue
    for binding in (snapshot.get('bindings') or []):
        if binding.get('dimension_id') is not None:
            dimension_ids.add(binding['dimension_id'])
    for case in (snapshot.get('cases') or []):
        if case.get('id') is not None:
            case_ids.add(case['id'])
    for row in results or ():
        if row.get('dimension_id') is not None:
            dimension_ids.add(row['dimension_id'])
        if row.get('dataset_case_id') is not None:
            case_ids.add(row['dataset_case_id'])

    return {'dimension_ids': dimension_ids, 'case_ids': case_ids}


def _citations_are_known(item, known: dict) -> bool:
    cited_dimensions = set(getattr(item, 'cited_dimension_ids', None) or ())
    cited_cases = set(getattr(item, 'cited_case_ids', None) or ())
    return (
        cited_dimensions <= known['dimension_ids']
        and cited_cases <= known['case_ids']
    )


def _patch_applies(item, instructions: str) -> bool:
    """Would the apply path accept this edit against the instructions that were analysed?"""
    try:
        apply_instructions_patch(
            instructions,
            expected_sha256=instructions_sha256(instructions),
            old_text=getattr(item, 'old_text', None),
            replacement=getattr(item, 'replacement', ''),
            replace_all=getattr(item, 'replace_all', False),
        )
    except InstructionsPatchConflictError:
        return False
    return True


def validate_agent_fixes(fixes, *, instructions: str, known: dict) -> tuple:
    """Return ``(kept, dropped_count)``.

    Each item must cite only ids from this run and survive a dry run against the frozen
    instructions. Both checks are cheap; the alternative is discovering the problem at apply time.
    """
    kept = []
    dropped = 0
    for item in fixes or []:
        if not _citations_are_known(item, known):
            dropped += 1
            continue
        if not _patch_applies(item, instructions or ''):
            dropped += 1
            continue
        kept.append(item)
    return kept, dropped


def validate_eval_fixes(fixes, *, known: dict) -> tuple:
    """Return ``(kept, dropped_count)``.

    ``target_id`` has to resolve in the namespace its ``kind`` implies — a rubric edit aimed at a
    case id would be applied to whichever dimension happens to share that number.
    """
    kept = []
    dropped = 0
    for item in fixes or []:
        if not _citations_are_known(item, known):
            dropped += 1
            continue

        kind = getattr(item, 'kind', None)
        target_id = getattr(item, 'target_id', None)
        if kind in _DIMENSION_TARGET_KINDS and target_id not in known['dimension_ids']:
            dropped += 1
            continue
        if kind in _CASE_TARGET_KINDS and target_id not in known['case_ids']:
            dropped += 1
            continue
        kept.append(item)
    return kept, dropped


def ground_proposal(proposal, *, instructions: str, snapshot: dict, results=()) -> dict:
    """Filter a validated proposal in place and report what was removed.

    Returns ``{'discarded_agent_fixes': int, 'discarded_eval_fixes': int}``. Dropping every item is
    a valid outcome, not an error: an empty proposal with a nonzero count is exactly how a prompt
    regression should surface.
    """
    known = collect_known_ids(snapshot, results)

    proposal.agent_fixes, discarded_agent = validate_agent_fixes(
        proposal.agent_fixes, instructions=instructions, known=known,
    )
    proposal.eval_fixes, discarded_eval = validate_eval_fixes(proposal.eval_fixes, known=known)

    proposal.coverage.discarded_agent_fixes = discarded_agent
    proposal.coverage.discarded_eval_fixes = discarded_eval
    return {
        'discarded_agent_fixes': discarded_agent,
        'discarded_eval_fixes': discarded_eval,
    }
