"""Request/response contract for "Enhance with AI" (ENH-2, §4/§5.1).

The LLM returns free-form JSON. This module is the boundary that decides what is allowed through
to a UI where the user can accept it with one click, and from there to a call that rewrites agent
instructions. Type validation is not enough on its own — ENH-5 adds semantic checks (citations
exist, patches actually apply) — but everything here is a shape the apply path cannot survive
without.

The central idea is the **triage** in §1.1: a missed target is either the agent's fault or the
*measurement's* fault. So the model returns two item families, and the AI has to attribute a gap
before proposing anything. A response that could only ever contain instruction edits would happily
rewrite a working agent to satisfy a broken rubric.
"""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .predict_llm import LLMSettingsRequest

# Caps are validation, not truncation: an over-long list means the prompt or the model went off
# the rails, and silently keeping the first 8 items would hide that.
MAX_AGENT_FIXES = 8
MAX_EVAL_FIXES = 8


class EvalFixKind:
    """What an eval-side proposal changes. Each maps to an existing eval CRUD endpoint (§6.2) —
    the enhancement feature persists nothing of its own."""

    dimension_rubric = 'dimension_rubric'
    dimension_target = 'dimension_target'
    dataset_case_expected = 'dataset_case_expected'
    dataset_coverage_gap = 'dataset_coverage_gap'


_EVAL_FIX_KINDS = {
    EvalFixKind.dimension_rubric,
    EvalFixKind.dimension_target,
    EvalFixKind.dataset_case_expected,
    EvalFixKind.dataset_coverage_gap,
}


class EnhanceFromEvalRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {"run_id": 42, "llm_settings": {"model_name": "gpt-5-mini", "temperature": 0.3}},
            ]
        }
    )

    run_id: int = Field(description="Finished evaluation run to derive gaps from.")
    dimension_ids: Optional[List[int]] = Field(
        default=None,
        description="Restrict the analysis to these dimensions. Defaults to the highest-impact "
        "gaps chosen by the server.",
    )
    llm_settings: Optional[LLMSettingsRequest] = Field(
        default=None,
        description="LLM model override. If not provided, uses the project's default model.",
    )


class AgentFixItem(BaseModel):
    """A proposed edit to the agent's instructions.

    Field names are deliberately the exact contract ``apply_instructions_patch()`` already
    enforces (``old_text`` / ``replacement`` / ``replace_all``) so an accepted item is forwarded
    unchanged. Reshaping between the review UI and the apply call is where a patch that was shown
    as safe turns into one that is not.
    """

    old_text: Optional[str] = Field(
        default=None,
        description="Exact existing text to replace. Required unless replace_all is true, and "
        "must occur exactly once in the analysed instructions or the patch is rejected.",
    )
    replacement: str = Field(description="Text to put in place of old_text.")
    replace_all: bool = Field(
        default=False,
        description="Replace the *entire* instructions with replacement, ignoring old_text. This "
        "is the wholesale-rewrite escape hatch, not an all-occurrences flag.",
    )
    rationale: str = Field(description="Why this edit addresses the cited gap.")
    cited_dimension_ids: List[int] = Field(
        default_factory=list,
        description="Dimensions whose missed targets motivated this edit.",
    )
    cited_case_ids: List[int] = Field(
        default_factory=list,
        description="Dataset cases whose failures motivated this edit.",
    )

    @model_validator(mode='after')
    def _reject_patches_the_apply_path_would_refuse(self):
        """Reject here what ``apply_instructions_patch`` would reject later.

        Every rule below is one that function already enforces (``mcp_versioning.py:19``). Letting
        such an item reach the review UI means the user accepts a patch that then fails with a
        conflict error, which reads as a platform bug rather than a bad proposal.
        """
        if not self.replacement.strip() and not (self.old_text or '').strip():
            raise ValueError('agent fix must specify a replacement or the text it replaces')
        if not self.replace_all and not (self.old_text or ''):
            raise ValueError('agent fix requires old_text unless replace_all is true')
        if self.old_text is not None and self.old_text == self.replacement:
            raise ValueError('agent fix is a no-op: old_text equals replacement')
        return self


class EvalFixItem(BaseModel):
    """A proposed change to the *evaluation* rather than the agent — the other half of the triage.

    ``current_value`` is carried so the review UI can show current-vs-proposed without a second
    round trip, and so a stale proposal is visible: if the current value on screen no longer
    matches what the AI reasoned about, the user can see that before accepting.
    """

    kind: str = Field(description=f"One of: {', '.join(sorted(_EVAL_FIX_KINDS))}.")
    target_id: Optional[int] = Field(
        default=None,
        description="Dimension or dataset case the fix applies to. Null only for "
        "dataset_coverage_gap, which proposes a new case.",
    )
    target_name: Optional[str] = Field(
        default=None, description="Human-readable name of the target, for display.",
    )
    current_value: Optional[str] = Field(
        default=None, description="Value as it stood in the run snapshot. Null for a new case.",
    )
    proposed_value: str = Field(description="Value the AI proposes instead.")
    rationale: str = Field(description="Why the measurement, not the agent, is the problem here.")
    cited_dimension_ids: List[int] = Field(default_factory=list)
    cited_case_ids: List[int] = Field(default_factory=list)

    @field_validator('kind')
    @classmethod
    def _known_kind(cls, value: str) -> str:
        if value not in _EVAL_FIX_KINDS:
            raise ValueError(f'unknown eval fix kind: "{value}"')
        return value

    @model_validator(mode='after')
    def _require_target_unless_new_case(self):
        """Every kind except ``dataset_coverage_gap`` edits something that already exists, and
        without an id there is nothing to apply the change to."""
        if self.kind != EvalFixKind.dataset_coverage_gap and self.target_id is None:
            raise ValueError(f'{self.kind} requires target_id')
        return self


class InstructionsPatchItem(BaseModel):
    """One accepted :class:`AgentFixItem`, stripped to what the apply path needs.

    The rationale and citations are review material and are deliberately not carried here: the
    server re-derives nothing from them, and accepting them would mean an apply request could claim
    a justification the analysis never produced.
    """

    old_text: Optional[str] = None
    replacement: str
    replace_all: bool = False

    @model_validator(mode='after')
    def _require_old_text_for_exact_replace(self):
        if not self.replace_all and not self.old_text:
            raise ValueError('old_text is required unless replace_all is true')
        return self


class InstructionsForkRequest(BaseModel):
    """Apply a batch of accepted instruction edits to a *new* version (ENH-6b, §6.1.1).

    One hash for the whole batch, not one per item: every accepted edit was proposed against the
    same starting text, and per-item hashes would let reads from different versions be mixed into a
    single apply.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "expected_instructions_sha256": "0" * 64,
                    "patches": [
                        {"old_text": "Answer the question.",
                         "replacement": "Answer the question and cite every source."},
                    ],
                },
            ]
        }
    )

    expected_instructions_sha256: str = Field(
        min_length=64,
        max_length=64,
        pattern=r'^[0-9a-fA-F]{64}$',
        description="Hash returned by the enhancement analysis. A mismatch rejects the whole batch.",
    )
    patches: List[InstructionsPatchItem] = Field(
        min_length=1,
        max_length=MAX_AGENT_FIXES,
        description="Applied in order, all or nothing.",
    )
    new_version_name: Optional[str] = Field(
        default=None,
        max_length=128,
        description="Name for the fork. Defaults to a generated 'enhanced-<version_id>-<timestamp>'.",
    )


class EnhanceCoverage(BaseModel):
    """What the analysis was actually based on.

    Present so the dialog can say the proposal came from a sample. A capped analysis presented as
    a complete one is the failure mode this exists to prevent — the user would read "no issues
    found in dimension X" when dimension X was never sent to the model.
    """

    total_cases: int = 0
    total_bindings: int = 0
    targeted_bindings: int = 0
    missed_bindings: int = 0
    gap_dimensions_total: int = 0
    gap_dimensions_returned: int = 0
    missed_cases_total: int = 0
    missed_cases_returned: int = 0
    excluded_error_results: int = 0
    excluded_pending_human: int = 0
    max_gap_dimensions: Optional[int] = None
    max_cases_per_dimension: Optional[int] = None
    # Filled by ENH-5. Reported rather than silently applied: a nonzero count here is how a prompt
    # regression that produces unusable items becomes visible instead of looking like a quiet run.
    discarded_agent_fixes: int = 0
    discarded_eval_fixes: int = 0


class EnhanceFromEvalResponse(BaseModel):
    """The proposal. Nothing here is persisted — the client saves accepted items via the existing
    version-patch/fork and eval CRUD endpoints."""

    run_id: Optional[int] = Field(
        default=None, description="Run the gaps were derived from. Set by the server.",
    )
    version_id: Optional[int] = Field(
        default=None,
        description="Application version the analysed instructions came from — the version the "
        "run was pinned to, which may not be the agent's current default. Set by the server.",
    )
    instructions_sha256: Optional[str] = Field(
        default=None,
        description="Hash of the instructions the proposal was derived from. Sent back on apply "
        "so a stale patch fails cleanly instead of corrupting edited text.",
    )
    diagnosis: str = Field(
        default='',
        description="Narrative attribution of the gaps: what the agent got wrong versus what the "
        "evaluation measured wrongly.",
    )
    coverage: EnhanceCoverage = Field(default_factory=EnhanceCoverage)
    agent_fixes: List[AgentFixItem] = Field(default_factory=list)
    eval_fixes: List[EvalFixItem] = Field(default_factory=list)

    @model_validator(mode='after')
    def _enforce_item_caps(self):
        if len(self.agent_fixes) > MAX_AGENT_FIXES:
            raise ValueError(f'too many agent fixes: {len(self.agent_fixes)} > {MAX_AGENT_FIXES}')
        if len(self.eval_fixes) > MAX_EVAL_FIXES:
            raise ValueError(f'too many eval fixes: {len(self.eval_fixes)} > {MAX_EVAL_FIXES}')
        return self
