"""Prompt assembly for "Enhance with AI" (ENH-3, §3.2/§3.3).

Renders the ranked gaps from ``enhancement_gap_selection`` into the brief the analyst LLM reads,
then fills the ``enhance_agent_from_eval`` service prompt template with it.

Dependency-free (no DB, no ORM, no Pylon) so the rendered brief is unit-testable and byte-stable
for a given run — the same reason ``enhancement_gap_selection`` is. A prompt that varies between
identical runs makes a prompt regression indistinguishable from ordinary model variance.

Two things the brief must get right, because both cause wrong proposals rather than bad ones:

* **Rubrics and instructions are reproduced verbatim** from the run snapshot and the run's pinned
  version. Summarising either means the AI critiques text that was never under test.
* **The absence of an expected output is stated, not omitted.** A reference-free case looks
  identical to one whose expected output we simply failed to render, and told nothing the model
  will invent a ground truth and diagnose against it.
"""

from typing import List, Optional

# Long instruction bodies are the usual reason the prompt blows the model's context. The gap
# payload is already capped by enhancement_gap_selection; this is the one unbounded field left.
MAX_INSTRUCTIONS_CHARS = 20000
_TRUNCATED_MARK = '\n... [instructions truncated for analysis]'

_NO_INSTRUCTIONS = '(no instructions set)'
_NO_GAPS = 'No dimension missed its target in this run.'


class EnhancePromptTemplateError(Exception):
    """The service prompt template does not accept the placeholders we fill.

    Separate from a generic error so the endpoint can report a misconfigured prompt rather than
    blaming the model for a response it was never asked for.
    """


def truncate_instructions(instructions: Optional[str]) -> str:
    """The analysed instructions, capped.

    Truncation is marked in the text the model reads: an edit anchored in text that was cut is
    a patch that cannot apply, and the model needs to know the tail is missing to avoid it.
    """
    text = (instructions or '').strip()
    if not text:
        return _NO_INSTRUCTIONS
    if len(text) <= MAX_INSTRUCTIONS_CHARS:
        return text
    return text[:MAX_INSTRUCTIONS_CHARS] + _TRUNCATED_MARK


def _format_number(value) -> str:
    if value is None:
        return 'n/a'
    if isinstance(value, float):
        return f'{value:.4g}'
    return str(value)


def _scale_clause(gap: dict) -> str:
    scale = gap.get('scale') or {}
    scale_type = scale.get('type') or 'continuous'
    low, high = scale.get('min'), scale.get('max')
    bounds = f' {_format_number(low)}..{_format_number(high)}' if high is not None else ''
    polarity = scale.get('polarity')
    polarity_clause = f' ({polarity})' if polarity else ''
    return f'scale {scale_type}{bounds}{polarity_clause}'


def render_case(case: dict) -> str:
    """One failing case as evidence.

    ``expected_output`` is the field that changes the AI's reasoning mode (§3.2): with a reference
    it can point at a concrete divergence, without one it can only reason from rubric + rationale.
    So its absence is spelled out rather than left as a missing line.
    """
    lines = [
        f"  case #{case.get('case_id')}  score {_format_number(case.get('native_score'))}"
        f"  (missed by {_format_number(case.get('shortfall'))} of the scale)",
        f"    input:    {case.get('input') or '(empty)'}",
        f"    output:   {case.get('output') or '(empty)'}",
    ]
    expected = case.get('expected_output')
    if expected:
        lines.append(f'    expected: {expected}')
    else:
        lines.append('    expected: (none — this case is reference-free; do not assume a '
                     'ground-truth answer)')
    reasoning = case.get('reasoning')
    if reasoning:
        lines.append(f'    judge:    {reasoning}')
    return '\n'.join(lines)


def render_gap(gap: dict) -> str:
    """One dimension's gap: how it was measured, how badly it missed, and the worst cases.

    The rubric is emitted verbatim under its own heading. It is the text an ``eval_fix`` proposal
    critiques, so the AI has to be able to read it exactly as the judge saw it.
    """
    # The id is printed, not just the name: citations are required by id and an item citing an id
    # that is not in this run is dropped, so a brief that shows only names asks for a guess.
    header = (
        f"dimension #{gap.get('dimension_id')}: {gap.get('name')} · engine {gap.get('engine')} · "
        f"{_scale_clause(gap)} · target {gap.get('target_operator')} {_format_number(gap.get('target'))} · "
        f"weight {_format_number(gap.get('weight'))}"
    )
    stats = (
        f"  missed {gap.get('missed_count')}/{gap.get('scored_count')} scored cases · "
        f"mean shortfall {_format_number(gap.get('mean_shortfall'))}"
    )
    rubric = gap.get('rubric')
    rubric_block = (
        f'  rubric (verbatim, as the judge saw it):\n    {rubric}' if rubric
        else '  rubric: (none recorded in the run snapshot)'
    )
    parts = [header, stats, rubric_block]
    parts.extend(render_case(case) for case in gap.get('cases') or [])
    return '\n'.join(parts)


def render_gaps(gaps: List[dict]) -> str:
    if not gaps:
        return _NO_GAPS
    return '\n\n'.join(render_gap(gap) for gap in gaps)


def render_coverage(coverage: dict) -> str:
    """What the brief is and is not based on.

    Told to the model, not just the user: an AI that believes it saw every failure will write a
    diagnosis in absolute terms ("the only problem is X") about a sample.
    """
    coverage = coverage or {}
    returned = coverage.get('gap_dimensions_returned')
    total = coverage.get('gap_dimensions_total')
    lines = [
        f"{coverage.get('total_cases', 0)} dataset cases · "
        f"{coverage.get('targeted_bindings', 0)} dimensions with a configured target · "
        f"{coverage.get('missed_bindings', 0)} of them missed it",
    ]
    if total and returned is not None and returned < total:
        lines.append(
            f'Showing the {returned} highest-impact of {total} failing dimensions — the rest are '
            'not in this brief, so do not describe your diagnosis as exhaustive.'
        )
    per_dimension = coverage.get('max_cases_per_dimension')
    if per_dimension:
        lines.append(f'At most {per_dimension} worst cases are shown per dimension.')

    excluded_error = coverage.get('excluded_error_results') or 0
    if excluded_error:
        lines.append(
            f'{excluded_error} result(s) errored (judge or script failure) and were excluded — '
            'the agent was never measured on those, so they are not evidence of anything.'
        )
    excluded_pending = coverage.get('excluded_pending_human') or 0
    if excluded_pending:
        lines.append(f'{excluded_pending} result(s) await human scoring and were excluded.')
    return '\n'.join(lines)


def render_agent_context(agent_context: Optional[dict]) -> str:
    """Read-only surroundings: model and toolkits (§3.2).

    P1 cannot propose changes here, but without it the AI attributes a gap to the instructions
    when the real cause is a missing toolkit — and it should be able to say so as a note.
    """
    context = agent_context or {}
    model_name = context.get('model_name')
    toolkits = context.get('toolkit_names') or []
    lines = [f"model: {model_name or '(unknown)'}"]
    lines.append(f"toolkits: {', '.join(toolkits) if toolkits else '(none)'}")
    lines.append(
        'This is context only. You cannot propose changes to the model or toolkits — if a gap is '
        'caused by a missing capability rather than by the instructions, say so in the diagnosis.'
    )
    return '\n'.join(lines)


def build_enhance_system_prompt(
    template: str,
    application_name: str,
    instructions: Optional[str],
    gaps: List[dict],
    coverage: Optional[dict] = None,
    agent_context: Optional[dict] = None,
) -> str:
    """Fill the ``enhance_agent_from_eval`` service prompt template.

    Mirrors ``build_eval_dimensions_system_prompt``: the template is config-managed so it can be
    tuned without a deploy, and a malformed one raises rather than silently producing a prompt
    with a literal ``{placeholder}`` in it.
    """
    try:
        return template.format(
            application_name=application_name or '(unnamed agent)',
            instructions=truncate_instructions(instructions),
            agent_context=render_agent_context(agent_context),
            coverage=render_coverage(coverage),
            gaps=render_gaps(gaps or []),
        )
    except (KeyError, IndexError, ValueError) as exc:
        raise EnhancePromptTemplateError(
            'enhance_agent_from_eval template is malformed'
        ) from exc
