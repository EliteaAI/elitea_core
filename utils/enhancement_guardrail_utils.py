"""Admin-configured policy for which "Enhance with AI" fix kinds may be proposed.

Distinct from ``enhancement_validation``'s grounding checks: grounding drops items the model
invented, this drops kinds an admin has turned off entirely. Read live (uncached) so a change made
in ``admin_ui`` applies to the next request without a pylon restart, matching
``skill_publish_utils._skill_guardrail_config``.
"""

from tools import this

from ..models.pd.enhance_from_eval import EvalFixKind

_ALL_EVAL_FIX_KINDS = (
    EvalFixKind.dimension_rubric,
    EvalFixKind.dimension_target,
    EvalFixKind.dataset_case_expected,
    EvalFixKind.dataset_coverage_gap,
)


def _enhance_guardrail_config() -> dict:
    """Live guardrail config (read each call so admin changes need no reload)."""
    return this.descriptor.config.get('enhance_guardrail', {}) or {}


def is_agent_fixes_enabled() -> bool:
    return bool(_enhance_guardrail_config().get('agent_fixes_enabled', True))


def get_allowed_eval_fix_kinds() -> set:
    kinds_cfg = _enhance_guardrail_config().get('eval_fix_kinds', {}) or {}
    return {kind for kind in _ALL_EVAL_FIX_KINDS if bool(kinds_cfg.get(kind, True))}
