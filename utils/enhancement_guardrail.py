"""Admin-policy filter on a validated "Enhance with AI" proposal.

Applied after :func:`enhancement_validation.ground_proposal`, which already filters for grounding
(quality) reasons. This filters for policy reasons — an admin turned a fix kind off — which is why
the counts are reported separately (``coverage.blocked_*``, not ``discarded_*``): a nonzero
``discarded_*`` count signals a grounding regression worth investigating, a nonzero ``blocked_*``
count just reflects a deliberate admin choice.
"""


def apply_kind_guardrail(proposal, *, agent_fixes_enabled: bool, allowed_eval_fix_kinds: set) -> dict:
    """Filter ``proposal`` in place per admin-configured fix-type policy.

    Returns ``{'blocked_agent_fixes': int, 'blocked_eval_fixes': int}``.
    """
    blocked_agent = 0
    if not agent_fixes_enabled and proposal.agent_fixes:
        blocked_agent = len(proposal.agent_fixes)
        proposal.agent_fixes = []

    kept_eval_fixes = []
    blocked_eval = 0
    for fix in proposal.eval_fixes:
        if fix.kind in allowed_eval_fix_kinds:
            kept_eval_fixes.append(fix)
        else:
            blocked_eval += 1
    proposal.eval_fixes = kept_eval_fixes

    proposal.coverage.blocked_agent_fixes = blocked_agent
    proposal.coverage.blocked_eval_fixes = blocked_eval
    return {
        'blocked_agent_fixes': blocked_agent,
        'blocked_eval_fixes': blocked_eval,
    }
