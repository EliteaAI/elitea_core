"""Normalise the target fields of AI-generated eval dimension drafts."""
from typing import Optional, Tuple

from pylon.core.tools import log

# Narrower than the model's _OPERATORS: the dimension form only offers these three, so a draft
# carrying '>' or '<' would open with a criterion the user cannot see or edit.
_DRAFT_TARGET_OPERATORS = frozenset({'>=', '<=', '=='})
_DRAFT_TARGET_PAIRS = (
    ('default_target', 'default_target_operator'),
    ('target', 'target_operator'),
)


def _as_number(value) -> Optional[float]:
    if isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _valid_draft_target(item: dict, target, operator) -> Optional[Tuple[float, str]]:
    number = _as_number(target)
    if number is None or operator not in _DRAFT_TARGET_OPERATORS:
        return None
    scale_min = _as_number(item.get('scale_min', 0.0))
    scale_max = _as_number(item.get('scale_max', 100.0))
    if scale_min is None or scale_max is None or not scale_min <= number <= scale_max:
        return None
    if item.get('scale_type') == 'binary' and (operator != '==' or number not in (0.0, 1.0)):
        return None
    return number, operator


def sanitize_draft_target(item: dict) -> None:
    """Normalise an LLM dimension draft's target pairs in place.

    A target the form cannot represent is dropped rather than failing the whole draft — the user
    can still set one by hand. When only one of the dimension-default / binding pairs survives,
    it is copied to the other so the draft opens with the target filled in either way.
    """
    valid = {}
    for target_key, operator_key in _DRAFT_TARGET_PAIRS:
        target, operator = item.get(target_key), item.get(operator_key)
        pair = _valid_draft_target(item, target, operator)
        if pair is None and (target is not None or operator is not None):
            log.info(
                "generate_eval_dimensions: dropping unusable %s=%r %s=%r on draft %r",
                target_key, target, operator_key, operator, item.get('name'),
            )
        valid[target_key] = pair

    default_pair, binding_pair = valid['default_target'], valid['target']
    default_pair = default_pair or binding_pair
    binding_pair = binding_pair or default_pair

    for (target_key, operator_key), pair in zip(_DRAFT_TARGET_PAIRS, (default_pair, binding_pair)):
        item[target_key], item[operator_key] = pair if pair else (None, None)
