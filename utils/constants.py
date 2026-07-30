import re
from enum import Enum

PROMPT_LIB_MODE = 'prompt_lib'

# Detects embedded images pasted into agent text fields (issue #6040): base64
# image data URIs and <img> tags. Scoped to images only so instructions that
# legitimately contain angle brackets, code, or XML/HTML examples are not flagged.
_EMBEDDED_IMAGE_RE = re.compile(r'data:image/[^;]+;base64,|<img\b', re.IGNORECASE)


def contains_embedded_image(value) -> bool:
    """Return True if the string carries an embedded image payload."""
    if not isinstance(value, str):
        return False
    return bool(_EMBEDDED_IMAGE_RE.search(value))

ENTITY_DESCRIPTION_LEN_LIMITATION_4_LIST_API: int = 210

ICON_PATH_DELIMITER: str = '___'

# Predefined agent categories shown in the publish modal and Agent Studio filter
# bar. These are non-removable system defaults; admins may add extra categories
# via the guardrails configuration. "Other" is the permanent fallback category.
DEFAULT_AGENT_CATEGORIES: list = [
    'Business Analyst',
    'Quality Assurance',
    'Development',
    'DevOps',
    'Project Management',
    'Knowledge & Documentation',
    'Elitea',
    'Epam',
    'Other',
]

# Skills keep their OWN category list, independently managed from agents. It is
# seeded as a duplicate of DEFAULT_AGENT_CATEGORIES so the two start identical,
# but they can diverge (admins manage each list separately).
DEFAULT_SKILL_CATEGORIES: list = [
    'Business Analyst',
    'Quality Assurance',
    'Development',
    'DevOps',
    'Project Management',
    'Knowledge & Documentation',
    'Elitea',
    'Epam',
    'Other',
]

# Permanent fallback category. Agents without any valid category are surfaced
# under this filter in Agent Studio.
DEFAULT_FALLBACK_CATEGORY: str = 'Other'


# Analytics: system user filtering (excluded from all analytics aggregations)
SYSTEM_USER_EMAILS = ['system@centry.user']
SYSTEM_USER_EMAIL_PATTERN = 'system_user_%@centry.user'

# Analytics: default date range when no date params provided
DEFAULT_DATE_RANGE_DAYS = 7

# Analytics: hard cap on the queryable span so an unbounded daily-trend query
# (one row per calendar day) can't be driven to pathological cardinality.
MAX_DATE_RANGE_DAYS = 366


class PredictionEvents(str, Enum):
    prediction_done = 'prediction_done'
