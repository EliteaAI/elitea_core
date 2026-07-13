from enum import Enum

PROMPT_LIB_MODE = 'prompt_lib'

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

# Permanent fallback category. Agents without any valid category are surfaced
# under this filter in Agent Studio.
DEFAULT_FALLBACK_CATEGORY: str = 'Other'


# Analytics: system user filtering (excluded from all analytics aggregations)
SYSTEM_USER_EMAILS = ['system@centry.user']
SYSTEM_USER_EMAIL_PATTERN = 'system_user_%@centry.user'

# Analytics: default date range when no date params provided
DEFAULT_DATE_RANGE_DAYS = 7


class PredictionEvents(str, Enum):
    prediction_done = 'prediction_done'
