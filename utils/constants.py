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


class PredictionEvents(str, Enum):
    prediction_done = 'prediction_done'
