import json

from ..models.pd.project_context import (
    PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN,
    PROJECT_CONTEXT_MAX_LEN,
)


# the model is told the same number enforcement rejects against, so the prompt cannot drift from it
PROJECT_CONTEXT_OUTPUT_CONTRACT = f"""Required output contract (this overrides any earlier output-shape wording):
Return only a valid JSON object with exactly these keys:
{{
  "activation_description": "A concise description of the user requests that should load this context, max {PROJECT_CONTEXT_ACTIVATION_DESCRIPTION_MAX_LEN} characters",
  "project_background": "The complete Project Background in Markdown, max {PROJECT_CONTEXT_MAX_LEN} characters"
}}

{PROJECT_CONTEXT_MAX_LEN} is a hard limit, not a target: a project_background longer than that is rejected outright and
the whole response is discarded, so plan the sections to fit and finish inside the budget.
The activation description must classify intent without revealing or repeating the full context.
Do not include explanations, markdown fences, or extra keys."""


def build_create_project_context_system_prompt(template: str) -> str:
    return f"{template.rstrip()}\n\n{PROJECT_CONTEXT_OUTPUT_CONTRACT}"


def build_edit_project_context_system_prompt(
    template: str,
    current_project_background: str,
    current_activation_description: str | None = None,
) -> str:
    current_config = {
        "activation_description": current_activation_description or "",
        "project_background": current_project_background or "",
    }
    rendered = template.format(current_config=json.dumps(current_config, indent=2))
    return build_create_project_context_system_prompt(rendered)
