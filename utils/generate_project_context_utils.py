import json


def build_edit_project_context_system_prompt(template: str, current_project_background: str) -> str:
    current_config = {"project_background": current_project_background or ""}
    return template.format(current_config=json.dumps(current_config, indent=2))
