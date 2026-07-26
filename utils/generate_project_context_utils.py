import json


def build_edit_project_context_system_prompt(template: str, current_content: str) -> str:
    current_config = {"project_background": current_content or ""}
    return template.format(current_config=json.dumps(current_config, indent=2))
