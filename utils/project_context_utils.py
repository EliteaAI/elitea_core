"""Pure Project Context delivery rules shared by chat entrypoints."""


def prepend_project_context(instructions: str, content: str) -> str:
    return f"# Project Context\n\n{content}\n\n---\n\n{instructions}"


def prepare_project_context_delivery(instructions: str, project_context: dict) -> tuple[str, dict | None]:
    """Use progressive disclosure when configured; otherwise preserve legacy injection."""
    content = project_context.get('content') or ''
    if not project_context.get('enabled') or not content:
        return instructions, None

    activation_description = project_context.get('activation_description')
    if not activation_description:
        return prepend_project_context(instructions, content), None

    return instructions, {
        'content': content,
        'activation_description': activation_description,
        'revision': project_context.get('revision'),
    }
