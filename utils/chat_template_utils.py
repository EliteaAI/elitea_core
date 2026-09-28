"""Utilities for keeping chat_templates participants in sync with entity changes."""

from pylon.core.tools import log

from tools import db

from ..models.chat_template import ChatTemplate


def delete_entity_from_templates(project_id: int, entity_names: list, entity_id: int, entity_project_id: int) -> None:
    """Remove every participant matching entity_names + entity_id + entity_project_id
    from all chat templates in the given project.
    """
    try:
        with db.get_session(project_id) as session:
            templates = session.query(ChatTemplate).all()
            changed = False
            for template in templates:
                original = list(template.participants or [])
                updated = [
                    p for p in original
                    if not (
                        p.get('entity_name') in entity_names
                        and p.get('id') == entity_id
                        and p.get('project_id') == entity_project_id
                    )
                ]
                if len(updated) != len(original):
                    template.participants = updated
                    changed = True
            if changed:
                session.commit()
    except Exception:  # pylint: disable=broad-except
        log.exception(
            "chat_template_utils: failed to delete entity (entity_names=%s, "
            "entity_id=%s, entity_project_id=%s) from templates in project %s",
            entity_names, entity_id, entity_project_id, project_id,
        )


def update_entity_name_in_templates(
    project_id: int, entity_names: list, entity_id: int, entity_project_id: int, new_name: str
) -> None:
    """Update the name field for all matching participants in chat templates."""
    try:
        with db.get_session(project_id) as session:
            templates = session.query(ChatTemplate).all()
            any_changed = False
            for template in templates:
                updated = []
                template_changed = False
                for p in list(template.participants or []):
                    if (
                        p.get('entity_name') in entity_names
                        and p.get('id') == entity_id
                        and p.get('project_id') == entity_project_id
                    ):
                        p = {**p, 'name': new_name}
                        template_changed = True
                    updated.append(p)
                if template_changed:
                    template.participants = updated
                    any_changed = True
            if any_changed:
                session.commit()
    except Exception:  # pylint: disable=broad-except
        log.exception(
            "chat_template_utils: failed to update entity name (entity_names=%s, "
            "entity_id=%s, entity_project_id=%s) in templates in project %s",
            entity_names, entity_id, entity_project_id, project_id,
        )
