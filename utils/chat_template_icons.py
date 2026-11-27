"""Resolve the current custom icon of chat template participants at read time.

Templates store only participant references, so icons are looked up from the
entities themselves: a changed icon shows up without re-saving the template.
"""

from collections import defaultdict

from pylon.core.tools import log
from sqlalchemy.orm import selectinload
from tools import db

from .utils import get_public_project_id
from ..models.all import Application
from ..models.skill import Skill

_MODEL_BY_ENTITY_NAME = {
    'application': Application,
    'pipeline': Application,
    'skill': Skill,
}


def _select_icon_version(entity):
    versions = entity.versions or []
    if not versions:
        return None
    return entity.get_default_version() or min(versions, key=lambda version: version.created_at)


def _custom_icon_meta(entity):
    version = _select_icon_version(entity)
    icon_meta = ((version.meta or {}) if version else {}).get('icon_meta')
    # Resetting to the default icon leaves an icon_meta with a blank url behind
    if isinstance(icon_meta, dict) and str(icon_meta.get('url') or '').strip():
        return icon_meta
    return None


def _readable_project_ids(project_id: int) -> set:
    try:
        public_project_id = get_public_project_id()
    except Exception:  # pylint: disable=broad-except
        log.exception("chat_template_icons: failed to resolve the public project id")
        public_project_id = None
    return {project_id, public_project_id} - {None}


def _participant_ref(participant: dict, project_id: int):
    model = _MODEL_BY_ENTITY_NAME.get(participant.get('entity_name'))
    if model is None or participant.get('id') is None:
        return None
    return participant.get('project_id') or project_id, model, participant['id']


def _load_icons(wanted: dict) -> dict:
    icons = {}
    for (owner_id, model), entity_ids in wanted.items():
        try:
            with db.with_project_schema_session(owner_id) as session:
                entities = (
                    session.query(model)
                    .options(selectinload(model.versions))
                    .filter(model.id.in_(entity_ids))
                    .all()
                )
                for entity in entities:
                    icons[(owner_id, model, entity.id)] = _custom_icon_meta(entity)
        except Exception:  # pylint: disable=broad-except
            log.exception(
                "chat_template_icons: failed to load %s icons in project %s", model.__name__, owner_id,
            )
    return icons


def attach_participant_icons(templates: list, project_id: int) -> list:
    """Add the current icon_meta to agent, pipeline and skill participants of serialized templates.

    Only the template's own project and the public project are read, matching where
    template participants may come from. Participants without a custom icon are left as is.
    """
    readable_project_ids = _readable_project_ids(project_id)
    participants = [p for template in templates for p in template.get('participants') or []]

    wanted = defaultdict(set)
    for participant in participants:
        ref = _participant_ref(participant, project_id)
        if ref is not None and ref[0] in readable_project_ids:
            owner_id, model, entity_id = ref
            wanted[(owner_id, model)].add(entity_id)
    if not wanted:
        return templates

    icons = _load_icons(wanted)
    for participant in participants:
        icon_meta = icons.get(_participant_ref(participant, project_id))
        if icon_meta:
            participant['icon_meta'] = icon_meta
    return templates
