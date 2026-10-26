"""Project skills offered to a chat turn without being participants (#7126).

The catalog carries identity and description only; the SDK fetches a body on
load_skill, and `~name` loads at most the per-turn cap of bodies here.
"""
import difflib
import re
from typing import Optional

from pylon.core.tools import log
from sqlalchemy import func
from tools import context, db, rpc_tools

from .folder_access import folder_ids_exclusion_clause
from .skill_utils import get_max_invoked_skills_per_turn
from .support_utils import get_support_config
from ..models.enums.all import ParticipantTypes
from ..models.skill import Skill, SkillVersion


PROJECT_SKILLS_CONFIG_TYPE = 'project_skills'
SKILL_ORIGIN_PROJECT = 'project'
SOCIAL_PLUGIN = 'social'
BASE_VERSION_NAME = 'base'
DID_YOU_MEAN_CUTOFF = 0.6
MENTION_TOKEN_PATTERN = re.compile(r'(?<![0-9A-Za-z~-])~([a-z0-9][a-z0-9-]{0,62})(?![0-9A-Za-z-])', re.IGNORECASE)


class UnknownSkillMentionError(ValueError):
    pass


def is_project_skills_in_chat_enabled(project_id: int) -> bool:
    try:
        record = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_first_filtered_project(
            project_id=project_id, filter_fields={'type': PROJECT_SKILLS_CONFIG_TYPE},
        )
    except Exception as exc:  # pylint: disable=W0718
        log.warning('Could not read project skills setting of project %s: %s', project_id, exc)
        return False
    return ((record or {}).get('data') or {}).get('enabled') is True


def is_support_project(project_id: int) -> bool:
    return get_support_config().get('project_id') == project_id


def conversation_user_ids(conversation) -> set:
    return {
        str((participant.entity_meta or {}).get('id'))
        for participant in conversation.participants
        if participant.entity_name == ParticipantTypes.user.value
    }


def only_the_sender_reads(conversation, sender_id) -> bool:
    return bool(conversation.is_private) and conversation_user_ids(conversation) <= {str(sender_id)}


def social_installed() -> bool:
    """Folders exist only with the social plugin; when that cannot be told, assume they do."""
    try:
        return SOCIAL_PLUGIN in context.module_manager.modules
    except Exception:  # pylint: disable=W0718
        return True


def user_restricted_folder_ids(project_id: int, user_id) -> list:
    """Unlike folder_access.get_restricted_folder_ids, an RPC timeout (queue.Empty) propagates instead of reading as "no folders"."""
    return rpc_tools.RpcMixin().rpc.timeout(3).social_get_restricted_folder_ids(
        project_id=project_id, entity_type='skill', user_id=user_id,
    ) or []


def restricted_folder_ids_for_everyone(project_id: int) -> list:
    rpc = rpc_tools.RpcMixin().rpc
    if not rpc.timeout(3).social_folder_permissions_supported(project_id):
        return []
    member_ids = rpc.timeout(5).admin_get_users_ids_in_project(project_id=project_id) or []
    restricted = set()
    for member_id in {int(member_id) for member_id in member_ids}:
        restricted.update(user_restricted_folder_ids(project_id, member_id))
    return sorted(restricted)


def restricted_folder_ids_for_chat(project_id: int, sender_id, conversation) -> list:
    if not social_installed():
        return []
    if only_the_sender_reads(conversation, sender_id):
        return user_restricted_folder_ids(project_id, sender_id)
    return restricted_folder_ids_for_everyone(project_id)


def load_project_skill_rows(project_id: int, restricted_folder_ids: list) -> list:
    with db.with_project_schema_session(project_id) as session:
        query = session.query(
            Skill.id, Skill.name, Skill.description, Skill.meta,
            SkillVersion.id, SkillVersion.name, SkillVersion.meta,
            (func.length(func.btrim(SkillVersion.instructions)) > 0).label('has_body'),
        ).join(SkillVersion, SkillVersion.skill_id == Skill.id)
        clause = folder_ids_exclusion_clause('skill', Skill.id, restricted_folder_ids)
        if clause is not None:
            query = query.filter(clause)
        return query.order_by(Skill.id, SkillVersion.id).all()


def default_version(versions: dict, skill_meta: Optional[dict]) -> Optional[dict]:
    default_version_id = (skill_meta or {}).get('default_version_id')
    if default_version_id in versions:
        return versions[default_version_id]
    return next((version for version in versions.values() if version['name'] == BASE_VERSION_NAME), None)


def catalog_entries(rows: list, project_id: int) -> list:
    skills = {}
    for skill_id, name, description, skill_meta, version_id, version_name, version_meta, has_body in rows:
        skill = skills.setdefault(skill_id, {'name': name, 'description': description, 'meta': skill_meta, 'versions': {}})
        skill['versions'][version_id] = {'id': version_id, 'name': version_name, 'meta': version_meta, 'has_body': has_body}
    entries, taken_names = [], set()
    for skill_id in sorted(skills):
        skill = skills[skill_id]
        version = default_version(skill['versions'], skill['meta'])
        name_key = (skill['name'] or '').strip().lower()
        if version is None or not version['has_body'] or not name_key or name_key in taken_names:
            continue
        taken_names.add(name_key)
        entries.append({
            'skill_id': skill_id,
            'skill_version_id': version['id'],
            'skill_project_id': project_id,
            'name': skill['name'],
            'description': skill['description'],
            'version_name': version['name'],
            'icon_meta': (version['meta'] or {}).get('icon_meta'),
            'origin': SKILL_ORIGIN_PROJECT,
        })
    return entries


def project_skill_catalog(project_id: int, sender_id, conversation) -> list:
    if is_support_project(project_id):
        return []
    try:
        restricted = restricted_folder_ids_for_chat(project_id, sender_id, conversation)
        return catalog_entries(load_project_skill_rows(project_id, restricted), project_id)
    except Exception:  # pylint: disable=W0718
        log.exception('[Skills] Project skill catalog of project %s unavailable this turn', project_id)
        return []


def mention_tokens(user_input) -> list:
    texts = []
    if isinstance(user_input, str):
        texts.append(user_input)
    elif isinstance(user_input, list):
        texts.extend(
            block.get('text') or '' for block in user_input
            if isinstance(block, dict) and block.get('type') == 'text'
        )
    return list(dict.fromkeys(match.group(1).lower() for text in texts for match in MENTION_TOKEN_PATTERN.finditer(text)))


def did_you_mean_error(unknown_tokens: list, known_names: set) -> Optional[UnknownSkillMentionError]:
    suggestions = []
    for token in unknown_tokens:
        close = difflib.get_close_matches(token, sorted(known_names), n=1, cutoff=DID_YOU_MEAN_CUTOFF)
        if close:
            suggestions.append(f'No skill named "~{token}". Did you mean ~{close[0]}?')
    if not suggestions:
        return None
    return UnknownSkillMentionError(' '.join(suggestions))


def load_skill_bodies(project_id: int, version_ids: list) -> dict:
    if not version_ids:
        return {}
    with db.with_project_schema_session(project_id) as session:
        rows = session.query(SkillVersion.id, SkillVersion.instructions).filter(
            SkillVersion.id.in_(version_ids)
        ).all()
    return {version_id: instructions for version_id, instructions in rows}


def resolve_mentioned_project_skills(user_input, known_names: set, catalog: list, project_id: int,
                                     strict: bool = False) -> list:
    """Catalog entries named by `~token` in the message, with their bodies, at most the per-turn cap.

    Only a fresh send is strict: a regenerated or continued message was accepted
    against an older catalog, so its unknown tokens stay literal instead of failing.
    """
    known = {name.lower() for name in known_names if name}
    tokens = [token for token in mention_tokens(user_input) if token not in known]
    if not tokens:
        return []
    by_name = {entry['name'].lower(): entry for entry in catalog}
    if strict:
        error = did_you_mean_error([token for token in tokens if token not in by_name], known | set(by_name))
        if error is not None:
            raise error
    mentioned = [by_name[token] for token in tokens if token in by_name][:get_max_invoked_skills_per_turn()]
    bodies = load_skill_bodies(project_id, [entry['skill_version_id'] for entry in mentioned])
    return [{**entry, 'instructions': bodies.get(entry['skill_version_id']) or ''} for entry in mentioned]


def with_mentioned_bodies(catalog: list, mentioned: list) -> list:
    loaded = {(entry['skill_project_id'], entry['skill_id']): entry for entry in mentioned}
    return [loaded.get((entry['skill_project_id'], entry['skill_id']), entry) for entry in catalog]
