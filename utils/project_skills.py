import difflib
import re

from pylon.core.tools import log
from sqlalchemy import func
from tools import context, db, rpc_tools, this

from .folder_access import build_folder_ids_exclusion_clause
from .skill_utils import SkillInvocationLimitError, get_max_invoked_skills_per_turn
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
    platform_settings = (this.descriptor.config.get('skills') or {}).get('project_skills_in_chat') or {}
    if platform_settings.get('available') is not True:
        return False
    try:
        record = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_first_filtered_project(
            project_id=project_id, filter_fields={'type': PROJECT_SKILLS_CONFIG_TYPE},
        )
    except Exception as exc:  # pylint: disable=W0718
        log.warning('Could not read project skills setting of project %s: %s', project_id, exc)
        return False
    project_choice = ((record or {}).get('data') or {}).get('enabled')
    if project_choice is not None:
        return project_choice is True
    return platform_settings.get('project_default') is True


def build_project_skill_catalog(project_id: int, sender_id, conversation) -> list:
    if get_support_config().get('project_id') == project_id:
        return []
    reader_ids = {
        str((participant.entity_meta or {}).get('id'))
        for participant in conversation.participants
        if participant.entity_name == ParticipantTypes.user.value
    }
    try:
        rpc = rpc_tools.RpcMixin().rpc
        if SOCIAL_PLUGIN not in context.module_manager.modules:
            folder_owner_ids = set()
        elif conversation.is_private and reader_ids <= {str(sender_id)}:
            folder_owner_ids = {sender_id}
        elif rpc.timeout(3).social_folder_permissions_supported(project_id):
            folder_owner_ids = {int(member_id) for member_id in rpc.timeout(5).admin_get_users_ids_in_project(project_id=project_id) or []}
        else:
            folder_owner_ids = set()
        restricted_folder_ids = sorted({
            folder_id
            for owner_id in folder_owner_ids
            for folder_id in rpc.timeout(3).social_get_restricted_folder_ids(
                project_id=project_id, entity_type='skill', user_id=owner_id,
            ) or []
        })
        folder_filter = build_folder_ids_exclusion_clause('skill', Skill.id, restricted_folder_ids)
        if restricted_folder_ids and folder_filter is None:
            raise RuntimeError('No folder filter for restricted skill folders; the social model lookup failed')
        with db.with_project_schema_session(project_id) as session:
            query = session.query(
                Skill.id, Skill.name, Skill.description, Skill.meta,
                SkillVersion.id, SkillVersion.name, SkillVersion.meta,
                (func.length(func.btrim(SkillVersion.instructions)) > 0).label('has_body'),
            ).join(SkillVersion, SkillVersion.skill_id == Skill.id)
            if folder_filter is not None:
                query = query.filter(folder_filter)
            rows = query.order_by(Skill.id, SkillVersion.id).all()
    except Exception:  # pylint: disable=W0718
        log.exception('[Skills] Project skill catalog of project %s unavailable this turn', project_id)
        return []

    skills = {}
    for skill_id, name, description, skill_meta, version_id, version_name, version_meta, has_body in rows:
        skill = skills.setdefault(skill_id, {'name': name, 'description': description, 'meta': skill_meta, 'versions': {}})
        skill['versions'][version_id] = {'id': version_id, 'name': version_name, 'meta': version_meta, 'has_body': has_body}
    catalog, taken_names = [], set()
    for skill_id in sorted(skills):
        skill = skills[skill_id]
        versions = skill['versions']
        version = versions.get((skill['meta'] or {}).get('default_version_id')) or next(
            (candidate for candidate in versions.values() if candidate['name'] == BASE_VERSION_NAME), None,
        )
        name_key = (skill['name'] or '').strip().lower()
        if version is None or not version['has_body'] or not name_key or name_key in taken_names:
            continue
        taken_names.add(name_key)
        catalog.append({
            'skill_id': skill_id,
            'skill_version_id': version['id'],
            'skill_project_id': project_id,
            'name': skill['name'],
            'description': skill['description'],
            'version_name': version['name'],
            'icon_meta': (version['meta'] or {}).get('icon_meta'),
            'origin': SKILL_ORIGIN_PROJECT,
        })
    return catalog


def resolve_mentioned_project_skills(user_input, known_names: set, catalog: list, project_id: int,
                                     strict: bool = False) -> list:
    if isinstance(user_input, str):
        texts = [user_input]
    elif isinstance(user_input, list):
        texts = [block.get('text') or '' for block in user_input if isinstance(block, dict) and block.get('type') == 'text']
    else:
        texts = []
    known = {name.lower() for name in known_names if name}
    mention_tokens = dict.fromkeys(match.group(1).lower() for text in texts for match in MENTION_TOKEN_PATTERN.finditer(text))
    tokens = [token for token in mention_tokens if token not in known]
    if not tokens:
        return []
    by_name = {entry['name'].lower(): entry for entry in catalog}
    if strict:
        suggestable_names = sorted(known | set(by_name))
        suggestions = []
        for token in tokens:
            close = [] if token in by_name else difflib.get_close_matches(token, suggestable_names, n=1, cutoff=DID_YOU_MEAN_CUTOFF)
            if close:
                suggestions.append(f'No skill named "~{token}". Did you mean ~{close[0]}?')
        if suggestions:
            raise UnknownSkillMentionError(' '.join(suggestions))
    mentioned = [by_name[token] for token in tokens if token in by_name]
    max_allowed = get_max_invoked_skills_per_turn()
    if len(mentioned) > max_allowed:
        message_mention_count = len([token for token in mention_tokens if token in known or token in by_name])
        raise SkillInvocationLimitError(message_mention_count, max_allowed)
    if not mentioned:
        return []
    with db.with_project_schema_session(project_id) as session:
        bodies = dict(session.query(SkillVersion.id, SkillVersion.instructions).filter(
            SkillVersion.id.in_([entry['skill_version_id'] for entry in mentioned])
        ).all())
    return [{**entry, 'instructions': bodies.get(entry['skill_version_id']) or ''} for entry in mentioned]
