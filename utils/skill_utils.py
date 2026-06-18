import re
from contextlib import contextmanager
from typing import List, Optional, Tuple, Literal

from sqlalchemy import func, or_, asc, desc
from sqlalchemy.orm import selectinload

from tools import db, auth, serialize, rpc_tools

from .utils import set_columns_as_attrs
from ..models.skill import Skill, SkillVersion, EntitySkillMapping
from ..models.all import Tag
from ..models.enums.all import SkillEntityTypes
from ..models.pd.skill import (
    SkillCreateModel,
    SkillDetailModel,
    SkillUpdateModel,
    SkillImportResultModel,
)
from ..models.pd.skill_version import (
    SkillVersionCreateModel,
    SkillVersionUpdateModel,
    SkillVersionDetailModel,
)


MAX_SKILLS_PER_AGENT = 5

_SKILL_SORT_WHITELIST = frozenset({'created_at', 'name', 'id'})


class SkillError(Exception):
    """Base class for all skill domain errors.

    Each subclass carries ``http_status`` — the HTTP status the v2 API boundary
    returns for it. The API catches ``SkillError`` and returns
    ``{'error': str(exc)}, exc.http_status``; non-HTTP callers (e.g. the RPC
    layer) let it propagate and ignore ``http_status``. Defaults to 400;
    subclasses override it. An unexpected, non-``SkillError`` error is never
    caught at the boundary and correctly propagates to a 500.
    """
    http_status = 400


class SkillNotFoundError(SkillError):
    http_status = 404

    def __init__(self, skill_id: int):
        super().__init__(f"Skill with id {skill_id} not found")
        self.skill_id = skill_id


class SkillVersionNotFoundError(SkillError):
    http_status = 404

    def __init__(self, skill_id: int, version_id: int = None, version_name: str = None):
        if version_id:
            msg = f"Skill version with id {version_id} not found for skill {skill_id}"
        else:
            msg = f"Skill version '{version_name}' not found for skill {skill_id}"
        super().__init__(msg)
        self.skill_id = skill_id
        self.version_id = version_id
        self.version_name = version_name


class SkillVersionInUseError(SkillError):
    http_status = 409

    def __init__(self, version_id: int, usage_count: int):
        super().__init__(
            f"Skill version {version_id} is attached to {usage_count} agent(s). "
            "Detach it from all agents before deleting."
        )
        self.version_id = version_id
        self.usage_count = usage_count


class SkillLimitExceededError(SkillError):
    http_status = 400

    def __init__(self, entity_version_id: int, current_count: int):
        super().__init__(
            f"Agent version {entity_version_id} already has {current_count} skills attached. "
            f"Maximum allowed is {MAX_SKILLS_PER_AGENT}."
        )
        self.entity_version_id = entity_version_id
        self.current_count = current_count


class SkillAlreadyAttachedError(SkillError):
    http_status = 409

    def __init__(self, skill_id: int, entity_version_id: int):
        super().__init__(
            f"Skill {skill_id} is already attached to agent version {entity_version_id}"
        )
        self.skill_id = skill_id
        self.entity_version_id = entity_version_id


class SkillNotAttachedError(SkillError):
    """Raised when detaching a skill that has no mapping to the agent version."""
    http_status = 404

    def __init__(self, skill_id: int, entity_version_id: int):
        super().__init__(
            f"Skill {skill_id} is not attached to agent version {entity_version_id}"
        )
        self.skill_id = skill_id
        self.entity_version_id = entity_version_id


class SkillVersionConflictError(SkillError):
    """Raised when a version name already exists for the skill (create/rename clash)."""
    http_status = 409

    def __init__(self, skill_id: int, version_name: str):
        super().__init__(
            f'Version name "{version_name}" already exists for skill {skill_id}'
        )
        self.skill_id = skill_id
        self.version_name = version_name


class SkillVersionNotUpdatableError(SkillError):
    """Raised for forbidden version mutations (rename 'base', delete only/base version)."""
    http_status = 400

    def __init__(self, message: str, version_id: int = None):
        super().__init__(message)
        self.version_id = version_id


@contextmanager
def _skill_session(session, project_id):
    """Yield a usable session. Own commit/rollback/close ONLY when we created it.

    Caller-passed session: flush (so IDs populate) but never commit/rollback/close —
    the caller owns the transaction; exceptions propagate untouched.
    Owned session (created here via ``closing(...)``): commit on success, rollback
    on error, and close on exit (the closing() context manager closes).
    """
    if session is not None:
        yield session
        session.flush()
        return
    with db.get_session(project_id) as owned:   # closing(...) → close on exit
        try:
            yield owned
            owned.commit()
        except Exception:
            owned.rollback()
            raise


def list_skills(
    project_id: int,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = 'created_at',
    sort_order: Literal['asc', 'desc'] = 'desc',
    filters: Optional[list] = None,
    session=None,
) -> Tuple[int, List[Skill]]:
    """
    List skills with pagination and sorting.

    Args:
        project_id: Project ID for database session
        limit: Maximum number of results
        offset: Number of results to skip
        sort_by: Field to sort by (created_at, name)
        sort_order: Sort direction (asc, desc)
        filters: SQLAlchemy filter conditions
        session: Database session (optional, creates one if not provided)

    Returns:
        Tuple of (total_count, list_of_skills)
    """
    if filters is None:
        filters = []

    with _skill_session(session, project_id) as s:
        # Count query
        count_query = s.query(func.count(Skill.id))
        if filters:
            count_query = count_query.filter(*filters)
        total = count_query.scalar() or 0

        if total == 0:
            return 0, []

        # Main query with eager loading
        query = s.query(Skill).options(
            selectinload(Skill.versions).selectinload(SkillVersion.tags)
        )

        if filters:
            query = query.filter(*filters)

        # Add pin status (project-wide) so pinned skills surface first and each
        # row carries `is_pinned`/`pin_updated_at` — mirrors application_utils.
        add_pins_with_priority = rpc_tools.RpcMixin().rpc.timeout(2).social_add_pins_with_priority()
        query, extra_columns = add_pins_with_priority(
            original_query=query,
            project_id=project_id,
            entity=Skill,
        )

        # Sorting: pinned first (is_pinned DESC, pin_updated_at DESC), then the
        # user's sort, then id. The query now ends with the is_pinned and
        # pin_updated_at columns added above.
        if sort_by not in _SKILL_SORT_WHITELIST:
            sort_by = 'created_at'
        sort_fn = asc if sort_order == 'asc' else desc
        query = query.order_by(
            desc(query.column_descriptions[-2]['expr']),  # is_pinned column
            desc(query.column_descriptions[-1]['expr']),  # pin_updated_at column
            sort_fn(getattr(Skill, sort_by, Skill.created_at)),
            asc(Skill.id),
        )

        # Pagination
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)

        # With extra columns the query yields Row tuples; map them back onto
        # each Skill instance so `is_pinned`/`pin_updated_at` are serializable.
        skills = list(set_columns_as_attrs(query.all(), extra_columns))
        return total, skills


def list_skills_api(
    project_id: int,
    tags: str | list | None = None,
    author_id: int | None = None,
    q: str | None = None,
    limit: int = 10,
    offset: int = 0,
    sort_by: str = 'created_at',
    sort_order: Literal['asc', 'desc'] = 'desc',
    session=None,
) -> dict:
    """
    List skills with filtering and pagination for API use.

    Args:
        project_id: Project ID
        tags: Tag IDs to filter by (comma-separated string or list)
        author_id: Filter by author ID
        q: Search query for name/description
        limit: Maximum results
        offset: Results to skip
        sort_by: Sort field
        sort_order: Sort direction
        session: Database session

    Returns:
        Dictionary with 'total' and 'skills' keys
    """
    filters = []

    # Author filter
    if author_id:
        filters.append(
            Skill.versions.any(SkillVersion.author_id == author_id)
        )

    # Tag filter
    if tags:
        if isinstance(tags, str):
            tags = [int(tag) for tag in tags.split(',')]
        for tag_id in tags:
            filters.append(
                Skill.versions.any(
                    SkillVersion.tags.any(Tag.id == tag_id)
                )
            )

    # Search filter
    if q:
        filters.append(
            or_(
                Skill.name.ilike(f"%{q}%"),
                Skill.description.ilike(f"%{q}%")
            )
        )

    total, skills = list_skills(
        project_id=project_id,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        sort_order=sort_order,
        filters=filters,
        session=session,
    )

    return {
        'total': total,
        'skills': skills,
    }


def get_skill_details(
    project_id: int,
    skill_id: int,
    version_name: str = None,
    version_id: int = None,
    session=None,
) -> dict:
    """
    Get detailed skill information with optional version details.

    Args:
        project_id: Project ID
        skill_id: Skill ID
        version_name: Optional version name to include details for
        version_id: Optional version ID to include details for
        session: Database session

    Returns:
        Dictionary with 'data' key containing skill details, or None if not found
    """
    with _skill_session(session, project_id) as s:
        skill = s.query(Skill).filter(
            Skill.id == skill_id
        ).options(
            selectinload(Skill.versions).selectinload(SkillVersion.tags)
        ).first()

        if not skill:
            return {'data': None}

        result = SkillDetailModel.model_validate(skill)

        # Get specific version details
        version = None
        if version_id:
            version = next(
                (v for v in skill.versions if v.id == version_id),
                None
            )
        elif version_name:
            version = next(
                (v for v in skill.versions if v.name == version_name),
                None
            )
        else:
            version = skill.get_default_version()

        if version:
            result.version_details = SkillVersionDetailModel.model_validate(version)

        return {'data': serialize(result)}


def create_skill(
    skill_data: SkillCreateModel,
    session,
    project_id: int,
) -> Skill:
    """Create a new skill with its initial version."""
    # Create skill
    skill = Skill(
        name=skill_data.name,
        description=skill_data.description,
        owner_id=skill_data.owner_id,
        author_id=skill_data.versions[0].author_id if skill_data.versions else auth.current_user().get('id'),
        meta=skill_data.meta or {},
    )
    session.add(skill)
    session.flush()

    # Create initial version
    for version_data in skill_data.versions:
        version = SkillVersion(
            skill_id=skill.id,
            name=version_data.name,
            instructions=version_data.instructions,
            author_id=version_data.author_id or skill.author_id,
            meta=version_data.meta or {},
        )
        session.add(version)

        # Handle tags
        if version_data.tags:
            _apply_tags_to_version(session, version, version_data.tags)

    session.flush()
    return skill


def build_skill_detail(skill: Skill) -> SkillDetailModel:
    """Build a SkillDetailModel with version_details from a refreshed skill."""
    result = SkillDetailModel.model_validate(skill)
    if skill.versions:
        result.version_details = SkillVersionDetailModel.model_validate(skill.versions[0])
    return result


def update_skill(
    project_id: int,
    skill_id: int,
    update_data: SkillUpdateModel,
    session=None,
) -> dict:
    """Update skill metadata and optionally version content."""

    with _skill_session(session, project_id) as s:
        skill = s.query(Skill).filter(
            Skill.id == skill_id
        ).options(
            selectinload(Skill.versions)
        ).first()

        if not skill:
            raise SkillNotFoundError(skill_id)

        # Update skill metadata
        if update_data.name is not None:
            skill.name = update_data.name
        if update_data.description is not None:
            skill.description = update_data.description
        if update_data.meta is not None:
            skill.meta = {**(skill.meta or {}), **update_data.meta}

        # Update version if provided
        if update_data.version:
            version = skill.get_default_version()
            if version:
                _update_version_fields(s, version, update_data.version)

        return serialize(SkillDetailModel.model_validate(skill))


def delete_skill(
    project_id: int,
    skill_id: int,
    session=None,
) -> dict:
    """Delete a skill and all its versions (cascades to agent attachments)."""
    with _skill_session(session, project_id) as s:
        skill = s.query(Skill).filter(
            Skill.id == skill_id
        ).first()

        if not skill:
            raise SkillNotFoundError(skill_id)

        s.query(EntitySkillMapping).filter(
            EntitySkillMapping.skill_id == skill_id
        ).delete(synchronize_session=False)

        s.delete(skill)
        return None


def create_skill_version(
    project_id: int,
    skill_id: int,
    version_data: SkillVersionCreateModel,
    session=None,
) -> dict:
    """Create a new version for an existing skill."""
    with _skill_session(session, project_id) as s:
        # Verify skill exists
        skill = s.query(Skill).filter(Skill.id == skill_id).first()
        if not skill:
            raise SkillNotFoundError(skill_id)

        existing_version = s.query(SkillVersion.id).filter(
            SkillVersion.skill_id == skill_id,
            SkillVersion.name == version_data.name,
        ).first()
        if existing_version:
            raise SkillVersionConflictError(skill_id, version_data.name)

        # Create version
        version = SkillVersion(
            skill_id=skill_id,
            name=version_data.name,
            instructions=version_data.instructions,
            author_id=version_data.author_id or auth.current_user().get('id'),
            meta=version_data.meta or {},
        )
        s.add(version)
        s.flush()

        # Handle tags
        if version_data.tags:
            _apply_tags_to_version(s, version, version_data.tags)

        return serialize(SkillVersionDetailModel.model_validate(version))


def update_skill_version(
    project_id: int,
    skill_id: int,
    version_id: int,
    update_data: SkillVersionUpdateModel,
    session=None,
) -> dict:
    """Update an existing skill version."""
    with _skill_session(session, project_id) as s:
        version = s.query(SkillVersion).filter(
            SkillVersion.id == version_id,
            SkillVersion.skill_id == skill_id,
        ).options(
            selectinload(SkillVersion.tags)
        ).first()

        if not version:
            raise SkillVersionNotFoundError(skill_id, version_id=version_id)

        # Prevent renaming 'base' version
        if version.name == 'base' and update_data.name and update_data.name != 'base':
            raise SkillVersionNotUpdatableError(
                'Cannot rename the base version', version_id=version_id
            )

        # Pre-check rename uniqueness (commit moved to the helper boundary).
        if update_data.name and update_data.name != version.name:
            conflict = s.query(SkillVersion.id).filter(
                SkillVersion.skill_id == skill_id,
                SkillVersion.name == update_data.name,
                SkillVersion.id != version_id,
            ).first()
            if conflict:
                raise SkillVersionConflictError(skill_id, update_data.name)

        _update_version_fields(s, version, update_data)

        return serialize(SkillVersionDetailModel.model_validate(version))


def delete_skill_version(
    project_id: int,
    skill_id: int,
    version_id: int,
    session=None,
) -> dict:
    """Delete a skill version (validates not in use by agents)."""

    with _skill_session(session, project_id) as s:
        version = s.query(SkillVersion).filter(
            SkillVersion.id == version_id,
            SkillVersion.skill_id == skill_id,
        ).first()

        if not version:
            raise SkillVersionNotFoundError(skill_id, version_id=version_id)

        # Prevent deleting 'base' version if it's the only one
        other_versions = s.query(SkillVersion).filter(
            SkillVersion.skill_id == skill_id,
            SkillVersion.id != version_id,
        ).count()

        if version.name == 'base' and other_versions == 0:
            raise SkillVersionNotUpdatableError(
                'Cannot delete the only version of a skill. Delete the skill instead.',
                version_id=version_id,
            )

        # Check if version is in use by any agents
        usage_count = s.query(EntitySkillMapping).filter(
            EntitySkillMapping.skill_version_id == version_id
        ).count()

        if usage_count > 0:
            raise SkillVersionInUseError(version_id, usage_count)

        s.delete(version)
        return None


def get_skill_version_by_name(
    project_id: int,
    skill_id: int,
    version_name: str,
    session=None,
) -> Optional[SkillVersion]:
    """Get a skill version by name."""
    with _skill_session(session, project_id) as s:
        return s.query(SkillVersion).filter(
            SkillVersion.skill_id == skill_id,
            SkillVersion.name == version_name,
        ).first()


def import_skill(
    project_id: int,
    name: str,
    description: str,
    versions: list,
    author_id: int,
    *,
    session=None,
) -> SkillImportResultModel:
    if not versions:
        raise ValueError(f"Skill '{name}' has no versions to import")

    payloads = [
        {
            'name': v.get('name', 'base'),
            'instructions': v.get('instructions', ''),
            'author_id': v.get('author_id', author_id),
            'tags': v.get('tags') or None,
            'meta': v.get('meta') or None,
        }
        for v in versions
    ]

    with _skill_session(session, project_id) as s:
        # New skill: create_skill enforces exactly one version, so create with the
        # first and append the rest — all in this one transaction.
        skill_model = SkillCreateModel.model_validate({
            'name': name,
            'description': description or name,
            'owner_id': project_id,
            'project_id': project_id,
            'user_id': author_id,
            'versions': payloads[:1],
            'meta': None,
        })
        skill = create_skill(skill_model, s, project_id)
        version_map = {v.name: v.id for v in skill.versions}
        for vp in payloads[1:]:
            detail = create_skill_version(
                project_id=project_id,
                skill_id=skill.id,
                version_data=SkillVersionCreateModel.model_validate(vp),
                session=s,
            )
            version_map[vp['name']] = detail['id']
        return SkillImportResultModel(id=skill.id, versions=version_map, reused=False)


def validate_agent_skill_limit(
    session,
    entity_version_id: int,
    entity_type: str = SkillEntityTypes.agent,
) -> bool:
    current_count = session.query(EntitySkillMapping).filter(
        EntitySkillMapping.entity_version_id == entity_version_id,
        EntitySkillMapping.entity_type == entity_type,
    ).count()

    if current_count >= MAX_SKILLS_PER_AGENT:
        raise SkillLimitExceededError(entity_version_id, current_count)

    return True


def attach_skill_to_agent(
    project_id: int,
    entity_version_id: int,
    skill_id: int,
    skill_version_id: int,
    entity_type: str = SkillEntityTypes.agent,
    session=None,
) -> dict:
    """Attach a skill to an agent version."""

    with _skill_session(session, project_id) as s:
        # Validate skill limit (raises SkillLimitExceededError)
        validate_agent_skill_limit(s, entity_version_id, entity_type)

        # Verify skill exists
        skill = s.query(Skill).filter(Skill.id == skill_id).first()
        if not skill:
            raise SkillNotFoundError(skill_id)

        # Verify version exists and belongs to skill
        version = s.query(SkillVersion).filter(
            SkillVersion.id == skill_version_id,
            SkillVersion.skill_id == skill_id,
        ).first()
        if not version:
            raise SkillVersionNotFoundError(skill_id, version_id=skill_version_id)

        # Pre-check duplicate attach (the mapping unique key is
        # (entity_version_id, skill_id, entity_type)). The DB unique constraint
        # remains a backstop for a rare TOCTOU race.
        existing_mapping = s.query(EntitySkillMapping.id).filter(
            EntitySkillMapping.entity_version_id == entity_version_id,
            EntitySkillMapping.entity_type == entity_type,
            EntitySkillMapping.skill_id == skill_id,
        ).first()
        if existing_mapping:
            raise SkillAlreadyAttachedError(skill_id, entity_version_id)

        # Create mapping
        mapping = EntitySkillMapping(
            entity_version_id=entity_version_id,
            entity_type=entity_type,
            skill_id=skill_id,
            skill_version_id=skill_version_id,
        )
        s.add(mapping)

        return {
            'skill_id': skill_id,
            'skill_version_id': skill_version_id,
            'skill_name': skill.name,
            'version_name': version.name,
        }


def detach_skill_from_agent(
    project_id: int,
    entity_version_id: int,
    skill_id: int,
    entity_type: str = SkillEntityTypes.agent,
    session=None,
) -> dict:
    """Detach a skill from an agent version."""
    with _skill_session(session, project_id) as s:
        mapping = s.query(EntitySkillMapping).filter(
            EntitySkillMapping.entity_version_id == entity_version_id,
            EntitySkillMapping.entity_type == entity_type,
            EntitySkillMapping.skill_id == skill_id,
        ).first()

        if not mapping:
            raise SkillNotAttachedError(skill_id, entity_version_id)

        s.delete(mapping)
        return None


class SkillVersionDeletedError(SkillError):
    """Raised at chat predict time when an attached skill's selected version is gone."""
    http_status = 400

    def __init__(self, skill_id: int, skill_version_id: int, skill_name: str = None):
        label = f"'{skill_name}'" if skill_name else f"id={skill_id}"
        super().__init__(
            f"Attached skill {label} references a deleted version "
            f"(skill_version_id={skill_version_id}). Re-attach the skill with a "
            "valid version before chatting."
        )
        self.skill_id = skill_id
        self.skill_version_id = skill_version_id
        self.skill_name = skill_name


def detach_skills_for_entity_versions(session, entity_version_ids, entity_type: str = None) -> int:
    """Delete skill mappings for the given entity version ids (no-op if empty)."""
    ids = [vid for vid in (entity_version_ids or []) if vid is not None]
    if not ids:
        return 0
    query = session.query(EntitySkillMapping).filter(
        EntitySkillMapping.entity_version_id.in_(ids)
    )
    if entity_type is not None:
        query = query.filter(EntitySkillMapping.entity_type == entity_type)
    return query.delete(synchronize_session=False)


def validate_agent_skills(skills: List[dict]) -> None:
    """Validate the resolved attached-skills list for the chat payload"""
    for skill in skills or []:
        instructions = skill.get('instructions')
        if instructions is None or not instructions.strip():
            raise SkillVersionDeletedError(
                skill_id=skill.get('skill_id'),
                skill_version_id=skill.get('skill_version_id'),
                skill_name=skill.get('name'),
            )


def get_available_skills_for_agent(
    project_id: int,
    entity_version_id: int,
    entity_type: str = SkillEntityTypes.agent,
    session=None,
) -> List[dict]:
    with _skill_session(session, project_id) as s:
        mappings = s.query(EntitySkillMapping).filter(
            EntitySkillMapping.entity_version_id == entity_version_id,
            EntitySkillMapping.entity_type == entity_type,
        ).all()

        skills = []
        for mapping in mappings:
            skill = mapping.skill
            version = mapping.skill_version

            if skill:
                skills.append({
                    'name': skill.name,
                    'description': skill.description,
                    'skill_id': skill.id,
                    'version_id': mapping.skill_version_id,
                    'version_name': version.name if version else 'unknown',
                    'version_missing': version is None,
                })

        return skills


_NAME_BOUNDARY_CHARS = re.compile(r'[0-9A-Za-z\-]')


def parse_invoked_skill_names(user_input: Optional[str], attached_names: List[str]) -> List[str]:
    """Parse ``~<skill-name>`` tokens from the raw user message."""
    if not user_input or not attached_names:
        return []

    lower_to_original: dict = {}
    for name in attached_names:
        if not name:
            continue
        key = name.lower()
        if key not in lower_to_original:
            lower_to_original[key] = name
    if not lower_to_original:
        return []

    # Longest name first so prefix-overlap resolves to the longer match.
    candidates = sorted(lower_to_original.keys(), key=len, reverse=True)

    text = user_input
    lower_text = text.lower()
    ordered: List[str] = []
    seen: set = set()

    i = 0
    n = len(lower_text)
    while i < n:
        ch = lower_text[i]
        if ch != '~':
            i += 1
            continue

        start = i + 1
        matched_key = None
        for cand in candidates:
            end = start + len(cand)
            if end > n:
                continue
            if lower_text[start:end] != cand:
                continue
            # Require a word/punctuation boundary AFTER the match: the next char
            # must not be a name char (so ``~code`` won't match ``code-review``,
            # and ``~pirate-talkative`` won't match ``pirate-talk``).
            if end < n and _NAME_BOUNDARY_CHARS.match(lower_text[end]):
                continue
            matched_key = cand
            break

        if matched_key is not None:
            original = lower_to_original[matched_key]
            if matched_key not in seen:
                seen.add(matched_key)
                ordered.append(original)
                if len(ordered) >= MAX_SKILLS_PER_AGENT:
                    break
            # Advance past the matched name (next ``~`` scan resumes after it).
            i = start + len(matched_key)
        else:
            # Lone ``~`` (prose/code/path/email) — advance one char and keep scanning.
            i += 1

    return ordered


def build_invoked_skills(
    user_input: Optional[str],
    attached_skills: List[dict],
) -> List[dict]:
    """Build the per-turn ``invoked_skills`` payload."""
    if not user_input or not attached_skills:
        return []

    by_name: dict = {}
    for skill in attached_skills:
        name = skill.get('name')
        if name and name.lower() not in by_name:
            by_name[name.lower()] = skill

    matched_names = parse_invoked_skill_names(user_input, [s.get('name') for s in attached_skills])

    invoked: List[dict] = []
    for name in matched_names:
        skill = by_name.get(name.lower())
        if not skill:
            continue
        instructions = skill.get('instructions')
        if not instructions or not instructions.strip():
            # Defensive: validate_agent_skills already blocks deleted/empty bodies.
            continue
        invoked.append({
            'skill_id': skill.get('skill_id'),
            'skill_version_id': skill.get('skill_version_id'),
            'name': skill.get('name'),
            'version_name': skill.get('version_name'),
            'instructions': instructions,
        })
    return invoked


def _apply_tags_to_version(session, version: SkillVersion, tags: List) -> None:
    """Apply tags to a skill version."""
    if not tags:
        return

    existing_tags = session.query(Tag).filter(
        Tag.name.in_({t.name for t in tags})
    ).all()
    existing_tags_map = {t.name: t for t in existing_tags}

    for tag in tags:
        tag_obj = existing_tags_map.get(tag.name)
        if not tag_obj:
            tag_obj = Tag(name=tag.name)
            session.add(tag_obj)
        version.tags.append(tag_obj)

    # Flush so newly-created tags receive their auto-increment ids before the
    # version is serialized (SkillVersionDetailModel.tags requires int ids).
    session.flush()


def _update_version_fields(session, version: SkillVersion, update_data: SkillVersionUpdateModel) -> None:
    """Apply field updates from ``update_data`` onto ``version`` in place."""
    if update_data.name is not None:
        version.name = update_data.name
    if update_data.instructions is not None:
        version.instructions = update_data.instructions
    if update_data.meta is not None:
        version.meta = {**(version.meta or {}), **update_data.meta}

    # Handle tags
    if update_data.tags is not None:
        # Clear existing tags
        version.tags.clear()
        session.flush()

        # Add new tags
        _apply_tags_to_version(session, version, update_data.tags)
