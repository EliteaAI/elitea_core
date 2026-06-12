from typing import List, Optional, Tuple, Literal

from sqlalchemy import func, or_, asc, desc
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import IntegrityError

from tools import db, auth, serialize
from pylon.core.tools import log

from ..models.skill import Skill, SkillVersion, EntitySkillMapping
from ..models.all import Tag
from ..models.enums.all import SkillEntityTypes
from ..models.pd.skill import (
    SkillCreateModel,
    SkillDetailModel,
    SkillUpdateModel,
)
from ..models.pd.skill_version import (
    SkillVersionCreateModel,
    SkillVersionUpdateModel,
    SkillVersionDetailModel,
)


MAX_SKILLS_PER_AGENT = 5


class SkillNotFoundError(Exception):
    def __init__(self, skill_id: int):
        super().__init__(f"Skill with id {skill_id} not found")
        self.skill_id = skill_id


class SkillVersionNotFoundError(Exception):
    def __init__(self, skill_id: int, version_id: int = None, version_name: str = None):
        if version_id:
            msg = f"Skill version with id {version_id} not found for skill {skill_id}"
        else:
            msg = f"Skill version '{version_name}' not found for skill {skill_id}"
        super().__init__(msg)
        self.skill_id = skill_id
        self.version_id = version_id
        self.version_name = version_name


class SkillVersionInUseError(Exception):
    def __init__(self, version_id: int, usage_count: int):
        super().__init__(
            f"Skill version {version_id} is attached to {usage_count} agent(s). "
            "Detach it from all agents before deleting."
        )
        self.version_id = version_id
        self.usage_count = usage_count


class SkillLimitExceededError(Exception):
    def __init__(self, entity_version_id: int, current_count: int):
        super().__init__(
            f"Agent version {entity_version_id} already has {current_count} skills attached. "
            f"Maximum allowed is {MAX_SKILLS_PER_AGENT}."
        )
        self.entity_version_id = entity_version_id
        self.current_count = current_count


class SkillAlreadyAttachedError(Exception):
    def __init__(self, skill_id: int, entity_version_id: int):
        super().__init__(
            f"Skill {skill_id} is already attached to agent version {entity_version_id}"
        )
        self.skill_id = skill_id
        self.entity_version_id = entity_version_id


class SkillResolutionError(Exception):
    """Raised when a skill cannot be resolved for chat invocation."""
    def __init__(self, skill_name: str, reason: str):
        super().__init__(f"Cannot invoke skill '~{skill_name}': {reason}")
        self.skill_name = skill_name
        self.reason = reason


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

    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        # Count query
        count_query = session.query(func.count(Skill.id))
        if filters:
            count_query = count_query.filter(*filters)
        total = count_query.scalar() or 0

        if total == 0:
            return 0, []

        # Main query with eager loading
        query = session.query(Skill).options(
            selectinload(Skill.versions).selectinload(SkillVersion.tags)
        )

        if filters:
            query = query.filter(*filters)

        # Sorting
        sort_column = getattr(Skill, sort_by, Skill.created_at)
        if sort_order == 'desc':
            query = query.order_by(desc(sort_column))
        else:
            query = query.order_by(asc(sort_column))

        # Pagination
        if limit:
            query = query.limit(limit)
        if offset:
            query = query.offset(offset)

        skills = query.all()
        return total, skills

    finally:
        if owns_session:
            session.close()


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
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        skill = session.query(Skill).filter(
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

    finally:
        if owns_session:
            session.close()


def create_skill(
    skill_data: SkillCreateModel,
    session,
    project_id: int,
) -> Skill:
    """
    Create a new skill with its initial version.

    Args:
        skill_data: Validated skill creation data
        session: Database session
        project_id: Project ID

    Returns:
        Created Skill object
    """
    # Create skill
    skill = Skill(
        name=skill_data.name,
        description=skill_data.description,
        owner_id=skill_data.owner_id,
        author_id=skill_data.versions[0].author_id if skill_data.versions else auth.current_user().get('id'),
        meta=skill_data.meta or {},
    )
    session.add(skill)
    session.flush()  # Get skill.id

    # Create initial version
    for version_data in skill_data.versions:
        version = SkillVersion(
            skill_id=skill.id,
            name=version_data.name,
            instructions=version_data.instructions,
            author_id=version_data.author_id if hasattr(version_data, 'author_id') else skill.author_id,
            meta=version_data.meta or {},
        )
        session.add(version)

        # Handle tags
        if version_data.tags:
            _apply_tags_to_version(session, version, version_data.tags)

    session.flush()
    return skill


def update_skill(
    project_id: int,
    skill_id: int,
    update_data: SkillUpdateModel,
    session=None,
) -> dict:
    """
    Update skill metadata and optionally version content.

    Args:
        project_id: Project ID
        skill_id: Skill ID to update
        update_data: Validated update data
        session: Database session

    Returns:
        Dictionary with 'ok' and 'data' or 'msg' keys
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        skill = session.query(Skill).filter(
            Skill.id == skill_id
        ).options(
            selectinload(Skill.versions)
        ).first()

        if not skill:
            return {'ok': False, 'msg': f'Skill with id {skill_id} not found'}

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
                result = _update_version_fields(session, version, update_data.version)
                if not result.get('ok', True):
                    return result

        session.commit()
        session.refresh(skill)

        result = SkillDetailModel.model_validate(skill)
        return {'ok': True, 'data': serialize(result)}

    except Exception as e:
        if owns_session:
            session.rollback()
        log.error(f"Error updating skill {skill_id}: {e}")
        return {'ok': False, 'msg': str(e)}
    finally:
        if owns_session:
            session.close()


def delete_skill(
    project_id: int,
    skill_id: int,
    session=None,
) -> dict:
    """
    Delete a skill and all its versions (cascades to agent attachments).

    Args:
        project_id: Project ID
        skill_id: Skill ID to delete
        session: Database session

    Returns:
        Dictionary with 'ok' key and optional 'msg' for errors
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        skill = session.query(Skill).filter(
            Skill.id == skill_id
        ).first()

        if not skill:
            return {'ok': False, 'msg': f'Skill with id {skill_id} not found'}

        session.delete(skill)
        session.commit()

        return {'ok': True}

    except Exception as e:
        if owns_session:
            session.rollback()
        log.error(f"Error deleting skill {skill_id}: {e}")
        return {'ok': False, 'msg': str(e)}
    finally:
        if owns_session:
            session.close()


def create_skill_version(
    project_id: int,
    skill_id: int,
    version_data: SkillVersionCreateModel,
    session=None,
) -> dict:
    """
    Create a new version for an existing skill.

    Args:
        project_id: Project ID
        skill_id: Skill ID to add version to
        version_data: Validated version data
        session: Database session

    Returns:
        Dictionary with 'ok' and 'data' or 'msg' keys
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        # Verify skill exists
        skill = session.query(Skill).filter(Skill.id == skill_id).first()
        if not skill:
            return {'ok': False, 'msg': f'Skill with id {skill_id} not found'}

        # Create version
        version = SkillVersion(
            skill_id=skill_id,
            name=version_data.name,
            instructions=version_data.instructions,
            author_id=version_data.author_id if hasattr(version_data, 'author_id') else auth.current_user().get('id'),
            meta=version_data.meta or {},
        )
        session.add(version)

        # Handle tags
        if version_data.tags:
            session.flush()  # Get version.id
            _apply_tags_to_version(session, version, version_data.tags)

        session.commit()
        session.refresh(version)

        result = SkillVersionDetailModel.model_validate(version)
        return {'ok': True, 'data': serialize(result)}

    except IntegrityError as e:
        if owns_session:
            session.rollback()
        if 'unique' in str(e).lower():
            return {'ok': False, 'msg': f'Version name "{version_data.name}" already exists for this skill'}
        return {'ok': False, 'msg': str(e)}
    except Exception as e:
        if owns_session:
            session.rollback()
        log.error(f"Error creating skill version: {e}")
        return {'ok': False, 'msg': str(e)}
    finally:
        if owns_session:
            session.close()


def update_skill_version(
    project_id: int,
    skill_id: int,
    version_id: int,
    update_data: SkillVersionUpdateModel,
    session=None,
) -> dict:
    """
    Update an existing skill version.

    Args:
        project_id: Project ID
        skill_id: Skill ID
        version_id: Version ID to update
        update_data: Validated update data
        session: Database session

    Returns:
        Dictionary with 'ok' and 'data' or 'msg' keys
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        version = session.query(SkillVersion).filter(
            SkillVersion.id == version_id,
            SkillVersion.skill_id == skill_id,
        ).options(
            selectinload(SkillVersion.tags)
        ).first()

        if not version:
            return {'ok': False, 'msg': f'Version {version_id} not found for skill {skill_id}'}

        # Prevent renaming 'base' version
        if version.name == 'base' and update_data.name and update_data.name != 'base':
            return {'ok': False, 'msg': 'Cannot rename the base version'}

        result = _update_version_fields(session, version, update_data)
        if not result.get('ok', True):
            return result

        session.commit()
        session.refresh(version)

        result = SkillVersionDetailModel.model_validate(version)
        return {'ok': True, 'data': serialize(result)}

    except IntegrityError as e:
        if owns_session:
            session.rollback()
        if 'unique' in str(e).lower():
            return {'ok': False, 'msg': f'Version name "{update_data.name}" already exists for this skill'}
        return {'ok': False, 'msg': str(e)}
    except Exception as e:
        if owns_session:
            session.rollback()
        log.error(f"Error updating skill version {version_id}: {e}")
        return {'ok': False, 'msg': str(e)}
    finally:
        if owns_session:
            session.close()


def delete_skill_version(
    project_id: int,
    skill_id: int,
    version_id: int,
    session=None,
) -> dict:
    """
    Delete a skill version (validates not in use by agents).

    Args:
        project_id: Project ID
        skill_id: Skill ID
        version_id: Version ID to delete
        session: Database session

    Returns:
        Dictionary with 'ok' key and optional 'msg' for errors
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        version = session.query(SkillVersion).filter(
            SkillVersion.id == version_id,
            SkillVersion.skill_id == skill_id,
        ).first()

        if not version:
            return {'ok': False, 'msg': f'Version {version_id} not found for skill {skill_id}'}

        # Prevent deleting 'base' version if it's the only one
        other_versions = session.query(SkillVersion).filter(
            SkillVersion.skill_id == skill_id,
            SkillVersion.id != version_id,
        ).count()

        if version.name == 'base' and other_versions == 0:
            return {'ok': False, 'msg': 'Cannot delete the only version of a skill. Delete the skill instead.'}

        # Check if version is in use by any agents
        usage_count = session.query(EntitySkillMapping).filter(
            EntitySkillMapping.skill_version_id == version_id
        ).count()

        if usage_count > 0:
            return {
                'ok': False,
                'msg': f'Version is attached to {usage_count} agent(s). Detach it from all agents before deleting.',
                'usage_count': usage_count
            }

        session.delete(version)
        session.commit()

        return {'ok': True}

    except Exception as e:
        if owns_session:
            session.rollback()
        log.error(f"Error deleting skill version {version_id}: {e}")
        return {'ok': False, 'msg': str(e)}
    finally:
        if owns_session:
            session.close()


def get_skill_version_by_name(
    project_id: int,
    skill_id: int,
    version_name: str,
    session=None,
) -> Optional[SkillVersion]:
    """
    Get a skill version by name.

    Args:
        project_id: Project ID
        skill_id: Skill ID
        version_name: Version name to look up
        session: Database session

    Returns:
        SkillVersion object or None if not found
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        return session.query(SkillVersion).filter(
            SkillVersion.skill_id == skill_id,
            SkillVersion.name == version_name,
        ).first()
    finally:
        if owns_session:
            session.close()


def validate_agent_skill_limit(
    session,
    entity_version_id: int,
    entity_type: str = SkillEntityTypes.agent,
) -> bool:
    """
    Validate that an agent hasn't exceeded the maximum skill limit.

    Args:
        session: Database session
        entity_version_id: Application version ID
        entity_type: Entity type (default: 'agent')

    Returns:
        True if under limit

    Raises:
        SkillLimitExceededError if limit exceeded
    """
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
    """
    Attach a skill to an agent version.

    Args:
        project_id: Project ID
        entity_version_id: Application version ID
        skill_id: Skill ID to attach
        skill_version_id: Specific version to attach
        entity_type: Entity type (default: 'agent')
        session: Database session

    Returns:
        Dictionary with 'ok' key and optional 'msg' or 'data'
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        # Validate skill limit
        try:
            validate_agent_skill_limit(session, entity_version_id, entity_type)
        except SkillLimitExceededError as e:
            return {'ok': False, 'msg': str(e)}

        # Verify skill exists
        skill = session.query(Skill).filter(Skill.id == skill_id).first()
        if not skill:
            return {'ok': False, 'msg': f'Skill with id {skill_id} not found'}

        # Verify version exists and belongs to skill
        version = session.query(SkillVersion).filter(
            SkillVersion.id == skill_version_id,
            SkillVersion.skill_id == skill_id,
        ).first()
        if not version:
            return {'ok': False, 'msg': f'Version {skill_version_id} not found for skill {skill_id}'}

        # Create mapping
        mapping = EntitySkillMapping(
            entity_version_id=entity_version_id,
            entity_type=entity_type,
            skill_id=skill_id,
            skill_version_id=skill_version_id,
        )
        session.add(mapping)
        session.commit()

        return {
            'ok': True,
            'data': {
                'skill_id': skill_id,
                'skill_version_id': skill_version_id,
                'skill_name': skill.name,
                'version_name': version.name,
            }
        }

    except IntegrityError as e:
        if owns_session:
            session.rollback()
        if 'unique' in str(e).lower():
            return {'ok': False, 'msg': f'Skill {skill_id} is already attached to this agent'}
        return {'ok': False, 'msg': str(e)}
    except Exception as e:
        if owns_session:
            session.rollback()
        log.error(f"Error attaching skill {skill_id} to agent: {e}")
        return {'ok': False, 'msg': str(e)}
    finally:
        if owns_session:
            session.close()


def detach_skill_from_agent(
    project_id: int,
    entity_version_id: int,
    skill_id: int,
    entity_type: str = SkillEntityTypes.agent,
    session=None,
) -> dict:
    """
    Detach a skill from an agent version.

    Args:
        project_id: Project ID
        entity_version_id: Application version ID
        skill_id: Skill ID to detach
        entity_type: Entity type (default: 'agent')
        session: Database session

    Returns:
        Dictionary with 'ok' key and optional 'msg'
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        mapping = session.query(EntitySkillMapping).filter(
            EntitySkillMapping.entity_version_id == entity_version_id,
            EntitySkillMapping.entity_type == entity_type,
            EntitySkillMapping.skill_id == skill_id,
        ).first()

        if not mapping:
            return {'ok': False, 'msg': 'Skill not attached to this agent'}

        session.delete(mapping)
        session.commit()

        return {'ok': True}

    except Exception as e:
        if owns_session:
            session.rollback()
        log.error(f"Error detaching skill {skill_id} from agent: {e}")
        return {'ok': False, 'msg': str(e)}
    finally:
        if owns_session:
            session.close()


def validate_agent_skills(
    project_id: int,
    entity_version_id: int,
    entity_type: str = SkillEntityTypes.agent,
    session=None,
) -> dict:
    """
    Validate all skills attached to an agent before runtime.

    Performs pre-flight validation to ensure all attached skill versions exist.
    This is a blocking validation - if any skill version is missing, the agent
    cannot run.

    Args:
        project_id: Project ID
        entity_version_id: Application version ID
        entity_type: Entity type (default: 'agent')
        session: Database session

    Returns:
        Dictionary with 'ok' key and 'errors' list if validation fails
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        mappings = session.query(EntitySkillMapping).filter(
            EntitySkillMapping.entity_version_id == entity_version_id,
            EntitySkillMapping.entity_type == entity_type,
        ).all()

        errors = []
        for mapping in mappings:
            # Check version exists
            version = session.query(SkillVersion).filter(
                SkillVersion.id == mapping.skill_version_id
            ).first()

            if not version:
                skill = session.query(Skill).filter(
                    Skill.id == mapping.skill_id
                ).first()
                skill_name = skill.name if skill else f'Skill {mapping.skill_id}'
                errors.append({
                    'skill_id': mapping.skill_id,
                    'skill_name': skill_name,
                    'version_id': mapping.skill_version_id,
                    'error': 'Attached version no longer exists',
                })

        if errors:
            return {'ok': False, 'errors': errors}

        return {'ok': True}

    finally:
        if owns_session:
            session.close()


def get_skill_for_agent(
    project_id: int,
    entity_version_id: int,
    skill_name: str,
    entity_type: str = SkillEntityTypes.agent,
    session=None,
) -> dict:
    """
    Get a skill for runtime invocation by name.

    This is the runtime fetch function called when a user types ~skill-name.
    It does NOT fall back to 'base' version - if the attached version is missing,
    it raises an error.

    Args:
        project_id: Project ID
        entity_version_id: Application version ID
        skill_name: Skill name to look up
        entity_type: Entity type (default: 'agent')
        session: Database session

    Returns:
        Dictionary with skill data including 'instructions'

    Raises:
        SkillResolutionError if skill cannot be resolved
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        # Find skill by name
        skill = session.query(Skill).filter(
            Skill.name == skill_name
        ).first()

        if not skill:
            raise SkillResolutionError(skill_name, "Skill not found in this project")

        # Find mapping for this agent
        mapping = session.query(EntitySkillMapping).filter(
            EntitySkillMapping.entity_version_id == entity_version_id,
            EntitySkillMapping.entity_type == entity_type,
            EntitySkillMapping.skill_id == skill.id,
        ).first()

        if not mapping:
            raise SkillResolutionError(skill_name, "Skill is not attached to this agent")

        # Get the specific attached version (NO FALLBACK to base)
        version = session.query(SkillVersion).filter(
            SkillVersion.id == mapping.skill_version_id
        ).first()

        if not version:
            raise SkillResolutionError(
                skill_name,
                f"Attached version (id={mapping.skill_version_id}) no longer exists. "
                "Please re-attach the skill with a valid version."
            )

        return {
            'skill_id': skill.id,
            'skill_name': skill.name,
            'version_id': version.id,
            'version_name': version.name,
            'instructions': version.instructions,
            'description': skill.description,
        }

    finally:
        if owns_session:
            session.close()


def get_available_skills_for_agent(
    project_id: int,
    entity_version_id: int,
    entity_type: str = SkillEntityTypes.agent,
    session=None,
) -> List[dict]:
    """
    Get list of skills attached to an agent for UI autocomplete.

    Called when user types `~` in chat to show available skills.

    Args:
        project_id: Project ID
        entity_version_id: Application version ID
        entity_type: Entity type (default: 'agent')
        session: Database session

    Returns:
        List of skill dictionaries for autocomplete dropdown
    """
    owns_session = session is None
    if owns_session:
        session = db.get_session(project_id).__enter__()

    try:
        mappings = session.query(EntitySkillMapping).filter(
            EntitySkillMapping.entity_version_id == entity_version_id,
            EntitySkillMapping.entity_type == entity_type,
        ).all()

        skills = []
        for mapping in mappings:
            skill = session.query(Skill).filter(
                Skill.id == mapping.skill_id
            ).first()

            version = session.query(SkillVersion).filter(
                SkillVersion.id == mapping.skill_version_id
            ).first()

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

    finally:
        if owns_session:
            session.close()


# =============================================================================
# Helper Functions
# =============================================================================

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


def _update_version_fields(session, version: SkillVersion, update_data: SkillVersionUpdateModel) -> dict:
    """Update version fields from update data."""
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

    return {'ok': True}
