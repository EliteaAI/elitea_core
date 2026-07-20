from typing import List, Optional

from sqlalchemy import and_

from pylon.core.tools import web, log

from tools import db, serialize

from ..models.skill import Skill, SkillVersion, EntitySkillMapping
from ..models.enums.all import PublishStatus, SkillEntityTypes
from ..models.pd.skill import SkillCreateModel
from ..models.pd.search import MultipleApplicationSearchModel
from ..utils.searches import get_search_options
from ..utils.skill_utils import (
    get_skill_details,
    create_skill,
    build_skill_detail,
    attach_skill_to_agent,
    detach_skill_from_agent,
    get_available_skills_for_agent,
    import_skill,
)
from ..utils.skill_export_import import ensure_base_version
from ..utils.skill_publish_utils import get_default_skill_validation_rules


class RPC:
    @web.rpc(
        "skills_get_default_publish_validation_rules",
        "skills_get_default_publish_validation_rules",
    )
    def skills_get_default_publish_validation_rules(self, **kwargs) -> str:
        return get_default_skill_validation_rules()

    @web.rpc("skills_get_search_options", "skills_get_search_options")
    def skills_get_search_options(self, project_id: int, **kwargs) -> dict:
        return get_search_options(
            project_id,
            Model=Skill,
            PDModel=MultipleApplicationSearchModel,
            joinedload_=None,
            args_prefix='skill',
            filters=[],
            search_fields=('name', 'description'),
        )

    @web.rpc("applications_import_skill", "applications_import_skill")
    def applications_import_skill(self, model_data: dict, project_id: int, author_id: int):
        # ensure an imported skill has a 'base' version (additive). See ensure_base_version.
        versions = ensure_base_version(model_data.get('versions') or [])
        try:
            imported = import_skill(
                project_id=project_id,
                name=model_data['name'],
                description=model_data.get('description') or model_data['name'],
                versions=versions,
                author_id=author_id,
            )
        except Exception as ex:
            log.error(f"[IMPORT] Failed to import skill '{model_data.get('name')}': {ex}")
            return None, [f"Skill import failed: {ex}"]

        result = {
            'id': imported.id,
            'name': model_data['name'],
            'reused': imported.reused,
            'versions': imported.versions,
        }
        return result, []

    @web.rpc("skills_get_skill_by_id", "get_skill_by_id")
    def skills_get_skill_by_id(
        self,
        project_id: int,
        skill_id: int,
        version_name: str = None,
        version_id: int = None,
    ) -> Optional[dict]:
        result = get_skill_details(
            project_id=project_id,
            skill_id=skill_id,
            version_name=version_name,
            version_id=version_id,
        )
        return result.get('data')

    @web.rpc("skills_create_skill", "create_skill")
    def skills_create_skill(
        self,
        skill_data: dict,
        project_id: int,
        author_id: int,
    ) -> dict:
        raw = dict(skill_data)
        raw['owner_id'] = project_id
        raw['project_id'] = project_id
        raw['user_id'] = author_id
        for version in raw.get('versions', []):
            version['author_id'] = author_id

        validated = SkillCreateModel.model_validate(raw)

        with db.get_session(project_id) as session:
            skill = create_skill(validated, session, project_id)
            session.commit()
            session.refresh(skill)

            return serialize(build_skill_detail(skill))

    @web.rpc("skills_get_skills_for_agent", "get_skills_for_agent")
    def skills_get_skills_for_agent(
        self,
        project_id: int,
        entity_version_id: int,
        entity_type: str = SkillEntityTypes.agent,
    ) -> List[dict]:
        with db.get_session(project_id) as session:
            mappings = session.query(EntitySkillMapping).filter(
                EntitySkillMapping.entity_version_id == entity_version_id,
                EntitySkillMapping.entity_type == entity_type,
            ).all()

            skills = []
            for mapping in mappings:
                version = session.query(SkillVersion).filter(
                    SkillVersion.id == mapping.skill_version_id
                ).first()
                if not version:
                    continue

                skill = session.query(Skill).filter(
                    Skill.id == mapping.skill_id
                ).first()

                skills.append({
                    'skill_id': mapping.skill_id,
                    'name': skill.name if skill else 'Unknown',
                    'description': skill.description if skill else None,
                    'version_id': version.id,
                    'version_name': version.name,
                    'instructions': version.instructions,
                })

            return skills

    @web.rpc("skills_attach_to_agent", "attach_skill_to_agent")
    def skills_attach_to_agent(
        self,
        project_id: int,
        entity_version_id: int,
        skill_id: int,
        skill_version_id: int,
        entity_type: str = SkillEntityTypes.agent,
    ) -> dict:
        return attach_skill_to_agent(
            project_id=project_id,
            entity_version_id=entity_version_id,
            skill_id=skill_id,
            skill_version_id=skill_version_id,
            entity_type=entity_type,
        )

    @web.rpc("skills_detach_from_agent", "detach_skill_from_agent")
    def skills_detach_from_agent(
        self,
        project_id: int,
        entity_version_id: int,
        skill_id: int,
        entity_type: str = SkillEntityTypes.agent,
    ) -> dict:
        detach_skill_from_agent(
            project_id=project_id,
            entity_version_id=entity_version_id,
            skill_id=skill_id,
            entity_type=entity_type,
        )
        return True

    @web.rpc("skills_get_available_for_agent", "get_available_skills_for_agent")
    def skills_get_available_for_agent(
        self,
        project_id: int,
        entity_version_id: int,
        entity_type: str = SkillEntityTypes.agent,
    ) -> List[dict]:
        return get_available_skills_for_agent(
            project_id=project_id,
            entity_version_id=entity_version_id,
            entity_type=entity_type,
        )

    @web.rpc("skills_get_stats", "get_skills_stats")
    def skills_get_stats(self, project_id: int, author_id: int) -> dict:
        result = {}
        with db.with_project_schema_session(project_id) as session:
            result['total_skills'] = session.query(Skill).filter(
                Skill.versions.any(SkillVersion.author_id == author_id)
            ).count()
            # Both conditions must hold on the SAME version: two separate any()
            # clauses would count a skill whose published version belongs to
            # someone else.
            result['public_skills'] = session.query(Skill).filter(
                Skill.versions.any(and_(
                    SkillVersion.author_id == author_id,
                    SkillVersion.status == PublishStatus.published,
                ))
            ).count()

        return result
