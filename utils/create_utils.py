from tools import serialize, store_secrets

from ..models.all import ApplicationVersion, Application, ApplicationVariable, EliteATool, EntityToolMapping
from ..models.pd.application import (
    ApplicationCreateModel, ApplicationImportModel,
)
from ..models.pd.version import ApplicationVersionCreateModel, ApplicationVersionBaseCreateModel, TagBaseModel
from typing import Generator, List

from ..models.all import Tag


def get_existing_tags(
        tags: List[TagBaseModel],
        session=None,
        project_id: int | None = None
) -> dict[str, Tag]:
    assert session or project_id, 'session or project_id is required'
    if not session and project_id:
        from tools import db
        with db.with_project_schema_session(project_id) as project_session:
            existing_tags: List[Tag] = project_session.query(Tag).filter(
                Tag.name.in_({i.name for i in tags})
            ).all()
    else:
        existing_tags: List[Tag] = session.query(Tag).filter(
            Tag.name.in_({i.name for i in tags})
        ).all()
    return {i.name: i for i in existing_tags}


def generate_tags(
        tags: List[TagBaseModel],
        existing_tags_map: dict[str, Tag]
) -> Generator[Tag, None, None]:
    for i in tags:
        yield existing_tags_map.get(i.name, Tag(**i.dict()))


def create_version(
        version_data: ApplicationVersionCreateModel | ApplicationVersionBaseCreateModel,
        application: Application | None = None,
        session=None
) -> ApplicationVersion:
    application_version = ApplicationVersion(**version_data.dict(
        exclude_unset=True,
        exclude={'tags', 'variables', 'tools'}
    ))
    # session.add(application_version)

    if application:
        application_version.application = application

    if version_data.tags:
        project_id = None
        if application:
            project_id = application.owner_id
        existing_tags_map = get_existing_tags(version_data.tags, session=session, project_id=project_id)
        application_version.tags = list(generate_tags(
            version_data.tags,
            existing_tags_map=existing_tags_map
        ))

    # application_version_id = application_version.id

    if version_data.variables:
        for var in version_data.variables:
            application_var = ApplicationVariable(
                **var.dict(exclude_unset=True)
            )
            application_var.application_version = application_version
            # session.add(application_var)

    session.add(application_version)
    session.flush()

    # do not allow to create toolkits alongside with version
    # todo: comment this piece of code
    if version_data.tools:
        for tool in version_data.tools:
            from tools import serialize, store_secrets
            from ..models.enums.all import ToolEntityTypes
            from ..utils.application_tools import wrap_provider_hub_secret_fields

            project_id: int = application.owner_id
            wrap_provider_hub_secret_fields(tool.type, tool.settings, project_id)
            store_secrets(tool.dict(), project_id)

            tool.fix_name(project_id)
            if tool.id:
                application_tool_id = tool.id
            else:
                application_tool = EliteATool(
                    **serialize(tool),
                )
                session.add(application_tool)
                session.flush()
                application_tool_id = application_tool.id
            # Extract selected_tools from tool settings if present
            tool_settings = tool.settings if isinstance(tool.settings, dict) else {}
            selected_tools = tool_settings.get('selected_tools')

            application_tool_to_application = EntityToolMapping(
                tool_id=application_tool_id,
                entity_version_id=application_version.id,
                entity_id=application.id,
                entity_type=ToolEntityTypes.agent,
                selected_tools=selected_tools
            )
            session.add(application_tool_to_application)
            session.flush()
    return application_version


def create_application(application_data: ApplicationCreateModel | ApplicationImportModel, session, project_id: int) -> Application:
    app_create_data = application_data.dict(exclude_unset=True, exclude={"versions"})
    store_secrets(app_create_data, project_id)

    application = Application(
        **serialize(app_create_data)
    )

    for ver in application_data.versions:
        create_version(ver, application=application, session=session)
    session.add(application)
    session.flush()  # Flush to get version IDs
    
    # Set the first version (base) as the default version
    if application.versions:
        application.meta = {'default_version_id': application.versions[0].id}
    
    return application
