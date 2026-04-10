import json
from copy import deepcopy
from typing import Optional, Tuple, List
from uuid import uuid4

from dateutil import parser
from pydantic import ValidationError
from pylon.core.tools import web, log
from sqlalchemy import Integer, and_
from sqlalchemy.orm import joinedload, selectinload
from tools import db, serialize, auth, store_secrets, VaultClient, this

from ..models.elitea_tools import EliteATool, EntityToolMapping
from ..models.all import Application, ApplicationVersion, ApplicationVersionTagAssociation
from ..models.enums.all import AgentTypes
from ..models.enums.events import ApplicationEvents
from ..models.message_items.attachment import AttachmentMessageItem
from ..models.pd.application import ApplicationDetailModel, ApplicationImportModel
from ..utils.attachment_utils import extract_as_bytes_from_base64_content
from ..models.pd.chat import ApplicationChatRequest, LLMChatRequest
from ..models.pd.search import MultipleApplicationSearchModel
from ..models.pd.tool import ToolDetails, ToolImportModel, ToolValidatedDetails
from ..models.pd.version import ApplicationVersionDetailModel, ApplicationVersionCreateModel
from ..utils.application_tools import (
    toolkits_listing,
    expand_toolkit_settings,
    find_toolkit_schema_by_type_everywhere,
    start_index_task,
    wrap_provider_hub_secret_fields,
)
from ..utils.application_utils import (
    get_application_details,
    get_application_version_details_expanded,
    ApplicationVersionNonFoundError,
    ApplicationToolExpandedError
)
from ..utils.create_utils import create_application, create_version
from ..utils.export_import import export_application
from ..utils.predict_utils import generate_predict_payload, PredictPayloadError, get_predict_base_url, \
    get_predict_token_and_session, load_context_settings_from_conversation
from ..utils.application_utils_general import deep_update
from ..models.enums.all import PublishStatus, ToolEntityTypes
from ..utils.searches import get_search_options_one_entity
from ..utils.sio_utils import SioValidationError, get_event_room, SioEvents
from ..utils.tracing_utils import add_trace_context_to_meta
from ..utils.chat_feature_flags import get_context_manager_feature_flag


class RPC:
    @web.rpc("applications_get_application_by_id", "get_application_by_id")
    def get_application_by_id(self, project_id: int, application_id: int,
                              version_name: str = None, first_existing_version: bool = False, **kwargs) -> Optional[
        dict]:
        # Note: version_name=None uses the default version or first existing version as fallback
        # Deprecated: version_name='latest' was removed - versions are now named 'base'
        return get_application_details(
            project_id,
            application_id,
            version_name,
            first_existing_version=first_existing_version
        ).get('data')

    @web.rpc("applications_get_application_by_version_id", "get_application_by_version_id")
    def get_application_by_version_id(self, project_id: int, application_id: int, version_id: int = None) -> Optional[
        dict]:

        if version_id is None:
            # Use default version (version_name=None uses first existing version)
            version_name = None
        else:
            with db.with_project_schema_session(project_id) as session:
                app_version: ApplicationVersion = session.query(ApplicationVersion).filter(
                    ApplicationVersion.id == version_id
                ).one_or_none()

                if app_version is None:
                    return

                version_name = app_version.name

        return self.get_application_by_id(
            project_id=project_id,
            application_id=application_id,
            version_name=version_name
        )

    @web.rpc("applications_predict_sio", "predict_sio")
    def predict_sio(self,
                    sid: str | None,
                    data: dict,
                    sio_event: str = SioEvents.application_predict,
                    start_event_content: Optional[dict] = None,
                    chat_project_id: Optional[int] = None,
                    await_task_timeout: int = -1,
                    user_id: int = None,
                    is_system_user: bool = False
                    ) -> dict:
        if start_event_content is None:
            start_event_content = {}
        data['message_id'] = data.get('message_id', str(uuid4()))
        data['stream_id'] = data.get('stream_id', data['message_id'])
        try:
            parsed = ApplicationChatRequest.model_validate(data)
        except ValidationError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=sio_event,
                error=e.errors(),
                stream_id=data.get("stream_id"),
                message_id=data.get("message_id")
            )

        if sid and not auth.is_sio_user_in_project(sid, parsed.project_id):
            log.warning("Sid %s is not in project %s", sid, parsed.project_id)
            return  # FIXME: need some proper error?

        if parsed.version_id:
            with db.get_session(parsed.project_id) as session:
                application_version: ApplicationVersion = session.query(ApplicationVersion).get(parsed.version_id)
                if not application_version:
                    raise SioValidationError(
                        sio=self.context.sio,
                        sid=sid,
                        event=sio_event,
                        error={'error': f"Application version with id '{parsed.version_id}' not found"},
                        stream_id=data.get("stream_id"),
                        message_id=data.get("message_id")
                    )

                if not application_version.llm_settings:  # TODO: Probably wrong check
                    raise SioValidationError(
                        sio=self.context.sio,
                        sid=sid,
                        event=sio_event,
                        error={'error': f"Application version with id '{parsed.version_id}' "
                                    f"was not found its settings, please provide it in request"},
                        stream_id=data.get("stream_id"),
                        message_id=data.get("message_id")
                    )

                application_version.project_id = parsed.project_id  # compatibility with pd model
                parsed_db = ApplicationChatRequest.from_orm(
                    application_version
                )

            parsed: ApplicationChatRequest = parsed_db.merge_update(parsed)

        # TODO: fragile code: app itself and toolkits may be in different projects
        # in generated version_details payload
        # so try using chat project_id where toolkit participants expected to be
        if parsed.version_details and chat_project_id:
            parsed.project_id = chat_project_id

        room = get_event_room(
            event_name=sio_event,
            room_id=parsed.stream_id
        )

        user_context = {}

        if sid:
            self.context.sio.enter_room(sid, room)

            if sid not in auth.sio_users.keys():
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=sio_event,
                    error='No such sid in sio users',
                    stream_id=data.get("stream_id"),
                    message_id=data.get("message_id")
                )

            current_user = auth.current_user(
                auth_data=auth.sio_users[sid]
            )
            user_id = current_user['id']
            if parsed.application_id and parsed.version_id:
                user_context = {
                    'user_id': user_id,
                    'project_id': parsed.project_id,
                    'chat_project_id': chat_project_id,
                }

        # Load context_settings from conversation.meta if not provided (only if context_manager feature is enabled)
        if not parsed.context_settings:
            conversation_id = parsed.conversation_id or parsed.stream_id
            if get_context_manager_feature_flag(parsed.project_id):
                context_strategy = load_context_settings_from_conversation(parsed.project_id, conversation_id)
                if context_strategy:
                    from ..models.pd.chat import ContextStrategyModel
                    parsed.context_settings = ContextStrategyModel(**context_strategy)

        try:
            payload: dict = generate_predict_payload(parsed, user_id=user_id, sid=sid, is_system_user=is_system_user)
        except PredictPayloadError as e:
            raise SioValidationError(
                sio=self.context.sio,
                sid=sid,
                event=sio_event,
                error=str(e),
                stream_id=data.get("stream_id"),
                message_id=data.get("message_id")
            )
        log.debug(f'{payload=}')

        if not user_context:
            user_context = {
                "user_id": user_id,
                "project_id": parsed.project_id,
            }

        # TODO probably better move to toolkits expand, check OpenAPI
        vc = VaultClient(parsed.project_id)
        payload = vc.unsecret(payload)

        task_id = self.task_node.start_task(
            "indexer_agent",
            args=[parsed.stream_id, parsed.message_id],
            kwargs=payload,
            pool="agents",
            meta=add_trace_context_to_meta({
                "task_name": "indexer_agent",
                "project_id": parsed.project_id,
                "message_id": parsed.message_id,
                "question_id": start_event_content.get('question_id') if start_event_content else None,
                "sio_event": f'{sio_event}',  # enums like this
                'chat_project_id': chat_project_id,
                'user_context': serialize(user_context)
            }),
        )
        if sio_event == SioEvents.chat_predict.value:
            self.context.event_manager.fire_event('applications_predict_task_id', {
                "task_id": task_id,
                "project_id": parsed.project_id,
                "message_group_id": parsed.message_id,
            })
        self.stream_response(sio_event, {
            "type": "start_task",
            "stream_id": parsed.stream_id,
            "message_id": parsed.message_id,
            "sio_event": f'{sio_event}',  # enums like this
            "content": {'task_id': task_id, **start_event_content},
            'interaction_uuid': parsed.interaction_uuid
        })

        if await_task_timeout > 0:
            result = self.task_node.join_task(task_id, timeout=int(await_task_timeout))
            if result is not ...:
                return {"result":  result}

        return {"task_id": task_id}

    @web.rpc("applications_predict_sio_llm", "predict_sio_llm")
    def predict_sio_llm(self,
                        sid: str | None,
                        data: dict,
                        sio_event: str = SioEvents.application_predict,
                        start_event_content: Optional[dict] = None,
                        chat_project_id: Optional[int] = None,
                        await_task_timeout: int = -1,
                        user_id: Optional[int] = None,
                        is_system_user: bool = False
                        ) -> dict:
        """
        LLM predict with dual behavior based on parameters

        Usage scenarios:
        1. API blocking call: sid=None, await_task_timeout>0 -> blocks and waits for result
        2. SIO streaming call: sid provided, await_task_timeout<=0 -> returns task_id with streaming

        Args:
            sid: Socket ID for SIO calls (None for API calls)
            data: LLM chat request data
            sio_event: SIO event name for communication
            start_event_content: Additional content for start event
            chat_project_id: Optional chat project ID
            await_task_timeout: -1 for SIO streaming, >0 for blocking API calls
            user_id: user id
            is_system_user: WARN if True you can't do tool or configuration expand (used in summarization)

        Returns:
            Dictionary with task_id (SIO) or result (API blocking)
        """
        if start_event_content is None:
            start_event_content = {}
        data['message_id'] = data.get('message_id', str(uuid4()))
        data['stream_id'] = data.get('stream_id', data['message_id'])

        # Determine call type and validate arguments
        is_blocking = await_task_timeout > 0

        # Validate argument combinations
        if is_blocking and sid:
            raise ValueError("Blocking calls should not have sid (API calls use sid=None)")
        # if not is_blocking and not sid:
        #     raise ValueError("Non-blocking calls require sid (SIO calls)")

        try:
            parsed = LLMChatRequest.model_validate(data)
        except ValidationError as e:
            if sid:
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=sio_event,
                    error=e.errors(),
                    stream_id=data.get("stream_id"),
                    message_id=data.get("message_id")
                )
            else:
                raise ValidationError(e.errors())

        if sid and not auth.is_sio_user_in_project(sid, parsed.project_id):
            log.warning("Sid %s is not in project %s", sid, parsed.project_id)
            return  # FIXME: need some proper error?

        room = get_event_room(
            event_name=sio_event,
            room_id=parsed.stream_id
        )

        if sid:
            # SIO-based call (non-blocking with streaming)
            self.context.sio.enter_room(sid, room)

            if sid not in auth.sio_users.keys():
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=sio_event,
                    error='No such sid in sio users',
                    stream_id=data.get("stream_id"),
                    message_id=data.get("message_id")
                )

            current_user = auth.current_user(
                auth_data=auth.sio_users[sid]
            )
            user_id = current_user['id']

        # Load context_settings from conversation.meta if not provided (only if context_manager feature is enabled)
        if not parsed.context_settings:
            conversation_id = parsed.conversation_id or parsed.stream_id
            if get_context_manager_feature_flag(parsed.project_id):
                context_strategy = load_context_settings_from_conversation(parsed.project_id, conversation_id)
                if context_strategy:
                    from ..models.pd.chat import ContextStrategyModel
                    parsed.context_settings = ContextStrategyModel(**context_strategy)

        try:
            payload: dict = generate_predict_payload(parsed, user_id=user_id, sid=sid, is_system_user=is_system_user)
        except PredictPayloadError as e:
            if sid:
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=sio_event,
                    error=str(e),
                    stream_id=data.get("stream_id"),
                    message_id=data.get("message_id")
                )
            else:
                raise PredictPayloadError(str(e))

        log.debug(f'{payload=}')

        # TODO probably better move to toolkits expand, check OpenAPI
        vc = VaultClient(parsed.project_id)
        payload = vc.unsecret(payload)

        task_id = self.task_node.start_task(
            "indexer_predict_agent",
            args=[parsed.stream_id, parsed.message_id],
            kwargs=payload,
            pool="agents",
            meta=add_trace_context_to_meta({
                "task_name": "indexer_predict_agent",
                "project_id": parsed.project_id,
                "message_id": parsed.message_id,
                "question_id": start_event_content.get('question_id') if start_event_content else None,
                "sio_event": f'{sio_event}',
                'chat_project_id': chat_project_id,
                'user_context': {
                    "user_id": user_id,
                    "project_id": parsed.project_id,
                },  # NOTE: needed for external providers to work!
            }),
        )

        # Send SIO events only for SIO calls
        if sid:
            if sio_event == SioEvents.chat_predict.value:
                self.context.event_manager.fire_event('applications_predict_task_id', {
                    "task_id": task_id,
                    "project_id": parsed.project_id,
                    "message_group_id": parsed.message_id,
                })
            self.stream_response(sio_event, {
                "type": "start_task",
                "stream_id": parsed.stream_id,
                "message_id": parsed.message_id,
                "sio_event": f'{sio_event}',
                "content": {'task_id': task_id, **start_event_content},
                'interaction_uuid': parsed.interaction_uuid
            })

        # Wait for result only for blocking calls
        if is_blocking:
            result = self.task_node.join_task(task_id, timeout=int(await_task_timeout))
            if result is not ...:
                return {"result": result}

        return {"task_id": task_id}

    @web.rpc('applications_get_application_count', "get_application_count")
    def applications_get_application_count(self, project_id: int, **kwargs) -> int:
        with db.with_project_schema_session(project_id) as session:
            return session.query(Application).count()

    @web.rpc('applications_get_search_options', 'applications_get_search_options')
    def applications_get_search_options(self, project_id: int, pipeline: bool = False) -> int:
        return get_search_options_one_entity(
            project_id,
            "application",
            Application,
            ApplicationVersion,
            MultipleApplicationSearchModel,
            ApplicationVersionTagAssociation,
            "pipeline" if pipeline else None
        )

    @web.rpc('applications_get_toolkit_search_options', 'applications_get_toolkit_search_options')
    def applications_get_toolkit_search_options(self, project_id: int, **kwargs) -> int:
        toolkit_type = kwargs.get('toolkit_type', None)
        if toolkit_type is None:
            toolkit_types = []
        else:
            toolkit_types = [toolkit_type]
        toolkits = toolkits_listing(
            project_id=project_id,
            query=kwargs.get('query', None),
            toolkit_type=toolkit_types,
            limit=kwargs.get('limit', 10),
            offset=kwargs.get('offset', 0),
            sort_by=kwargs.get('sort', 'created_at'),
            sort_order=kwargs.get('order', 'desc')
        )
        toolkits['rows'] = [
            {
                'id': t['id'],
                'name': t.get('toolkit_name') or t.get('settings', {}).get('elitea_title'),
            } for t in toolkits['rows']
        ]
        return toolkits

    @web.rpc('applications_get_toolkits_by_settings', 'applications_get_toolkits_by_settings')
    def applications_get_toolkits_by_settings(self, project_id: int, **kwargs) -> int:
        """
        Get toolkits by its settings
        :param project_id: Project ID
        :param kwargs: filter settings
        :return: list of filtered toolkits

        Example:
            applications_get_toolkits_by_settings(
            project_id,
            toolkit_type='artifact',
            settings={'bucket': 'publictest'})
        """
        toolkit_type = kwargs.get('toolkit_type', None)
        if toolkit_type is None:
            toolkit_types = []
        else:
            toolkit_types = [toolkit_type]
        toolkits = toolkits_listing(
            project_id=project_id,
            query=kwargs.get('query', None),
            toolkit_type=toolkit_types,
            limit=kwargs.get('limit', 10),
            offset=kwargs.get('offset', 0),
            sort_by=kwargs.get('sort', 'created_at'),
            sort_order=kwargs.get('order', 'desc')
        )
        # search for settings in kwargs
        return [
            t for t in toolkits['rows']
            if all(
                t.get('settings', {}).get(k) == v
                for k, v in (kwargs.get('settings') or {}).items()
            )
        ]

    @web.rpc('applications_import_toolkit', 'applications_import_toolkit')
    def applications_import_toolkit(self, payload: dict, project_id: int, author_id: int) -> str:
        payload['user_id'] = payload['author_id'] = author_id
        payload['project_id'] = project_id
        try:
            toolkit_data = ToolImportModel.model_validate(payload)
            with db.get_session(project_id) as session:
                wrap_provider_hub_secret_fields(toolkit_data.type, toolkit_data.settings, project_id)
                store_secrets(toolkit_data.model_dump(), project_id)

                toolkit_new = EliteATool(
                    **serialize(toolkit_data),
                )
                session.add(toolkit_new)
                session.commit()

                result = ToolDetails.from_orm(toolkit_new)

                return result.model_dump(mode='json')
        except ValidationError as e:
            raise RuntimeError(str(e))
        except Exception as e:
            log.error(e)
            raise RuntimeError("Import function has been failed")

    @web.rpc('applications_toolkit_link', 'applications_toolkit_link')
    def applications_toolkit_link(self, project_id: int, toolkit_id: int, payload: dict):
        with db.get_session(project_id) as session:
            entity_version_id = payload['entity_version_id']
            entity_id = payload['entity_id']
            entity_type = ToolEntityTypes(payload['entity_type'])

            # Check if link already exists to avoid duplicate key violation
            existing_link = session.query(EntityToolMapping).filter(
                EntityToolMapping.tool_id == toolkit_id,
                EntityToolMapping.entity_version_id == entity_version_id,
                EntityToolMapping.entity_type == entity_type
            ).first()

            if existing_link:
                log.info(f"[IMPORT] Toolkit link already exists: toolkit_id={toolkit_id}, "
                         f"entity_version_id={entity_version_id}, entity_type={entity_type}")
                return

            application_tool_to_entity = EntityToolMapping(
                tool_id=toolkit_id,
                entity_version_id=entity_version_id,
                entity_id=entity_id,
                entity_type=entity_type
            )
            session.add(application_tool_to_entity)
            session.commit()

    @web.rpc('applications_import_application', 'applications_import_application')
    def applications_import_application(self, payload: dict, project_id: int, author_id: int) -> Tuple[
        str, list
    ]:
        errors = []

        if not payload:
            raise ValueError

        payload['owner_id'] = project_id

        def set_base(version_: dict):
            version_['name'] = 'base'

        with db.get_session(project_id) as session:
            try:
                versions = deepcopy(payload.get("versions", []))

                for version in versions:
                    meta = version.get('meta') or {}
                    if 'parent_author_id' in meta:
                        version["author_id"] = meta.get('parent_author_id')
                    else:
                        version["author_id"] = author_id
                    # Rename empty names and "latest" to "base"
                    if not version.get('name') or version.get('name') == 'latest':
                        set_base(version)

                # Deduplicate versions by name (keep first occurrence of each name)
                seen_names = set()
                deduplicated_versions = []
                for version in versions:
                    if version['name'] not in seen_names:
                        seen_names.add(version['name'])
                        deduplicated_versions.append(version)
                versions = deduplicated_versions

                if not any(v['name'] == 'base' for v in versions):
                    if len(versions) == 1:
                        # Single version - just rename it to 'base' instead of duplicating
                        original_name = versions[0].get('name', 'unknown')
                        set_base(versions[0])
                        log.info(f"[IMPORT] No 'base' version found for '{payload.get('name', '')}'. "
                                 f"Renamed version '{original_name}' to 'base'.")
                    else:
                        # Multiple versions - duplicate one as 'base'
                        # Sort by created_at if available (JSON export), otherwise use last in list
                        versions_with_dates = [v for v in versions if v.get('created_at')]
                        if versions_with_dates:
                            source = sorted(
                                versions_with_dates,
                                key=lambda x: parser.parse(x['created_at']),
                                reverse=False
                            )[-1]
                        else:
                            source = versions[-1]
                        latest_version = deepcopy(source)
                        source_version_name = latest_version.get('name', 'unknown')
                        set_base(latest_version)
                        # Insert base as the first version so it becomes the default
                        versions.insert(0, latest_version)
                        log.info(f"[IMPORT] No 'base' version found for '{payload.get('name', '')}'. "
                                 f"Created 'base' version from '{source_version_name}'.")

                payload['versions'] = versions

                payload['project_id'] = project_id
                payload['user_id'] = author_id

                application_data = ApplicationImportModel.model_validate(payload)
            except ValidationError as e:
                errors.append(str(e))
                return '', errors

            try:
                application = create_application(application_data, session, project_id)
            except Exception as e:
                errors.append(str(e))

            session.commit()

            # Explicitly load relationships for the first version since they are now lazy
            first_version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == application.versions[0].id
            ).options(
                selectinload(ApplicationVersion.tools),
                selectinload(ApplicationVersion.tool_mappings),
                selectinload(ApplicationVersion.variables)
            ).first()

            result = ApplicationDetailModel.from_orm(application)
            result.version_details = ApplicationVersionDetailModel.from_orm(first_version)

        return result.model_dump(mode='json'), errors

    @web.rpc("applications_export_application", "export_application")
    def export_application(self, applications_grouped: dict, user_id: int, forked: bool = False) -> dict:
        result = {}

        for project_id, application_ids in applications_grouped.items():
            export_data = export_application(project_id, user_id, application_ids, forked=forked)
            if not export_data.pop('ok'):
                raise RuntimeError(export_data['msg'])
            result = deep_update(result, export_data)

        return result

    '''
    @web.rpc("applications_add_application_tool", "add_application_tool")
    def add_application_tool(self, data: dict, project_id: int, application_version_id: int, return_details: bool = False) -> dict:
        try:
            tool_data = ToolCreateModel.parse_obj(data)
        except ValidationError as e:
            return {"ok": False, "error": e.errors()}

        with db.get_session(project_id) as session:
            app_version = session.query(ApplicationVersion).filter(
                ApplicationVersion.id == application_version_id
            ).first()
            if not app_version:
                return {'ok': False, 'error': f'No such application version with id {application_version_id}'}

            wrap_provider_hub_secret_fields(tool_data.type, tool_data.settings, project_id)
            store_secrets(tool_data.dict(), project_id)

            application_tool = EliteATool(
                **serialize(tool_data),
            )
            session.add(application_tool)
            session.flush()
            application_tool_to_application = EntityToolMapping(
                tool_id=application_tool.id,
                entity_version_id=application_version_id,
                entity_id=app_version.application_id,
                entity_type=ToolEntityTypes.agent
            )
            session.add(application_tool_to_application)
            session.commit()

            if return_details:
                result = get_application_details(project_id, app_version.application_id)
                if not result['ok']:
                    return {'ok': False, 'error': result['msg']}
                return {'ok': True, 'details': result['data']}

            result = ToolDetails.from_orm(application_tool)
            result.fix_name(project_id)
            return {"ok": True, "details": json.loads(result.json())}
    '''

    @web.rpc("applications_delete_application", "delete_application")
    def application_delete(self, project_id: int, application_id: int) -> bool:
        with db.get_session(project_id) as session:
            if application := session.query(Application).get(application_id):
                # Block deletion if any version is published or embedded
                blocked_statuses = {PublishStatus.published, PublishStatus.embedded}
                for ver in application.versions:
                    if ver.status in blocked_statuses:
                        return {
                            'error': 'Cannot delete application with published or '
                                     'embedded versions. Unpublish first.',
                            'blocked_version_id': ver.id,
                            'blocked_status': ver.status,
                        }

                session.delete(application)
                session.commit()

                application_data = ApplicationDetailModel.from_orm(application)
                application_data = application_data.model_dump()
                application_data['project_id'] = project_id
                self.context.event_manager.fire_event(
                    ApplicationEvents.application_deleted, application_data
                )

                return True
        return False

    @web.rpc("applications_find_existing_fork", "find_existing_fork")
    def applications_find_existing_fork(
            self, target_project_id: int, parent_entity_id: int, parent_project_id: int
    ) -> tuple[int, int] | tuple[None, None]:
        with (db.get_session(target_project_id) as session):
            if parent_project_id == target_project_id:
                target_project_forked_application = session.query(Application).filter(
                    Application.id == parent_entity_id
                ).first()
                if target_project_forked_application:
                    target_forked_latest_version = target_project_forked_application.get_latest_version()
                    return target_project_forked_application.id, target_forked_latest_version.id
                else:
                    return None, None
            else:
                is_forked_subquery = (
                    session.query(ApplicationVersion.application_id)
                    .filter(ApplicationVersion.meta.op('->>')('parent_entity_id').isnot(None),
                            ApplicationVersion.meta.op('->>')('parent_project_id').isnot(None))
                    .subquery()
                )
                target_project_forked_applications = session.query(Application).filter(
                    Application.id.in_(is_forked_subquery)
                ).all()

            for forked_application in target_project_forked_applications:
                for version in forked_application.versions:
                    forked_version_meta = version.meta or {}
                    forked_version_parent_entity_id = forked_version_meta.get('parent_entity_id')
                    forked_version_parent_project_id = forked_version_meta.get('parent_project_id')
                    if parent_entity_id == forked_version_parent_entity_id \
                            and parent_project_id == forked_version_parent_project_id:
                        return forked_application.id, version.id
            return None, None

    @web.rpc("applications_update_tool_with_existing_fork", "update_tool_with_existing_fork")
    def applications_update_tool_with_existing_fork(
            self, target_project_id: int, input_tool: dict,
            tool_parent_entity_id: int, tool_parent_project_id: int
    ) -> Tuple[dict, str]:
        forked_application_id, forked_version_id = self.find_existing_fork(
            target_project_id, tool_parent_entity_id, tool_parent_project_id
        )
        if forked_application_id and forked_version_id:
            input_tool['settings'].pop('import_uuid', None)
            import_version_uuid = input_tool['settings'].pop('import_version_uuid')
            input_tool['settings'].update({
                'application_version_id': forked_version_id,
                'application_id': forked_application_id,
            })
            return input_tool, import_version_uuid
        else:
            return input_tool, str()

    @web.rpc("applications_get_stats", "get_stats")
    def get_stats(self, project_id: int, author_id: int):
        result = {}
        pipeline_only_query = Application.versions.any(
            ApplicationVersion.agent_type == AgentTypes.pipeline.value
        )
        with db.with_project_schema_session(project_id) as session:
            query = session.query(Application).filter(
                Application.versions.any(ApplicationVersion.author_id == author_id),
                ~pipeline_only_query
            )
            result['total_applications'] = query.count()
            query = query.filter(
                Application.versions.any(ApplicationVersion.status == PublishStatus.published),
                ~pipeline_only_query
            )
            result['public_applications'] = query.count()

        return result

    @web.rpc("applications_get_pipelines_stats", "get_pipeline_stats")
    def get_pipeline_stats(self, project_id: int, author_id: int):
        result = {}
        pipeline_only_query = Application.versions.any(
            ApplicationVersion.agent_type == AgentTypes.pipeline.value
        )
        with db.with_project_schema_session(project_id) as session:
            query = session.query(Application).filter(
                Application.versions.any(ApplicationVersion.author_id == author_id),
                pipeline_only_query
            )
            result['total_pipelines'] = query.count()
            query = query.filter(
                Application.versions.any(ApplicationVersion.status == PublishStatus.published),
                pipeline_only_query
            )
            result['public_pipelines'] = query.count()

        return result

    @web.rpc("applications_get_toolkits_stats", "get_toolkits_stats")
    def get_toolkits_stats(self, project_id: int, author_id: int):
        result = {}
        with db.with_project_schema_session(project_id) as session:
            query = session.query(EliteATool).filter(
                EliteATool.author_id == author_id
            )
            result['total_toolkits'] = query.count()

        return result

    @web.rpc("applications_get_application_ids_to_name", "get_application_ids_to_name")
    def get_application_ids_to_name(self, project_id: int, application_ids: List[int]):
        result = {
            "entity": {},
            "entity_versions": {}
        }
        with db.get_session(project_id) as session:
            applications = session.query(
                Application
            ).filter(
                Application.id.in_(application_ids),
            ).options(
                selectinload(Application.versions)
            ).all()

            for app in applications:
                result['entity'][app.id] = app.name
                result['entity_versions'].update({
                    version.id: version.name for version in app.versions
                })

        return result

    @web.rpc("applications_find_existing_toolkit_fork", "find_existing_toolkit_fork")
    def find_existing_toolkit_fork(
            self, target_project_id: int, parent_entity_id: int, parent_project_id: int
    ) -> int | None:
        with db.get_session(target_project_id) as session:
            result = session.query(EliteATool.id).where(
                EliteATool.meta['parent_entity_id'].astext.cast(Integer) == parent_entity_id,
                EliteATool.meta['parent_project_id'].astext.cast(Integer) == parent_project_id,
            ).first()
            return result[0] if result else None

    @web.rpc("applications_get_toolkit_by_id", "get_toolkit_by_id")
    def get_toolkit_by_id(self, project_id: int, toolkit_id: int) -> dict:
        with db.get_session(project_id) as session:
            toolkit = session.query(EliteATool).where(
                EliteATool.id == toolkit_id,
            ).first()
            return serialize(ToolDetails.from_orm(toolkit)) if toolkit else {}

    @web.rpc("applications_configuration_check_connection", "configuration_check_connection")
    def configuration_check_connection(self, type_: str, settings: dict) -> str | None:
        task_id = self.task_node.start_task(
            "indexer_configuration_check_connection",
            kwargs={
                "configuration_type": type_,
                "settings": settings
            },
            pool="indexer",
            meta={}
        )
        return self.task_node.join_task(task_id, timeout=60)

    @web.rpc("applications_toolkit_settings_validator", "toolkit_settings_validator")
    def toolkit_settings_validator(self, settings: dict, type_: str, project_id: int, user_id: int) -> str | None:
        tk, external = find_toolkit_schema_by_type_everywhere(type_, project_id, user_id)
        if not tk:
            # Passthrough if no schema found
            return {"ok": True, "result": settings}
        if not external:
            task_id = self.task_node.start_task(
                "indexer_validator",
                kwargs={
                    "toolkit_type": type_,
                    "settings": settings
                },
                pool="indexer",
                meta={}
            )
            task_result = self.task_node.join_task(task_id, timeout=60)
            if "error" in task_result:
                return {"ok": False, "error": task_result['error']}

            return {"ok": True, "result": task_result['result']}
        else:
            # TODO: validate by different rpc from external service
            return {"ok": True, "result": settings}

    @web.rpc("applications_configuration_validator", "configuration_validator")
    def configuration_validator(self, settings: dict, type_: str, ) -> str | None:
        task_id = self.task_node.start_task(
            "indexer_configuration_validator",
            kwargs={
                "configuration_type": type_,
                "settings": settings
            },
            pool="indexer",
            meta={}
        )
        task_result = self.task_node.join_task(task_id, timeout=60)
        if "error" in task_result:
            raise ValueError(task_result['error'])

        return task_result['result']

    @web.rpc("applications_get_toolkit_available_tools", "get_toolkit_available_tools")
    def get_toolkit_available_tools(self, toolkit_type: str, settings: dict) -> dict:
        """
        Get available tools and per-tool JSON schemas for a toolkit instance.

        This is used by the UI when spec-dependent tool enumeration is needed
        (e.g. OpenAPI where tools depend on the spec provided in settings).

        Args:
            toolkit_type: toolkit type string (e.g. 'openapi')
            settings: persisted toolkit settings

        Returns:
            {
              "tools": [{"name": str, "description": str}],
              "args_schemas": {"tool_name": <json schema dict>}
            }
        """
        task_id = self.task_node.start_task(
            "indexer_toolkit_available_tools",
            kwargs={
                "toolkit_type": toolkit_type,
                "settings": settings,
            },
            pool="indexer",
            meta={},
        )
        return self.task_node.join_task(task_id, timeout=60)

    @web.rpc("applications_discover_mcp_tools", "discover_mcp_tools")
    def discover_mcp_tools(self, toolkit_type: str, settings: dict) -> dict | str:
        """
        Discover tools from an MCP server by calling the SDK's check_connection.

        This is used by the "Load Tools" button for MCP toolkit configuration.
        The SDK connects to the MCP server and retrieves the list of available tools
        along with their input schemas.

        Args:
            toolkit_type: MCP toolkit type (e.g., 'mcp_github_copilot')
            settings: Toolkit settings (credentials, etc.)

        Returns:
            On success: {
                "tools": [{"name": str, "description": str, "inputSchema": dict}],
                "args_schemas": {"tool_name": <json schema dict>}
            }
            On error: str with error message
        """
        task_id = self.task_node.start_task(
            "indexer_configuration_check_connection",
            kwargs={
                "configuration_type": toolkit_type,
                "settings": settings,
            },
            pool="indexer",
            meta={},
        )
        return self.task_node.join_task(task_id, timeout=60)

    @web.rpc("applications_test_toolkit_tool_sio", "test_toolkit_tool_sio")
    def test_toolkit_tool_sio(self,
                              sid: str | None,
                              data: dict,
                              sio_event: str = "test_toolkit_tool",
                              start_event_content: Optional[dict] = None,
                              await_task_timeout: int = -1,
                              chat_project_id: Optional[int] = None,
                              **kwargs
                              ) -> dict:
        """
        Test a single toolkit tool using the indexer_test_toolkit_tool method.

        Args:
            sid: Socket ID for real-time communication
            data: Test parameters containing:
                - toolkit_config: Toolkit initialization parameters (required)
                - tool_name: Name of the tool to test (required)
                - tool_params: Parameters to pass to the tool execution (optional, defaults to {})
                - project_id: Project ID (required)
                - llm_model: LLM model to use (optional)
                - llm_settings: LLM configuration settings (optional)
                - runtime_config: Runtime configuration (optional)
            sio_event: SIO event name for communication
            start_event_content: Additional content for start event
            await_task_timeout: Timeout for task completion (-1 for no wait)

        Returns:
            Dictionary with task_id and optionally result if awaited
        """
        if start_event_content is None:
            start_event_content = {}

        # Generate unique IDs for tracking
        data['message_id'] = data.get('message_id', str(uuid4()))
        data['stream_id'] = data.get('stream_id', data['message_id'])

        # Basic validation
        if not data.get('toolkit_config'):
            raise ValueError("toolkit_config is required")
        if not data.get('tool_name'):
            raise ValueError("tool_name is required")

        tool_name = data.get('tool_name')
        project_id = data.get('project_id')
        if not project_id:
            raise ValueError("project_id is required")
        data['chat_project_id'] = chat_project_id

        if sid and not auth.is_sio_user_in_project(sid, project_id):
            log.warning("Sid %s is not in project %s", sid, project_id)
            return  # FIXME: need some proper error?

        # Ensure tool_params is present (can be empty dict)
        if 'tool_params' not in data:
            data['tool_params'] = {}

        if sid:
            if sid not in auth.sio_users.keys():
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=sio_event,
                    error='No such sid in sio users',
                    stream_id=data.get("stream_id"),
                    message_id=data.get("message_id")
                )

            current_user = auth.current_user(
                auth_data=auth.sio_users[sid]
            )
            data['user_id'] = current_user['id']
            log.debug(f"Got user_id from SID: {data['user_id']} (sid: {sid})")
        else:
            # For direct API calls without SID, try to get user from current session
            try:
                current_user = auth.current_user()
                if current_user and current_user.get('id'):
                    data['user_id'] = current_user['id']
                    log.debug(f"Got user_id from current session: {data['user_id']}")
                else:
                    log.warning("No user_id available from current session")
            except Exception as e:
                log.warning(f"Could not get current user for configuration resolution: {e}")
                log.debug(f"No SID provided and no current user session available")

        user_id = data.get('user_id')
        log.info(f"About to expand toolkit configurations: user_id={user_id}, project_id={project_id}")
        try:
            log.info(f"Starting toolkit configuration expansion for user {user_id} in project {project_id}")
            log.debug(f"Original toolkit_config: {data.get('toolkit_config', {})}")

            toolkit_config = data.get('toolkit_config', {})
            toolkit_type = toolkit_config.get('type', 'unknown_toolkit')

            toolkit_settings_expanded = expand_toolkit_settings(
                toolkit_type, toolkit_config.get('settings', {}), project_id=project_id, user_id=user_id
            )
            validation_result = self.toolkit_settings_validator(toolkit_settings_expanded, type_=toolkit_type, project_id=project_id, user_id=user_id)
            if not validation_result.get('ok'):
                raise ValueError(f"Toolkit settings validation failed: {validation_result.get('error')}")

            data['toolkit_config'] = {
                'id': toolkit_config.get('toolkit_id', None),
                'type': toolkit_type,  # Preserve the toolkit type
                'toolkit_name': toolkit_config.get('toolkit_name', 'unknown_toolkit'),
                'settings': toolkit_settings_expanded
            }

            log.info(f"Expanded toolkit configuration for user {user_id} in project {project_id}")
            log.debug(f"Expanded toolkit_config: {data['toolkit_config']}")
        except Exception as e:
            log.error(f"Error expanding toolkit configurations: {str(e)}")
            # Continue with original config if expansion fails
            log.warning("Continuing with original toolkit configuration")

        # Get project-specific auth_token from secrets (not exposed to user)
        try:
            data['project_auth_token'], _ = get_predict_token_and_session(project_id, data['user_id'], sid)
            data['deployment_url'] = get_predict_base_url(project_id)
        except Exception as e:
            log.warning(f"Failed to retrieve project secrets: {e}")

        # Add authentication parameters to the data (don't expose auth_token to user)

        # Get room for SIO communication
        room = get_event_room(
            event_name=sio_event,
            room_id=data['stream_id']
        )

        if sid:
            self.context.sio.enter_room(sid, room)

        # Log the parameters being passed to indexer for debugging
        log.info(f"Testing toolkit tool: {tool_name} with toolkit_config keys: {list(data.get('toolkit_config', {}).keys())} and tool_params: {data.get('tool_params', {})}")

        # Prepare kwargs without stream_id and message_id since they're passed as args
        task_kwargs = {k: v for k, v in data.items() if k not in ['stream_id', 'message_id']}

        # Unsecret the entire task data once before creating the task
        try:
            VaultClient(project_id).unsecret(task_kwargs)
            log.debug("Unsecreted all task data including toolkit_config and tool_params")
        except Exception as e:
            log.warning(f"Failed to unsecret task data: {e}")

        # Start the test toolkit tool task
        if tool_name == 'index_data':
            task_id = start_index_task(self.task_node, data, sio_event)
        else:
            task_id = self.task_node.start_task(
                "indexer_test_toolkit_tool",
                args=[data['stream_id'], data['message_id']],
                kwargs=task_kwargs,
                pool="agents",
                meta={
                    "task_name": "indexer_test_toolkit_tool",
                    "project_id": project_id,
                    'chat_project_id': chat_project_id,
                    "message_id": data['message_id'],
                    "question_id": start_event_content.get('question_id') if start_event_content else None,
                    "sio_event": sio_event,
                    "toolkit_config": task_kwargs.get('toolkit_config', {}),
                    "tool_name": task_kwargs.get('tool_name', ''),
                    "tool_params": task_kwargs.get('tool_params', {}),
                    "user_id": task_kwargs.get('user_id', ''),
                    "deployment_url": task_kwargs.get('deployment_url', ''),
                    "project_auth_token": task_kwargs.get('project_auth_token', ''),
                    "user_context": {
                        "user_id": task_kwargs.get("user_id", None),
                        "project_id": project_id,
                    },
                },
            )

        # Send start event
        self.stream_response(sio_event, {
            "type": "start_task",
            "stream_id": data['stream_id'],
            "message_id": data['message_id'],
            "sio_event": sio_event,
            "content": {'task_id': task_id, **start_event_content},
        })

        # Wait for task completion if requested
        if await_task_timeout > 0:
            result = self.task_node.join_task(task_id, timeout=int(await_task_timeout))
            if result is not ...:
                return {"result": result}

        return {"task_id": task_id}

    @web.rpc("applications_test_mcp_connection_sio", "test_mcp_connection_sio")
    def test_mcp_connection_sio(self,
                                sid: str | None,
                                data: dict,
                                sio_event: str = "test_mcp_connection",
                                start_event_content: Optional[dict] = None,
                                await_task_timeout: int = -1,
                                chat_project_id: Optional[int] = None,
                                **kwargs
                                ) -> dict:
        """
        Test MCP server connection using protocol-level list_tools.

        This method verifies MCP server connectivity and authentication by calling
        the protocol-level tools/list JSON-RPC method (NOT executing a tool).
        This is ideal for auth checks as it validates the connection without
        requiring any tool execution.

        Args:
            sid: Socket ID for real-time communication
            data: Test parameters containing:
                - toolkit_config: MCP toolkit configuration (required, must have type='mcp')
                - project_id: Project ID (required)
                - mcp_tokens: MCP OAuth tokens (optional)
            sio_event: SIO event name for communication
            start_event_content: Additional content for start event
            await_task_timeout: Timeout for task completion (-1 for no wait)

        Returns:
            Dictionary with task_id and optionally result if awaited
        """
        if start_event_content is None:
            start_event_content = {}

        # Generate unique IDs for tracking
        data['message_id'] = data.get('message_id', str(uuid4()))
        data['stream_id'] = data.get('stream_id', data['message_id'])

        # Basic validation
        if not data.get('toolkit_config'):
            raise ValueError("toolkit_config is required")

        toolkit_config = data.get('toolkit_config', {})
        toolkit_type = toolkit_config.get('type', 'unknown')
        if toolkit_type != 'mcp':
            raise ValueError(f"test_mcp_connection only works with MCP toolkits, got type: {toolkit_type}")

        project_id = data.get('project_id')
        if not project_id:
            raise ValueError("project_id is required")
        data['chat_project_id'] = chat_project_id

        if not auth.is_sio_user_in_project(sid, project_id):
            log.warning("Sid %s is not in project %s", sid, project_id)
            return  # FIXME: need some proper error?

        if sid:
            if sid not in auth.sio_users.keys():
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=sio_event,
                    error='No such sid in sio users',
                    stream_id=data.get("stream_id"),
                    message_id=data.get("message_id")
                )

            current_user = auth.current_user(
                auth_data=auth.sio_users[sid]
            )
            data['user_id'] = current_user['id']
            log.debug(f"Got user_id from SID: {data['user_id']} (sid: {sid})")
        else:
            # For direct API calls without SID, try to get user from current session
            try:
                current_user = auth.current_user()
                if current_user and current_user.get('id'):
                    data['user_id'] = current_user['id']
                    log.debug(f"Got user_id from current session: {data['user_id']}")
                else:
                    log.warning("No user_id available from current session")
            except Exception as e:
                log.warning(f"Could not get current user for configuration resolution: {e}")
                log.debug(f"No SID provided and no current user session available")

        user_id = data.get('user_id')

        try:

            toolkit_settings_expanded = expand_toolkit_settings(
                toolkit_type, toolkit_config.get('settings', {}), project_id=project_id, user_id=user_id
            )
            validation_result = self.toolkit_settings_validator(toolkit_settings_expanded, type_=toolkit_type, project_id=project_id, user_id=user_id)
            if not validation_result.get('ok'):
                raise ValueError(f"Toolkit settings validation failed: {validation_result.get('error')}")

            data['toolkit_config'] = {
                'id': toolkit_config.get('toolkit_id', None),
                'type': toolkit_type,
                'toolkit_name': toolkit_config.get('toolkit_name', 'mcp_connection_test'),
                'settings': toolkit_settings_expanded
            }

        except Exception as e:
            log.error(f"Error expanding MCP toolkit configurations: {str(e)}")
            log.warning("Continuing with original toolkit configuration")

        # Get project-specific auth_token from secrets
        try:
            data['project_auth_token'], _ = get_predict_token_and_session(project_id, data['user_id'], sid)
            data['deployment_url'] = get_predict_base_url(project_id)
        except Exception as e:
            log.warning(f"Failed to retrieve project secrets: {e}")

        # Get room for SIO communication
        room = get_event_room(
            event_name=sio_event,
            room_id=data['stream_id']
        )

        if sid:
            self.context.sio.enter_room(sid, room)

        # Prepare kwargs without stream_id and message_id since they're passed as args
        task_kwargs = {k: v for k, v in data.items() if k not in ['stream_id', 'message_id']}

        # Unsecret the entire task data once before creating the task
        try:
            VaultClient(project_id).unsecret(task_kwargs)
            log.debug("Unsecreted all task data including toolkit_config")
        except Exception as e:
            log.warning(f"Failed to unsecret task data: {e}")

        # Start the test MCP connection task
        task_id = self.task_node.start_task(
            "indexer_test_mcp_connection",
            args=[data['stream_id'], data['message_id']],
            kwargs=task_kwargs,
            pool="agents",
            meta={
                "task_name": "indexer_test_mcp_connection",
                "project_id": project_id,
                'chat_project_id': chat_project_id,
                "message_id": data['message_id'],
                "question_id": start_event_content.get('question_id') if start_event_content else None,
                "sio_event": sio_event,
                "toolkit_config": task_kwargs.get('toolkit_config', {}),
                "user_id": task_kwargs.get('user_id', ''),
                "deployment_url": task_kwargs.get('deployment_url', ''),
                "project_auth_token": task_kwargs.get('project_auth_token', ''),
                "user_context": {
                    "user_id": task_kwargs.get("user_id", None),
                    "project_id": project_id,
                },
            },
        )

        # Send start event
        self.stream_response(sio_event, {
            "type": "start_task",
            "stream_id": data['stream_id'],
            "message_id": data['message_id'],
            "sio_event": sio_event,
            "content": {'task_id': task_id, **start_event_content},
        })

        # Wait for task completion if requested
        if await_task_timeout > 0:
            result = self.task_node.join_task(task_id, timeout=int(await_task_timeout))
            if result is not ...:
                return {"result": result}

        return {"task_id": task_id}

    @web.rpc("applications_mcp_sync_tools_sio", "mcp_sync_tools_sio")
    def mcp_sync_tools_sio(self,
                           sid: str | None,
                           data: dict,
                           sio_event: str = "mcp_sync_tools",
                           start_event_content: Optional[dict] = None,
                           await_task_timeout: int = -1,
                           **kwargs
                           ) -> dict:
        """
        Sync/fetch tools from a remote MCP server.

        This method discovers available tools from a remote MCP server without
        requiring a saved toolkit. If the server requires OAuth authorization,
        it will emit an 'mcp_authorization_required' socket event.

        Args:
            sid: Socket ID for real-time communication
            data: Parameters containing:
                - url: MCP server HTTP URL (required)
                - headers: HTTP headers for authentication (optional)
                - timeout: Request timeout in seconds (optional, default 60)
                - mcp_tokens: MCP OAuth tokens (optional)
                - project_id: Project ID (required)
                - ssl_verify: Verify SSL (optional, default True)
            sio_event: SIO event name for communication
            start_event_content: Additional content for start event
            await_task_timeout: Timeout for task completion (-1 for no wait)

        Returns:
            Dictionary with task_id and optionally result if awaited
        """
        if start_event_content is None:
            start_event_content = {}

        # Generate unique IDs for tracking
        data['message_id'] = data.get('message_id', str(uuid4()))
        data['stream_id'] = data.get('stream_id', data['message_id'])

        # Basic validation
        if not data.get('url'):
            raise ValueError("MCP server URL is required")

        project_id = data.get('project_id')
        if not project_id:
            raise ValueError("project_id is required")

        if sid:
            if sid not in auth.sio_users.keys():
                raise SioValidationError(
                    sio=self.context.sio,
                    sid=sid,
                    event=sio_event,
                    error='No such sid in sio users',
                    stream_id=data.get("stream_id"),
                    message_id=data.get("message_id")
                )

            current_user = auth.current_user(
                auth_data=auth.sio_users[sid]
            )
            data['user_id'] = current_user['id']
            log.debug(f"Got user_id from SID: {data['user_id']} (sid: {sid})")
        else:
            # For direct API calls without SID, try to get user from current session
            try:
                current_user = auth.current_user()
                if current_user and current_user.get('id'):
                    data['user_id'] = current_user['id']
                    log.debug(f"Got user_id from current session: {data['user_id']}")
            except Exception as e:
                log.warning(f"Could not get current user: {e}")

        # Get room for SIO communication
        room = get_event_room(
            event_name=sio_event,
            room_id=data['stream_id']
        )

        if sid:
            self.context.sio.enter_room(sid, room)

        log.info(f"MCP sync tools request: url={data.get('url')}, project_id={project_id}")

        # Prepare kwargs without stream_id and message_id since they're passed as args
        task_kwargs = {k: v for k, v in data.items() if k not in ['stream_id', 'message_id']}

        # Substitute {{secret.xxx}} placeholders in headers before dispatching the task
        try:
            VaultClient(project_id).unsecret(task_kwargs.get('headers', {}))
            log.debug("Unsecreted MCP headers")
        except Exception as e:
            log.warning(f"Failed to unsecret headers: {e}")

        # Start the MCP sync tools task
        task_id = self.task_node.start_task(
            "indexer_mcp_sync_tools",
            args=[data['stream_id'], data['message_id']],
            kwargs=task_kwargs,
            pool="agents",
            meta={
                "task_name": "indexer_mcp_sync_tools",
                "project_id": project_id,
                "message_id": data['message_id'],
                "sio_event": sio_event,
                "url": data.get('url', ''),
                "user_id": task_kwargs.get('user_id', ''),
                "user_context": {
                    "user_id": task_kwargs.get("user_id", None),
                    "project_id": project_id,
                },
            },
        )

        # Send start event
        self.stream_response(sio_event, {
            "type": "start_task",
            "stream_id": data['stream_id'],
            "message_id": data['message_id'],
            "sio_event": sio_event,
            "content": {'task_id': task_id, **start_event_content},
        })

        # Wait for task completion if requested
        if await_task_timeout > 0:
            result = self.task_node.join_task(task_id, timeout=int(await_task_timeout))
            if result is not ...:
                return {"result": result}

        return {"task_id": task_id}

    @web.rpc("applications_clone_version", "clone_version")
    def clone_version(self, project_id: int, application_id: str, version_id: int, new_version_name: str, author_id: int) -> dict:
        try:
            with db.get_session(project_id) as session:
                app_version: ApplicationVersion = session.query(ApplicationVersion).filter(
                    ApplicationVersion.id == version_id
                ).one_or_none()
                if not app_version:
                    return {
                        "error": f"Application version with id '{version_id}' not found"
                    }
                app_details = get_application_details(
                    project_id,
                    application_id,
                    app_version.name,
                    first_existing_version=False,
                    skip_like_details=True
                ).get('data')

                version_details = app_details.get('version_details', {})
                version_details['name'] = new_version_name
                version_details['project_id'] = project_id
                version_details['author_id'] = author_id
                version_details['user_id'] = author_id
                version_details['status'] = PublishStatus.draft
                new_version_pd = ApplicationVersionCreateModel.model_validate(version_details)
                application = session.query(Application).get(application_id)
                if not application:
                    return {
                        "error": f"Application with id '{application_id}' doesn't exist"
                    }
                version = create_version(
                    version_data=new_version_pd,
                    application=application,
                    session=session,
                )
                session.commit()

                # Explicitly load relationships since they are now lazy
                version = session.query(ApplicationVersion).filter(
                    ApplicationVersion.id == version.id
                ).options(
                    selectinload(ApplicationVersion.tools),
                    selectinload(ApplicationVersion.tool_mappings),
                    selectinload(ApplicationVersion.variables)
                ).first()

                result = ApplicationVersionDetailModel.from_orm(version)
                return result.model_dump(mode='json')
        except Exception as e:
            log.error(f"Failed to clone application {version_id=}: {e}")
            return {"error": "Can't clone version, check logs for details"}

    @web.rpc("applications_check_version_in_use", "check_version_in_use")
    def check_version_in_use(self, project_id: int, version_id: int) -> dict:
        """
        Check if a version is referenced by any parent agents/pipelines.
        Returns info about referencing parents and available replacement versions.
        """
        with db.get_session(project_id) as session:
            version = session.query(ApplicationVersion).get(version_id)
            if not version:
                return {'error': 'Version not found'}

            if version.name == 'latest':
                return {'error': 'You cannot delete latest application version'}

            # Check if version is referenced by any parent agents/pipelines
            referencing_tools = session.query(EliteATool).filter(
                EliteATool.type == 'application',
                EliteATool.settings['application_version_id'].astext.cast(Integer) == version_id
            ).all()

            # Get detailed info about referencing parents
            referencing_parents = []
            for tool in referencing_tools:
                # Find all entity mappings for this tool to get parent info
                mappings = session.query(EntityToolMapping).filter(
                    EntityToolMapping.tool_id == tool.id
                ).all()

                for mapping in mappings:
                    # Get the parent application info
                    parent_version = session.query(ApplicationVersion).filter(
                        ApplicationVersion.id == mapping.entity_version_id
                    ).first()

                    if parent_version:
                        parent_app = session.query(Application).filter(
                            Application.id == parent_version.application_id
                        ).first()

                        if parent_app:
                            referencing_parents.append({
                                'application_id': parent_app.id,
                                'application_name': parent_app.name,
                                'application_type': parent_version.agent_type,
                                'version_id': parent_version.id,
                                'version_name': parent_version.name,
                                'tool_id': tool.id,
                            })

            # Get available versions for replacement (excluding the one being deleted)
            available_versions = session.query(ApplicationVersion).filter(
                ApplicationVersion.application_id == version.application_id,
                ApplicationVersion.id != version_id
            ).all()

            replacement_versions = [
                {
                    'id': v.id,
                    'name': v.name,
                    'created_at': v.created_at.isoformat() if v.created_at else None,
                }
                for v in available_versions
            ]

            return {
                # Only report in_use if there are actual valid parent references
                # Orphan tools (without valid mappings) will be cleaned up during delete
                'in_use': len(referencing_parents) > 0,
                'referencing_parents': referencing_parents,
                'replacement_versions': replacement_versions,
                'version_name': version.name,
                'application_id': version.application_id,
            }

    @web.rpc("applications_delete_application_version", "delete_application_version")
    def application_delete_version(
        self, project_id: int, version_id: int, replacement_version_id: int = None
    ) -> dict:
        """
        Delete an application version. If the version is in use and replacement_version_id
        is provided, update all references to the replacement version before deleting.
        """
        with db.get_session(project_id) as session:
            version = session.query(ApplicationVersion).get(version_id)
            if not version:
                return {'error': 'Version not found'}

            # Check if this version is marked as default
            if version.application.meta.get('default_version_id') == version_id:
                return {'error': 'You cannot delete the default application version'}

            if version.name == 'base':
                return {'error': 'You cannot delete base application version'}

            # Block deletion of published or embedded versions
            if version.status in (PublishStatus.published, PublishStatus.embedded):
                return {
                    'error': 'Cannot delete a published or embedded version. '
                             'Unpublish first.',
                    'blocked_status': version.status,
                }

            # Check if version is referenced by any parent agents/pipelines
            referencing_tools = session.query(EliteATool).filter(
                EliteATool.type == 'application',
                EliteATool.settings['application_version_id'].astext.cast(Integer) == version_id
            ).all()

            # Separate tools with valid parent mappings from orphan tools
            tools_with_parents = []
            orphan_tools = []

            for tool in referencing_tools:
                # Check if this tool has any valid parent mappings
                mappings = session.query(EntityToolMapping).filter(
                    EntityToolMapping.tool_id == tool.id
                ).all()

                has_valid_parent = False
                for mapping in mappings:
                    parent_version = session.query(ApplicationVersion).filter(
                        ApplicationVersion.id == mapping.entity_version_id
                    ).first()
                    if parent_version:
                        has_valid_parent = True
                        break

                if has_valid_parent:
                    tools_with_parents.append(tool)
                else:
                    orphan_tools.append(tool)

            # Delete orphan tools automatically (they have no valid parent)
            for orphan_tool in orphan_tools:
                session.delete(orphan_tool)

            if orphan_tools:
                log.info(f"Cleaned up {len(orphan_tools)} orphan tool references for version {version_id}")

            # Only require replacement if there are tools with valid parent mappings
            if tools_with_parents:
                if not replacement_version_id:
                    # Version is in use but no replacement provided - return error
                    return {
                        'error': 'Version is in use and no replacement version provided. '
                                 'Use check_version_in_use first to get replacement options.'
                    }

                # Verify the replacement version exists and belongs to the same application
                new_version = session.query(ApplicationVersion).get(replacement_version_id)
                if not new_version:
                    return {'error': f'Replacement version {replacement_version_id} not found'}

                if new_version.application_id != version.application_id:
                    return {'error': 'Replacement version must belong to the same application'}

                # Update all tools with valid parents to point to the new version
                updated_count = 0
                for tool in tools_with_parents:
                    new_settings = dict(tool.settings) if tool.settings else {}
                    new_settings['application_version_id'] = replacement_version_id
                    tool.settings = new_settings
                    updated_count += 1

                session.flush()
                log.info(f"Updated {updated_count} references from version {version_id} to {replacement_version_id}")

            # Capture version data before deletion for event
            version_data = {
                'id': version.id,
                'application_id': version.application_id,
                'project_id': project_id,
                'status': version.status,
                'meta': dict(version.meta) if version.meta else {},
            }

            # Delete the version
            session.delete(version)
            session.commit()

            self.context.event_manager.fire_event(
                ApplicationEvents.application_version_deleted, version_data
            )

            return {
                'ok': True,
                'updated_references': len(tools_with_parents),
                'cleaned_orphans': len(orphan_tools)
            }

    @web.rpc("applications_batch_replace_version_references", "batch_replace_version_references")
    def batch_replace_version_references(
        self,
        project_id: int,
        old_version_id: int,
        new_version_id: int,
        delete_old_version: bool = True
    ) -> dict:
        """
        Replace all references to old_version_id with new_version_id across all parent agents/pipelines,
        then optionally delete the old version.
        """
        with db.get_session(project_id) as session:
            # Verify the new version exists
            new_version = session.query(ApplicationVersion).get(new_version_id)
            if not new_version:
                return {'error': f'Replacement version {new_version_id} not found'}

            old_version = session.query(ApplicationVersion).get(old_version_id)
            if not old_version:
                return {'error': f'Version {old_version_id} not found'}

            # Find all tools referencing the old version
            referencing_tools = session.query(EliteATool).filter(
                EliteATool.type == 'application',
                EliteATool.settings['application_version_id'].astext.cast(Integer) == old_version_id
            ).all()

            updated_count = 0
            for tool in referencing_tools:
                # Update the tool's settings to point to new version
                new_settings = dict(tool.settings) if tool.settings else {}
                new_settings['application_version_id'] = new_version_id
                tool.settings = new_settings
                updated_count += 1

            session.flush()

            # Optionally delete the old version
            if delete_old_version:
                session.delete(old_version)

            session.commit()

            return {
                'ok': True,
                'updated_references': updated_count,
                'old_version_deleted': delete_old_version
            }

    @web.rpc("applications_get_application_version_details_expanded", "get_application_version_details_expanded")
    def get_application_version_details_expanded(
        self,
        project_id: int,
        application_id: int,
        version_id: int,
        user_id: int,
        **kwargs
    ) -> dict:
        try:
            version_details = get_application_version_details_expanded(
                project_id=project_id,
                application_id=application_id,
                version_id=version_id,
                user_id=user_id
            )
        except (ApplicationVersionNonFoundError, ApplicationToolExpandedError) as e:
            return {'error': str(e)}
        except Exception as e:
            log.error("Expanssion of application version details failed")
            log.error(e)
            import traceback
            log.error(traceback.format_exc())
            return {'error': f'Can not get application details for {version_id=}'}

        return version_details

    @web.rpc("applications_get_toolkit_by_id_expanded", "get_toolkit_by_id_expanded")
    def get_toolkit_by_id_expanded(
        self,
        project_id: int,
        toolkit_id: int,
        user_id: int,
        **kwargs
    ) -> dict:
        with db.get_session(project_id) as session:
            toolkit = session.query(EliteATool).where(
                EliteATool.id == toolkit_id,
            ).first()
            if not toolkit:
                return {}
            toolkit_dict = toolkit.to_json()
            toolkit_dict['project_id'] = project_id
            toolkit_dict['user_id'] = user_id
            try:
                toolkit_details = ToolValidatedDetails.model_validate(toolkit_dict)
            except ApplicationToolExpandedError as ex:
                return {'error': str(ex)}
            except Exception as ex:
                log.error(ex)
                return {'error': f"Error expanding toolkit settings for {toolkit_id=}"}

            toolkit_details_expanded = serialize(toolkit_details)

            return toolkit_details_expanded

    @web.rpc("applications_stop_task", "stop_task")
    def stop_task(
            self,
            task_id: str
    ):
        try:
            self.task_node.stop_task(task_id)
        except Exception as e:
            log.warning(f'Task was stopped')
            log.debug(f'Task stopped details {task_id}: {e}')

#    # Uncomment when really used
#    @web.rpc("applications_detect_pipelines")
#    def detect_pipelines(self, project_id: int, application_ids: List[int]):
#        result = set()
#        with db.get_session(project_id) as session:
#            applications = session.query(
#                Application
#            ).filter(
#                Application.id.in_(application_ids),
#            ).options(
#                joinedload(Application.versions)
#            ).all()
#
#            for app in applications:
#                for version in app.versions:
#                    if version.name == 'latest':
#                        if version.agent_type == AgentTypes.pipeline:
#                            result.add(app.id)
#                        break
#
#        return list(result)

    @web.rpc("applications_get_supported_index_documents", "get_supported_index_documents")
    def get_supported_index_documents(self):
        return deepcopy(self.index_types.get('document_types', {}))

    @web.rpc("elitea_core_get_index_types", "get_index_types")
    def get_index_types(self):
        try:
            return deepcopy(self.index_types)
        except AttributeError:
            log.error("index_types not initialized in elitea_core module")
            return {"image_types": {}, "document_types": {}}
        except Exception as e:
            log.error(f"Failed to get index_types: {e}")
            return None
