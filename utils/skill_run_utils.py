from dataclasses import dataclass
from typing import Optional
from uuid import uuid4

from pydantic import ValidationError
from sqlalchemy.orm import selectinload

from pylon.core.tools import log
from tools import auth, db

from .application_utils import validate_and_resolve_llm_settings
from .exceptions import BudgetDoorClosedError, MaintenanceInProgressError, PoolSaturationError
from .mcp_versioning import INTERNAL_MCP_ENVIRON_KEY
from .predict_utils import get_project_context
from .project_context_utils import prepare_project_context_delivery
from .sio_utils import SioValidationError
from .usage_attribution import skill_attribution
from ..models.enums.all import PublishStatus
from ..models.pd.skill_predict import SkillPredictMcpRequest, SkillPredictRequest
from ..models.skill import Skill


SKILL_RUN_AGENT_TYPE = 'openai'
SKILL_RUN_TIMEOUT_SECONDS = 60 * 60
NO_MODEL_ERROR = 'No LLM model is configured for this project'
NOT_PUBLISHED_ERROR = 'Skill version is not published'
EMPTY_INSTRUCTIONS_ERROR = 'Skill version has no instructions'
SOCKET_NOT_OWNED_ERROR = 'Socket session does not belong to the caller'
SOCKET_NOT_IN_PROJECT_ERROR = 'Socket session is not authorized for this project. Please refresh the page and try again.'


class SkillRunError(Exception):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class SkillRunTarget:
    skill_id: int
    skill_name: str
    version_id: int
    version_name: str
    instructions: str
    icon_meta: Optional[dict]
    run_settings: dict


@dataclass(frozen=True)
class SkillRun:
    data: dict
    usage_entity: dict
    applied_skills: list
    platform_run_id: str
    stream_id: str
    target: SkillRunTarget
    llm_settings: dict
    model_fallback: bool

    def meta(self) -> dict:
        return {
            'invoked_skills': self.applied_skills,
            'skill_run': {
                'skill_id': self.target.skill_id,
                'skill_version_id': self.target.version_id,
                'version_name': self.target.version_name,
            },
        }

    def describe(self) -> dict:
        return {
            'run_id': self.platform_run_id,
            'stream_id': self.stream_id,
            'skill_id': self.target.skill_id,
            'skill_version_id': self.target.version_id,
            'version_name': self.target.version_name,
            'model': {
                'model_name': self.llm_settings.get('model_name'),
                'model_project_id': self.llm_settings.get('model_project_id'),
            },
            'model_fallback': self.model_fallback,
            'meta': self.meta(),
        }


def is_published(version) -> bool:
    return version.status == PublishStatus.published


def newest_published_version(skill):
    published = [v for v in skill.versions if is_published(v)]
    return max(published, key=lambda v: v.created_at, default=None)


def saved_run_settings(version) -> dict:
    return getattr(version, 'run_settings', None) or {}


def select_skill_version(skill, version_id: Optional[int], published_only: bool):
    if version_id is not None:
        version = next((v for v in skill.versions if v.id == version_id), None)
        if version is None:
            raise SkillRunError(f"Skill version '{version_id}' not found", 404)
        if published_only and not is_published(version):
            raise SkillRunError(NOT_PUBLISHED_ERROR)
        return version

    version = newest_published_version(skill) if published_only else skill.get_default_version()
    if version is None:
        raise SkillRunError('Skill has no runnable version', 404)
    return version


def load_skill_run_target(
    project_id: int, skill_id: int, version_id: Optional[int], published_only: bool,
) -> SkillRunTarget:
    with db.with_project_schema_session(project_id) as session:
        skill = session.query(Skill).options(
            selectinload(Skill.versions)
        ).filter(Skill.id == skill_id).first()
        if skill is None:
            raise SkillRunError('Skill not found', 404)
        version = select_skill_version(skill, version_id, published_only)
        if not (version.instructions or '').strip():
            raise SkillRunError(EMPTY_INSTRUCTIONS_ERROR)
        return SkillRunTarget(
            skill_id=skill.id,
            skill_name=skill.name,
            version_id=version.id,
            version_name=version.name,
            instructions=version.instructions,
            icon_meta=(version.meta or {}).get('icon_meta'),
            run_settings=saved_run_settings(version),
        )


def merge_llm_override(saved: Optional[dict], override: Optional[dict]) -> dict:
    saved, override = saved or {}, override or {}
    overrides_model_only = 'model_name' in override and 'model_project_id' not in override
    if overrides_model_only:
        saved = {key: value for key, value in saved.items() if key != 'model_project_id'}
    return {**saved, **override}


def is_model_replaced(requested: dict, resolved: dict) -> bool:
    requested_name = requested.get('model_name')
    if not requested_name:
        return False
    if resolved.get('model_name') != requested_name:
        return True
    requested_project_id = requested.get('model_project_id')
    return requested_project_id is not None and resolved.get('model_project_id') != requested_project_id


def resolve_skill_llm_settings(
    caller_project_id: int, saved: Optional[dict], override: Optional[dict],
) -> tuple[dict, bool]:
    requested = merge_llm_override(saved, override)
    resolved = validate_and_resolve_llm_settings(caller_project_id, requested) or {}
    if not resolved.get('model_name'):
        raise SkillRunError(NO_MODEL_ERROR)
    return resolved, is_model_replaced(requested, resolved)


def build_skill_run(
    *,
    caller_project_id: int,
    skill_project_id: int,
    target: SkillRunTarget,
    user_input,
    chat_history: Optional[list] = None,
    llm_override: Optional[dict] = None,
) -> SkillRun:
    llm_settings, model_fallback = resolve_skill_llm_settings(
        caller_project_id, target.run_settings.get('llm_settings'), llm_override,
    )
    ignore_project_context = bool(target.run_settings.get('ignore_project_context', False))

    instructions, runtime_project_context = target.instructions, None
    if not ignore_project_context:
        instructions, runtime_project_context = prepare_project_context_delivery(
            target.instructions, get_project_context(caller_project_id),
        )

    stream_id = str(uuid4())
    data = {
        'project_id': skill_project_id,
        'stream_id': stream_id,
        'message_id': stream_id,
        'application_name': target.skill_name,
        'version_name': target.version_name,
        'version_details': {
            'agent_type': SKILL_RUN_AGENT_TYPE,
            'instructions': instructions,
            'llm_settings': llm_settings,
            'tools': [],
            'meta': {'internal_tools': [], 'ignore_project_context': ignore_project_context},
        },
        'llm_settings': llm_settings,
        'user_input': user_input,
        'chat_history': chat_history or [],
        'tools': [],
        'internal_tools': [],
    }
    if runtime_project_context:
        data['project_context'] = runtime_project_context

    return SkillRun(
        data=data,
        usage_entity=skill_attribution(target.skill_id, target.version_id, target.skill_name),
        applied_skills=[{
            'skill_id': target.skill_id,
            'name': target.skill_name,
            'icon_meta': target.icon_meta,
        }],
        platform_run_id=str(uuid4()),
        stream_id=stream_id,
        target=target,
        llm_settings=llm_settings,
        model_fallback=model_fallback,
    )


def socket_owner_id(sid: str) -> Optional[int]:
    auth_data = auth.sio_users.get(sid)
    if auth_data is None:
        return None
    return auth.current_user(auth_data=auth_data).get('id')


def check_socket_access(sid: str, user_id: int, project_id: int) -> None:
    if socket_owner_id(sid) != user_id:
        raise SkillRunError(SOCKET_NOT_OWNED_ERROR, 403)
    if not auth.is_sio_user_in_project(sid, project_id):
        raise SkillRunError(SOCKET_NOT_IN_PROJECT_ERROR, 403)


def dispatch_skill_run(
    module, run: SkillRun, *, caller_project_id: int, user_id: int, wait: bool, return_chat_history: bool,
    sid: Optional[str] = None,
) -> dict:
    outcome = module.predict_sio(
        sid=sid,
        data=run.data,
        start_event_content=run.meta(),
        chat_project_id=caller_project_id,
        await_task_timeout=SKILL_RUN_TIMEOUT_SECONDS if wait else -1,
        user_id=user_id,
        return_chat_history=return_chat_history,
        platform_run_id=run.platform_run_id,
        usage_entity=run.usage_entity,
        applied_skills=run.applied_skills,
        sid_project_id=caller_project_id,
    )
    if outcome is None:
        raise SkillRunError(SOCKET_NOT_IN_PROJECT_ERROR, 403)
    return outcome


def json_safe_validation_errors(errors: list) -> list:
    return [
        {key: item[key] for key in ('type', 'loc', 'msg') if key in item} if isinstance(item, dict) else str(item)
        for item in errors
    ]


def sio_error_message(error: SioValidationError):
    if isinstance(error.error, dict) and 'error' in error.error:
        return error.error['error']
    if isinstance(error.error, list):
        return json_safe_validation_errors(error.error)
    return str(error.error)


def sio_error_response(error: SioValidationError) -> tuple[dict | list, int]:
    refusal = error.__context__
    if isinstance(refusal, BudgetDoorClosedError):
        return refusal.body(), 429
    if isinstance(refusal, MaintenanceInProgressError):
        return {'error': 'maintenance_in_progress', 'message': sio_error_message(error)}, 503
    return {'error': sio_error_message(error)}, 400


def run_response(run: SkillRun, outcome: dict, wait: bool) -> tuple[dict, int]:
    if outcome.get('error') == 'maintenance_in_progress':
        return outcome, 503
    if not wait:
        return {'message': 'Task started', 'task_id': outcome['task_id'], **run.describe()}, 200
    if 'result' not in outcome:
        return {'error': 'Timeout', 'task_id': outcome.get('task_id'), **run.describe()}, 400
    result = outcome['result']
    if isinstance(result, dict) and result.get('error') is not None:
        return {'result': result, 'error': str(result['error']), **run.describe()}, 400
    return {'result': result, **run.describe()}, 200


def request_model_for(environ) -> type[SkillPredictMcpRequest]:
    return SkillPredictMcpRequest if environ.get(INTERNAL_MCP_ENVIRON_KEY) else SkillPredictRequest


def is_async_query(args) -> bool:
    return args.get('async', 'no').lower().strip() in ('yes', 'true')


def execute_skill_predict(
    module,
    body,
    *,
    caller_project_id: int,
    skill_project_id: int,
    skill_id: int,
    version_id: Optional[int],
    published_only: bool,
    user_id: int,
    async_requested: bool = False,
    request_model: type[SkillPredictMcpRequest] = SkillPredictRequest,
) -> tuple[dict | list, int]:
    try:
        request = request_model.model_validate(body if body is not None else {})
    except ValidationError as e:
        return e.errors(include_url=False, include_context=False), 400

    sid = getattr(request, 'sid', None)
    callback_url = getattr(request, 'callback_url', None)
    wait = not (getattr(request, 'async_mode', False) or callback_url is not None or async_requested or sid)
    module.not_starting_task_event.clear()
    try:
        if sid:
            check_socket_access(sid, user_id, caller_project_id)
        target = load_skill_run_target(skill_project_id, skill_id, version_id, published_only)
        run = build_skill_run(
            caller_project_id=caller_project_id,
            skill_project_id=skill_project_id,
            target=target,
            user_input=request.user_input,
            chat_history=request.chat_history,
            llm_override=request.llm_settings.model_dump(exclude_unset=True) if request.llm_settings else None,
        )
        outcome = dispatch_skill_run(
            module, run,
            caller_project_id=caller_project_id,
            user_id=user_id,
            wait=wait,
            return_chat_history=request.return_chat_history,
            sid=sid,
        )
        if callback_url is not None and outcome.get('task_id'):
            module.callback_tasks[outcome['task_id']] = {
                'callback_url': callback_url,
                'callback_headers': request.callback_headers,
            }
    except SkillRunError as e:
        return {'error': e.message}, e.status_code
    except SioValidationError as e:
        return sio_error_response(e)
    except BudgetDoorClosedError as e:
        return e.body(), 429
    except PoolSaturationError as e:
        return {
            'error': 'temporarily_unavailable',
            'message': 'The service is busy processing other requests. Please try again in a few seconds.',
            'retry_after': e.retry_after,
        }, 503
    except Exception as e:  # pylint: disable=W0718
        log.exception('Skill predict error: %s', e)
        return {'error': 'Can not run skill'}, 500
    finally:
        module.not_starting_task_event.set()

    return run_response(run, outcome, wait)
