from typing import Optional

from pydantic import ValidationError

from pylon.core.tools import log
from tools import rpc_tools

from ..models.pd.skill_run_settings import SkillRunSettingsWriteModel


MODEL_BINDING_FIELDS = ('model_name', 'model_project_id', 'selection')
SHARED_MODEL_REQUIRED = 'Select a shared model or clear the model to publish this skill'


def selection_mode(llm_settings: Optional[dict]) -> Optional[str]:
    return ((llm_settings or {}).get('selection') or {}).get('mode')


def without_model_binding(llm_settings: Optional[dict]) -> dict:
    return {k: v for k, v in (llm_settings or {}).items() if k not in MODEL_BINDING_FIELDS}


def available_llm_models(project_id: int) -> Optional[dict]:
    try:
        return rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_available_models(
            project_id=project_id, section='llm', include_shared=True,
        )
    except Exception as exc:  # pylint: disable=W0718
        log.warning('Could not list LLM models of project %s: %s', project_id, exc)
        return None


def is_auto_routing_enabled(project_id: int) -> bool:
    try:
        settings = rpc_tools.RpcMixin().rpc.timeout(3).configurations_get_auto_routing_settings(project_id)
    except Exception as exc:  # pylint: disable=W0718
        log.warning('Could not read Auto routing settings of project %s: %s', project_id, exc)
        return False
    return bool((settings or {}).get('enabled'))


def find_model_project(available: dict, model_name: str, model_project_id: Optional[int]) -> Optional[int]:
    if (model_project_id, model_name) in available:
        return model_project_id
    return next((project for (project, name) in available if name == model_name), None)


def rebind_llm_settings(project_id: int, llm_settings: dict) -> dict:
    model_name = llm_settings.get('model_name')
    if not model_name or selection_mode(llm_settings) == 'auto':
        return llm_settings
    available = available_llm_models(project_id)
    if available is None:
        return llm_settings
    model_project_id = find_model_project(available, model_name, llm_settings.get('model_project_id'))
    if model_project_id is None:
        log.warning(
            'Skill run settings: model %r is not available in project %s; it was dropped',
            model_name, project_id,
        )
        return without_model_binding(llm_settings)
    rebound = {**llm_settings, 'model_project_id': model_project_id}
    if selection_mode(rebound) == 'fixed':
        rebound['selection'] = {
            **rebound['selection'],
            'model_ref': {'name': model_name, 'project_id': model_project_id},
        }
    return rebound


def validated_run_settings(raw) -> Optional[dict]:
    try:
        return SkillRunSettingsWriteModel.model_validate(raw).model_dump(exclude_none=True)
    except ValidationError as exc:
        log.warning('Skill run settings: invalid model settings were dropped: %s', exc.errors())
    ignore_project_context = raw.get('ignore_project_context') if isinstance(raw, dict) else None
    if isinstance(ignore_project_context, bool):
        return {'ignore_project_context': ignore_project_context}
    return None


def portable_run_settings(project_id: int, raw) -> Optional[dict]:
    if not raw:
        return None
    settings = validated_run_settings(raw)
    if settings and settings.get('llm_settings'):
        settings['llm_settings'] = rebind_llm_settings(project_id, settings['llm_settings'])
    return settings


def is_requested_model_usable(project_id: int, llm_settings: dict) -> bool:
    mode = selection_mode(llm_settings)
    if mode == 'auto':
        return is_auto_routing_enabled(project_id)
    if mode == 'fixed':
        available = available_llm_models(project_id)
        binding = (llm_settings.get('model_project_id'), llm_settings.get('model_name'))
        return available is None or binding in available
    return True


def unshared_model_issue(run_settings: Optional[dict], public_project_id: int) -> Optional[str]:
    llm_settings = (run_settings or {}).get('llm_settings') or {}
    model_name = llm_settings.get('model_name')
    if not model_name or selection_mode(llm_settings) == 'auto':
        return None
    model_project_id = llm_settings.get('model_project_id')
    if model_project_id is not None and int(model_project_id) == int(public_project_id):
        return None
    return f"Model '{model_name}' is not a shared model"
