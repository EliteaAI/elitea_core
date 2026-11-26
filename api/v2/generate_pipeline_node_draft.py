"""EL-7325: draft one pipeline node with AI (Build / Edit with AI from the node panel).

The model is the pipeline version's own, never the project default (AC-31). The prompt is
assembled here from service prompts; the answer is validated, repaired once and merged in
``utils/node_draft`` and returned as the full node the editor applies.
"""
import time

from flask import request
from pydantic import ValidationError

from pylon.core.tools import log
from tools import api_tools, auth, config as c, register_openapi, rpc_tools

from ...models.pd.node_draft import (
    LIMITS,
    DraftTarget,
    GeneratePipelineNodeDraftRequest,
    GeneratePipelineNodeDraftResponse,
    ModelReply,
)
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.draft_llm_utils import (
    answer_was_cut_short,
    describe_predict_failure,
    extract_answer,
    resolve_model,
    timeout_response,
)
from ...utils.exceptions import PoolSaturationError
from ...utils.node_draft.context import needs_resources, parse_pipeline, select_resources
from ...utils.node_draft.prompt import (
    build_messages,
    default_lookups,
    prompt_keys,
    registry_entry,
    resolve_prompt,
    resolve_spec,
    shipped_defaults,
)
from ...utils.node_draft.resources import index_resources, load_candidates, prompt_view
from ...utils.node_draft.schema import SUPPORTED_TYPES, defaults
from ...utils.node_draft.service import draft_llm_settings, run_draft
from ...utils.predict_utils import PredictPayloadError
from ...utils.utils import extract_json_from_text



class _StopDraft(Exception):
    def __init__(self, body: dict, status: int):
        super().__init__(body.get('error'))
        self.body, self.status = body, status


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name='Generate Pipeline Node Draft',
        description='Draft the configuration of one pipeline node from a plain-language requirement.',
        request_body=GeneratePipelineNodeDraftRequest,
        response_model=GeneratePipelineNodeDraftResponse,
        tags=['elitea_core/applications'],
        mcp_tool=False,
        available_to_users=False,
    )
    @auth.decorators.check_api({
        'permissions': ['models.applications.applications.update'],
        'recommended_roles': {
            c.ADMINISTRATION_MODE: {'admin': True, 'editor': True, 'viewer': False},
            c.DEFAULT_MODE: {'admin': True, 'editor': True, 'viewer': False},
        },
    })
    @api_tools.endpoint_metrics
    def post(self, project_id: int):
        started = time.monotonic()
        if (request.content_length or 0) > LIMITS.max_body_bytes:
            return {'error': 'This pipeline is too large for AI drafting.'}, 413
        try:
            req = GeneratePipelineNodeDraftRequest.model_validate(request.json)
        except ValidationError as exc:
            return {'error': 'Invalid request.', 'details': exc.errors(include_url=False)}, 400
        if req.node_type not in SUPPORTED_TYPES:
            return {'error': f'Node type {req.node_type} is not supported by AI.'}, 400

        user_id = auth.current_user().get('id')
        try:
            version = _load_version(project_id, req.application_id, req.version_id)
            llm_settings = _llm_settings(project_id, req)
            target, context = _build_target(project_id, user_id, req, version)
        except _StopDraft as stop:
            return stop.body, stop.status

        rpc = rpc_tools.RpcMixin().rpc
        project_lookup, public_lookup = default_lookups(project_id, rpc)
        shipped = shipped_defaults()
        core_key, _spec_key = prompt_keys('edit' if req.draft_node else req.mode, req.node_type)
        system, user = build_messages(
            core=resolve_prompt(core_key, project_lookup, public_lookup, shipped),
            spec=resolve_spec(req.node_type, project_lookup, public_lookup, shipped, registry_entry),
            context=context,
            requirement=req.requirement,
        )

        body, status = run_draft(
            system=system, user=user, target=target, llm_settings=llm_settings,
            call_model=self._model_caller(project_id, user_id), extract_json=extract_json_from_text,
        )
        log.info('pipeline_node_ai.generate node_type=%s mode=%s model=%s status=%s duration_ms=%d',
                 req.node_type, req.mode, llm_settings.get('model_name'), status,
                 int((time.monotonic() - started) * 1000))
        return body, status

    def _model_caller(self, project_id: int, user_id: int):
        def call(system, messages, llm_settings, timeout):
            budget = max(1, int(timeout))
            try:
                result = self.module.predict_sio_llm(
                    sid=None,
                    data={
                        'project_id': project_id,
                        'user_input': messages[-1]['content'],
                        'chat_history': messages[:-1],
                        'instructions': system,
                        'llm_settings': llm_settings,
                        'await_task_timeout': budget,
                    },
                    await_task_timeout=budget,
                    user_id=user_id,
                    skip_expansion=True,
                    return_chat_history=True,
                )
            except PredictPayloadError as exc:
                return ModelReply(error=str(exc))
            except PoolSaturationError:
                return ModelReply(error='The service is busy. Please try again in a few seconds.')
            except Exception:  # pylint: disable=W0703
                log.exception('generate_pipeline_node_draft: LLM call failed')
                return ModelReply(error='LLM generation failed.')
            if timeout_response(result, budget):
                return ModelReply(timed_out=True)
            text = extract_answer(result)
            if not text:
                return ModelReply(error=describe_predict_failure(result) or 'LLM returned an empty response.')
            return ModelReply(text=text, cut_short=answer_was_cut_short(result, extract_json_from_text(text)))
        return call


def _load_version(project_id: int, application_id: int, version_id: int):
    # pylint: disable=C0415
    from tools import db
    from ...models.all import ApplicationVersion

    with db.with_project_schema_session(project_id) as session:
        version = session.query(ApplicationVersion).filter(
            ApplicationVersion.id == version_id, ApplicationVersion.application_id == application_id,
        ).first()
        if version is None:
            raise _StopDraft({'error': 'Pipeline or version not found.'}, 404)
        return {'pipeline_settings': dict(version.pipeline_settings or {})}


def _llm_settings(project_id: int, req: GeneratePipelineNodeDraftRequest) -> dict:
    requested = req.llm_settings.model_dump(exclude_none=True) if req.llm_settings else None
    if not requested or not requested.get('model_name'):
        raise _StopDraft({'error': 'Select a model for this pipeline to use AI.', 'code': 'no_model'}, 400)
    unavailable, owning_project_id = resolve_model(
        project_id, requested['model_name'], requested.get('model_project_id'),
    )
    if unavailable:
        raise _StopDraft({'error': unavailable, 'code': 'model_unavailable'}, 400)
    if owning_project_id is not None:
        requested['model_project_id'] = owning_project_id
    try:
        config = rpc_tools.RpcMixin().rpc.timeout(5).configurations_get_configuration_model(
            requested.get('model_project_id') or project_id, requested['model_name'],
        ) or {}
    except Exception:  # pylint: disable=W0703
        config = {}
    return draft_llm_settings(requested, req.node_type, bool(config.get('supports_reasoning')))


def _build_target(project_id: int, user_id: int, req: GeneratePipelineNodeDraftRequest, version: dict):
    # pylint: disable=C0415
    from ...utils.pipeline_trigger import get_trigger_from_pipeline_settings

    trigger = get_trigger_from_pipeline_settings(version['pipeline_settings']).get('type')
    try:
        parsed = parse_pipeline(req.pipeline_yaml, req.node_id, trigger=trigger)
    except ValueError as exc:
        raise _StopDraft({'error': str(exc)}, 400) from exc

    candidates, offered = [], []
    if needs_resources(req.node_type):
        raw = load_candidates(project_id, user_id, req.version_id, req.node_type, req.application_id)
        candidates = select_resources(raw, req.requirement, attached={c['name'] for c in raw if c['attached']})
        # The prompt trims tool params for low-ranked resources; validation must see all of them
        picked = {c['name'] for c in candidates}
        offered = [c for c in raw if c['name'] in picked]

    current = req.current_node or {}
    current = {**current, 'id': req.node_id, 'type': req.node_type}
    mode = 'edit' if (req.mode == 'edit' or req.draft_node) else 'build'
    node_view = current if mode == 'edit' else {'id': req.node_id, 'type': req.node_type, **defaults(req.node_type)}
    context = {
        'node': {'id': req.node_id, 'type': req.node_type, 'current': node_view},
        'state': parsed.state,
        'nodes': [{'id': nid, 'type': _type_of(parsed, nid)} if nid != 'END' else {'id': 'END'}
                  for nid in parsed.node_ids if nid != req.node_id],
        'incoming': parsed.incoming,
        'entry_point': parsed.entry_point,
        'unattended': parsed.unattended,
    }
    if req.draft_node:
        context['node']['draft'] = req.draft_node
    if candidates:
        context['resources'] = prompt_view(candidates)

    target = DraftTarget(
        node_id=req.node_id, node_type=req.node_type, mode=mode, current=current,
        base=({**req.draft_node, 'id': req.node_id, 'type': req.node_type} if req.draft_node else None),
        state={s['name']: s['type'] for s in parsed.state},
        node_ids=set(parsed.node_ids) | {req.node_id},
        resources=index_resources(offered),
    )
    return target, context


def _type_of(parsed, node_id):
    return next((n.get('type') for n in parsed.nodes if n.get('id') == node_id), None)


class API(api_tools.APIBase):  # pylint: disable=R0903
    url_params = api_tools.with_modes(['<int:project_id>'])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI,
    }
