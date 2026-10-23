from opentelemetry import trace as otel_trace

from pylon.core.tools import log, web

from sqlalchemy.orm import joinedload

from tools import VaultClient, db, serialize

from ..models.pd.chat import ApplicationChatRequest, ContextStrategyModel
from ..utils.predict_utils import generate_predict_payload, load_context_settings_from_conversation, user_input_preview
from ..models.all import ApplicationVersion
from ..models.pd.admission import admission_request_from
from ..utils.budget_door import schema_may_use_llm
from ..utils.credential_slots import has_free_form_secret, mark_free_form_secret
from ..utils.governance import is_governed
from ..utils.mandate_admission import mandate_admission_request
from ..utils.trigger_principal import resolve_trigger_principal
from ..utils.utils import verify_signature
from ..utils.exceptions import VerifySignatureError, PoolSaturationError
from ..utils.pipeline_execution import TriggerType
from ..utils.input_guardrail import inspect_and_unsecret
from ..utils.sio_utils import SioEvents
from ..utils.vectorstore import get_pgvector_connection_string

class Method:

    @web.method()
    def do_predict(
        self,
        project_id: int,
        user_id: int,
        version_id: int,
        payload_in: dict,
        raw: bytes,
        webhook_signature=None,
        webhook_type="github",
        predict_wait=True,
        predict_timeout=float(60*60),  # 1 hour
        return_chat_history: bool = False,
        trigger_source: str = TriggerType.API,
    ):
        with db.get_session(project_id) as session:
            application_version = session.query(ApplicationVersion).options(
                joinedload(ApplicationVersion.application)
            ).get(version_id)

            if not application_version:
                return {
                    'error': f"Application version {version_id=} not found"
                }

            if webhook_signature is not None:
                secret = VaultClient(
                    project_id
                ).unsecret(application_version.application.webhook_secret)
                #
                if webhook_type == "github":
                    verify_signature(raw, secret, webhook_signature)
                elif webhook_type == "gitlab":
                    if webhook_signature != secret:
                        raise VerifySignatureError({"error": "x-gitlab-token token mismatch!"})
                elif webhook_type == "custom":
                    if ":" not in secret:
                        raise VerifySignatureError({"error": "format mismatch!"})
                    secret_header, secret_value = secret.split(":", 1)
                    if webhook_signature.get(secret_header) != secret_value:
                        raise VerifySignatureError({"error": "token mismatch!"})
                else:
                    raise VerifySignatureError({"error": "type mismatch!"})

            # Set entity context on the current OTEL span so that
            # the Flask tracing middleware records the application in audit events.
            try:
                span = otel_trace.get_current_span()
                if span and span.is_recording():
                    span.set_attribute('entity.type', 'application')
                    span.set_attribute('entity.id', str(application_version.application_id))
                    app_name = getattr(application_version.application, 'name', None)
                    if app_name:
                        span.set_attribute('entity.name', str(app_name))
            except Exception:
                pass

            # A governed webhook run belongs to the autonomous mandate of this agent version, whose owner it acts for.
            # The signature above is the proof the call is a real webhook; no mandate leaves it a system run, which
            # admission refuses (#6947). Governance off: no read, nothing changes.
            mandate_principal = None
            if webhook_signature is not None and is_governed(self):
                mandate_principal = resolve_trigger_principal(
                    session, trigger_kind="agent_webhook", project_id=project_id, version_id=version_id)
                if mandate_principal.kind == "mandate" and mandate_principal.owner_user_id is not None:
                    user_id = mandate_principal.owner_user_id
                else:
                    mandate_principal = None

            payload_in['project_id'] = project_id
            payload_in['version_id'] = version_id
            payload_in['application_id'] = application_version.application_id

            parsed = ApplicationChatRequest.parse_obj(payload_in)

            if not application_version.llm_settings:
                return {
                    'error':
                        f"Application version with id '{version_id}' "
                        f"was not found its settings, please provide it in request"
                }

            llm_free = not schema_may_use_llm(application_version.agent_type, application_version.instructions)
            application_version.project_id = project_id  # compatibility with pd model
            application_version_pd = ApplicationChatRequest.from_orm(
                application_version
            )

        parsed = application_version_pd.merge_update(parsed)

        # Set application_name AFTER merge to prevent client override (security)
        if version_id and application_version:
            parsed.application_name = application_version.application.name if application_version.application else None

        # Load context_settings from conversation.meta if not provided
        if not parsed.context_settings:
            conversation_id = parsed.conversation_id or parsed.stream_id
            context_strategy = load_context_settings_from_conversation(parsed.project_id, conversation_id)
            if context_strategy:
                parsed.context_settings = ContextStrategyModel(**context_strategy)

        payload: dict = generate_predict_payload(parsed, user_id=user_id, eligible_for_autoapproval=True, return_chat_history=return_chat_history)
        # Same as predict_sio: inspect, resolve {{secret.*}} and hand the worker its pgvector connstr pre-fork
        free_form_secret = has_free_form_secret(self, payload)   # governed: noticed before it is resolved (#6946)
        payload = inspect_and_unsecret(payload, VaultClient(parsed.project_id), parsed.project_id, user_id)
        payload['trigger_source'] = trigger_source  # usage analytics groups runs by it (BUG-12)
        mark_free_form_secret(payload, free_form_secret)
        try:
            payload['pgvector_connstr'] = get_pgvector_connection_string(parsed.project_id)
        except Exception as e:  # pylint: disable=W0703
            log.warning(
                "Failed to prefetch pgvector_connstr for project_id=%s: %s",
                parsed.project_id, e
            )

        user_context = {
            'user_id': user_id,
            'project_id': parsed.project_id,
        }

        # Governed runs are admitted from the authenticated HTTP caller and the version row loaded above, never
        # from the user_id argument or the payload; a webhook has no person behind it. None when governance is off
        if mandate_principal is not None:
            admission = mandate_admission_request(
                project_id=parsed.project_id, binding_project_id=project_id, version_row=application_version,
                principal=mandate_principal, entry_point="webhook", trigger_ref=f"webhook:version:{version_id}",
            )
        else:
            admission = admission_request_from(
                self, sid=None, project_id=parsed.project_id, binding_project_id=project_id,
                version_row=application_version, run_kind="system" if webhook_signature is not None else "interactive",
                entry_point="predict_api",
            )
        task_id = self.task_node.start_task(
            "indexer_agent",
            args=[None, None],
            kwargs=payload,
            pool="agents",
            admission=admission,
            meta={
                "task_name": "indexer_agent",
                "project_id": parsed.project_id,
                'user_context': serialize(user_context),
                'user_input_preview': user_input_preview(payload.get("user_input")),
                # Pure REST predict (webhook / async / blocking API) — never has a
                # Socket.IO consumer, so suppress streaming/UI-only events.
                'non_interactive': True,
                'llm_free': llm_free,
            }
        )

        # Handle pool saturation: start_task returns None when no workers available
        if task_id is None:
            log.warning(
                "Pool 'agents' saturated - no workers available for project_id=%s",
                parsed.project_id
            )
            raise PoolSaturationError(pool="agents", retry_after=5)

        if webhook_signature is not None or not predict_wait:
            result = {
                "message": "Task started",
                "task_id": task_id,
            }
        else:
            result = self.task_node.join_task(task_id, timeout=predict_timeout)
            if result is ...:
                self.task_node.stop_task(task_id)
                return {"error": "Timeout"}

        return result
