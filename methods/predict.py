from opentelemetry import trace as otel_trace

from pylon.core.tools import log, web

from sqlalchemy.orm import joinedload

from tools import VaultClient, db, serialize

from ..models.pd.chat import ApplicationChatRequest, ContextStrategyModel
from ..utils.predict_utils import generate_predict_payload, load_context_settings_from_conversation
from ..models.all import ApplicationVersion
from ..utils.utils import verify_signature
from ..utils.exceptions import VerifySignatureError
from ..utils.sio_utils import SioEvents

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

            application_version.project_id = project_id  # compatibility with pd model
            application_version_pd = ApplicationChatRequest.from_orm(
                application_version
            )

        parsed = application_version_pd.merge_update(parsed)

        # Load context_settings from conversation.meta if not provided
        if not parsed.context_settings:
            conversation_id = parsed.conversation_id or parsed.stream_id
            context_strategy = load_context_settings_from_conversation(parsed.project_id, conversation_id)
            if context_strategy:
                parsed.context_settings = ContextStrategyModel(**context_strategy)

        payload: dict = generate_predict_payload(parsed, user_id=user_id, eligible_for_autoapproval=True)

        user_context = {
            'user_id': user_id,
            'project_id': parsed.project_id,
        }

        task_id = self.task_node.start_task(
            "indexer_agent",
            args=[None, None],
            kwargs=payload,
            pool="agents",
            meta={
                "task_name": "indexer_agent",
                "project_id": parsed.project_id,
                'user_context': serialize(user_context),
            }
        )
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
