"""The chat's credential form (#7228, C-1009): the person creates the credential a paused governed run asked for.

``POST .../credential_form/prompt_lib/<project_id>/<conversation_uuid>`` ``{interrupt_id, message_id, credential_type,
title, fields, ownership, auth_mode?, delegation?}`` (``utils/credential_form``). The person's own signed-in session
only (no API token, no run token), ``governance.request`` in the project, governance enforced. Answers
``{reference_id, configuration_id, configuration_uuid, configuration_title, title, credential_type, state, requests}``:
never a value. It does not resume the run.
"""
from flask import request
from pydantic import ValidationError
from pylon.core.tools import log
from tools import api_tools, auth, register_openapi
from werkzeug.exceptions import RequestEntityTooLarge

from ...models.pd.credential_form import CredentialFormBody
from ...utils import binding_api as api
from ...utils.constants import PROMPT_LIB_MODE
from ...utils.credential_form import FormRefused, complete

MAX_BODY_BYTES = 64 * 1024


def _body():
    """``(body, None)`` or ``(None, response)``; a refusal names fields only, never a value."""
    if request.content_length is not None and request.content_length > MAX_BODY_BYTES:
        return None, api.refusal("PAYLOAD_TOO_LARGE")
    request.max_content_length = MAX_BODY_BYTES
    try:
        raw = request.get_json(silent=True)
    except RequestEntityTooLarge:
        return None, api.refusal("PAYLOAD_TOO_LARGE")
    if not isinstance(raw, dict):
        return None, (FormRefused("INVALID_REQUEST").body(), 400)
    try:
        return CredentialFormBody.model_validate(raw), None
    except ValidationError as exc:
        fields = sorted({str(error["loc"][0]) for error in exc.errors()
                         if error["loc"] and error["loc"][0] in CredentialFormBody.model_fields})
        body = FormRefused("INVALID_REQUEST").body()
        if fields:
            body["fields"] = fields
        return None, (body, 400)


class PromptLibAPI(api_tools.APIModeHandler):
    @register_openapi(
        name="Create Credential From Chat",
        description=("The person creates the credential a paused governed run asked for (the chat's credential form): "
                     "the configuration, its governed reference (personal, or a team one to approve), its value and the "
                     "requests the run's agent needs. Session only; the value never reaches the model."),
        tags=[api.OPENAPI_TAG],
        parameters=[{"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}},
                    {"name": "conversation_uuid", "in": "path", "required": True, "schema": {"type": "string"}}],
        responses={"200": {"description": "{reference_id, configuration_id, title, credential_type, state, requests}"},
                   "400": {"description": "Not valid."}, "403": {"description": "Not allowed."},
                   "404": {"description": "No such waiting request."}, "409": {"description": "Not enforced."}},
        mcp_tool=False, available_to_users=False,
    )
    @auth.decorators.check_api({"permissions": [api.REQUEST], "recommended_roles": api.REQUEST_ROLES})
    def post(self, project_id: int, conversation_uuid: str, **kwargs):
        from ...utils.governance import is_governed  # pylint: disable=C0415
        if not is_governed(self.module):
            return api.refusal("PROJECT_GOVERNANCE_OFF")
        actor = api.session_actor()   # the person's own session: never a token's owner, never a run
        if actor is None:
            return api.refusal("FORBIDDEN", "The credential form needs your own signed-in session.")
        body, error = _body()
        if error:
            return error
        try:
            return complete(self.module, project_id, conversation_uuid, body, actor), 200
        except FormRefused as exc:
            return exc.body(), exc.status
        except Exception as exc:  # pylint: disable=W0703
            log.error("credential form: failed (%s)", type(exc).__name__)
            return FormRefused("DEPENDENCY_UNAVAILABLE").body(), 503


class API(api_tools.APIBase):
    url_params = api_tools.with_modes(["<int:project_id>/<string:conversation_uuid>"])
    mode_handlers = {PROMPT_LIB_MODE: PromptLibAPI}
