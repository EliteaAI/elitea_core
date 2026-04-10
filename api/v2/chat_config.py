"""Chat configuration endpoint exposing attachment limits from vault secrets."""

from tools import api_tools, auth, config as c, VaultClient

from ...utils.constants import PROMPT_LIB_MODE


class PromptLibAPI(api_tools.APIModeHandler):
    @auth.decorators.check_api({
        "permissions": ["models.chat.conversation.details"],
        "recommended_roles": {
            c.ADMINISTRATION_MODE: {"admin": True, "editor": True, "viewer": False},
            c.DEFAULT_MODE: {"admin": True, "editor": True, "viewer": True},
        }})
    @api_tools.endpoint_metrics
    def get(self, project_id: int, **kwargs):
        """Return chat attachment limits from vault configuration."""
        vault_client = VaultClient(project_id)
        secrets = vault_client.get_all_secrets()
        return {
            "chat_max_upload_count": int(secrets.get('chat_max_upload_count', 10)),
            "chat_max_upload_size_mb": int(secrets.get('chat_max_upload_size_mb', 150)),
            "chat_max_file_upload_size_mb": int(secrets.get('chat_max_file_upload_size_mb', 150)),
            "chat_max_image_upload_count": int(secrets.get('chat_max_image_upload_count', 10)),
            "chat_max_image_upload_size_mb": int(secrets.get('chat_max_image_upload_size_mb', 3)),
        }, 200


class API(api_tools.APIBase):
    url_params = api_tools.with_modes([
        '<int:project_id>',
    ])

    mode_handlers = {
        PROMPT_LIB_MODE: PromptLibAPI
    }
