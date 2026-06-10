from pylon.core.tools import web, log
from tools import rpc_tools, db

from ..utils.mcp_client import is_client_connected


class RPC:
    @web.rpc('mcp_servers_handler')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def mcp_servers_handler(self):
        self.servers_storage.validate_all(is_client_connected)

    @web.rpc('chat_add_mcp_toolkits_to_conversation')
    @rpc_tools.wrap_exceptions(RuntimeError)
    def add_mcp_toolkits_to_conversation_rpc(
        self,
        support_project_id: int,
        user_id: int,
        conversation_id: int,
        mcp_endpoints: list,
        auth_headers: dict = None,
    ) -> None:
        """
        Create remote MCP EliteATool records in the support project and
        add them as participants to the given conversation.

        Each entry in mcp_endpoints must have: url, name, description.
        """
        from ..models.elitea_tools import EliteATool
        from ..models.conversation import Conversation
        from ..models.enums.all import ParticipantTypes
        from ..models.pd.participant import ParticipantCreate, ParticipantEntityToolkit
        from ..utils.participant_utils import add_participant_to_conversation

        metadata = {
            "categories": ["other"],
            "extra_categories": ["remote tools", "sse", "http"],
            "has_function_validators": False,
            "check_connection_supported": False,
            "mcp": True,
            "support_auto": True,
        }

        with db.get_session(support_project_id) as session:
            conversation = session.query(Conversation).filter(
                Conversation.id == conversation_id
            ).first()

            if not conversation:
                log.error(f"[MCP RPC] Conversation {conversation_id} not found in project {support_project_id}")
                return

            for endpoint in mcp_endpoints:
                try:
                    url = endpoint["url"]
                    settings = {"url": url}
                    if auth_headers:
                        settings["headers"] = auth_headers

                    toolkit = session.query(EliteATool).filter(
                        EliteATool.type == "mcp",
                        EliteATool.settings.op('->>')('url') == url,
                    ).first()

                    if toolkit is None:
                        toolkit = EliteATool(
                            type="mcp",
                            name=endpoint["name"],
                            description=endpoint.get("description", ""),
                            settings=settings,
                            author_id=user_id,
                            meta=metadata,
                        )
                        session.add(toolkit)
                        session.flush()
                        log.info(f"[MCP RPC] Created MCP toolkit '{endpoint['name']}' (id={toolkit.id})")
                    else:
                        toolkit.settings = settings
                        session.flush()
                        log.info(f"[MCP RPC] Updated MCP toolkit '{endpoint['name']}' (id={toolkit.id})")

                    participant_data = ParticipantCreate(
                        entity_name=ParticipantTypes.toolkit,
                        entity_meta=ParticipantEntityToolkit(
                            id=toolkit.id,
                            project_id=support_project_id,
                        ),
                    )
                    add_participant_to_conversation(
                        participant=participant_data,
                        conversation=conversation,
                        session=session,
                        project_id=support_project_id,
                        initiator_id=user_id,
                    )
                    session.flush()
                    log.info(f"[MCP RPC] Added MCP toolkit '{endpoint['name']}' to conversation {conversation_id}")
                except Exception as e:
                    log.warning(f"[MCP RPC] Failed to add MCP toolkit '{endpoint.get('name')}': {e}")

            session.commit()

