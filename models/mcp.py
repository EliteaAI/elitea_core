import re
from typing import Annotated, Any, List, Dict, Literal, Optional

from pydantic import BaseModel, BeforeValidator, Field

CLIENT_TEXT_LIMIT = 500
CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f-\x9f]")
FailureKind = Literal["command_not_found", "exited", "timeout", "unreachable", "auth", "invalid_config"]


def clean_client_text(value: Any) -> Any:
    """Text a local client reports is shown to users and logged: cut it short and drop control characters."""
    if not isinstance(value, str):
        return value
    return CONTROL_CHARACTERS.sub(" ", value)[:CLIENT_TEXT_LIMIT]


ClientText = Annotated[str, BeforeValidator(clean_client_text)]


class McpInputSchema(BaseModel):
    type: str
    properties: Dict[str, Dict[str, Any]]
    required: List[str]


class McpTool(BaseModel):
    name: str
    description: str
    inputSchema: McpInputSchema


class McpServer(BaseModel):
    name: str
    tools: List[McpTool]
    project_id: Optional[str] = None
    sio_sid: Optional[str] = None
    timeout_tools_list: Optional[int] = 90
    timeout_tools_call: Optional[int] = 90
    group: Optional[str] = 'Other'


class EliteaToolkitArgsSchema(BaseModel):
    name: str
    description: str
    inputSchema: Dict[str, Any]
    title: str


class EliteaToolkitItems(BaseModel):
    enum: List[str]
    type: str = "string"


class EliteaToolkitSelectedTools(BaseModel):
    title: str
    default: List[str] = Field(default_factory=list)
    args_schemas: Dict[str, EliteaToolkitArgsSchema]
    type: str = "array"
    items: EliteaToolkitItems


class EliteaToolkitMetadata(BaseModel):
    icon_url: Optional[str] = None
    label: str
    has_function_validators: bool = False
    categories: tuple[str] = ('MCP',)
    group: Optional[str] = 'Other'


class EliteaMcpToolkit(BaseModel):
    title: str
    type: str = "mcp"
    properties: Dict[str, EliteaToolkitSelectedTools | Dict[str, Any]] = Field(default_factory=dict)
    required: List[str] = Field(default_factory=list)
    metadata: EliteaToolkitMetadata
    name_required: bool = False
    
    def __hash__(self):
        return hash(self.title)


class McpFailedServer(BaseModel):
    name: ClientText
    transport: ClientText = "stdio"
    kind: FailureKind
    message: ClientText = ""


class McpConnectSioPayload(BaseModel):
    project_id: int
    timeout_tools_list: int
    timeout_tools_call: int
    toolkit_configs: List[McpServer]
    protocol: Optional[int] = None
    client_id: Optional[str] = Field(None, min_length=1, max_length=128)
    epoch: Optional[str] = Field(None, min_length=1, max_length=64)
    generation: Optional[int] = Field(None, ge=0)
    failed_servers: List[McpFailedServer] = Field(default_factory=list)

    @property
    def is_snapshot(self) -> bool:
        """A client that numbers its messages sends its whole state each time; older clients only add servers."""
        return self.generation is not None and self.client_id is not None and self.epoch is not None


class McpToolCallSioPayload(BaseModel):
    tool_call_id: str
    data: Dict[str, Any] | str


class McpToolCallPostBody(BaseModel):
    server: str
    tool_call_id: str
    tool_timeout_sec: int
    params: Dict[str, Any]
