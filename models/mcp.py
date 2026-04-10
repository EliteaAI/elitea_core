from typing import Any, List, Dict, Optional

from pydantic import BaseModel, Field


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


class EliteaMcpToolkit(BaseModel):
    title: str
    type: str = "mcp"
    properties: Dict[str, EliteaToolkitSelectedTools | Dict[str, Any]] = Field(default_factory=dict)
    required: List[str] = Field(default_factory=list)
    metadata: EliteaToolkitMetadata
    name_required: bool = False
    
    def __hash__(self):
        return hash(self.title)


class McpConnectSioPayload(BaseModel):
    project_id: int
    timeout_tools_list: int
    timeout_tools_call: int
    toolkit_configs: List[McpServer]


class McpToolCallSioPayload(BaseModel):
    tool_call_id: str
    data: Dict[str, Any] | str


class McpToolCallPostBody(BaseModel):
    server: str
    tool_call_id: str
    tool_timeout_sec: int
    params: Dict[str, Any]
