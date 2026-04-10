# from typing import List, Optional

# from pydantic.v1 import BaseModel


# class DatasourceToolSettings(BaseModel):
#     datasource_id: int
#     selected_tools: List[str] = []


# class PromptToolSettings(BaseModel):
#     prompt_id: int
#     prompt_version_id: int
#     variables: List[dict]


# class OpenApiSelectedToolSettings(BaseModel):
#     name: Optional[str]
#     description: Optional[str]
#     method: Optional[str]
#     path: Optional[str]


# class OpenApiToolSettings(BaseModel):
#     schema_settings: Optional[str]
#     selected_tools: Optional[List[OpenApiSelectedToolSettings]]
#     authentication: Optional[dict]


# _TOOL_SETTINGS_TYPE_MAPPER = {
#     'datasource': DatasourceToolSettings,
#     'prompt': PromptToolSettings,
#     'openapi': OpenApiToolSettings,
# }
