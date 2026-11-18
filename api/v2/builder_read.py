"""Elitea Builder MCP tool builder_read (#7383)."""
from ...models.pd.builder import BuilderReadModel
from ...utils.builder.api_base import make_api

API = make_api(
    "read", BuilderReadModel,
    "Elitea Builder: read an entity's current state and revision, or the exact argument schema of an operation.",
)
