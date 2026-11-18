"""Elitea Builder MCP tool builder_propose (#7383)."""
from ...models.pd.builder import BuilderProposeModel
from ...utils.builder.api_base import make_api

API = make_api(
    "propose", BuilderProposeModel,
    "Elitea Builder: propose a change. Nothing is written. Returns ready, needs_input, needs_confirmation, invalid or conflict, with a diff and a changeset_id.",
)
