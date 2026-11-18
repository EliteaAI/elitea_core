"""Elitea Builder MCP tool builder_apply (#7383)."""
from ...models.pd.builder import BuilderApplyModel
from ...utils.builder.api_base import make_api

API = make_api(
    "apply", BuilderApplyModel,
    "Elitea Builder: apply a proposed changeset once; returns the summary to report and the chip.",
)
