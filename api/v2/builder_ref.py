"""Elitea Builder MCP tool builder_reference (#7383)."""
from ...models.pd.builder import BuilderReferenceModel
from ...utils.builder.api_base import make_api

API = make_api(
    "reference", BuilderReferenceModel,
    "Elitea Builder: load a playbook (playbook:<type>) or pipeline authoring pages "
    "(pipeline:overview,node:<type>,pattern:<name>).",
)
