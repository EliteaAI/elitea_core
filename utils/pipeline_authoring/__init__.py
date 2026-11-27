"""Pipeline authoring (#6388): node registry, validator and the Builder's reference.

Twin of elitea-sdk's ``elitea_sdk/runtime/pipeline/``. pylon_main does not import the SDK at
runtime, so every other file in this package is a byte-identical copy of the SDK's, and
tests/unit/utils/test_6388_pipeline_authoring_twin.py fails when the two drift. Change the SDK
and copy the files here; never edit them in this repo alone.
"""

from .builder import VALIDATE_TOOL_DESCRIPTION, builder_reference, reference_tool_description, validate_for_builder
from .registry import get_node_registry, node_types
from .validator import ValidationContext, validate_pipeline

__all__ = [
    'VALIDATE_TOOL_DESCRIPTION',
    'ValidationContext',
    'builder_reference',
    'get_node_registry',
    'node_types',
    'reference_tool_description',
    'validate_for_builder',
    'validate_pipeline',
]
