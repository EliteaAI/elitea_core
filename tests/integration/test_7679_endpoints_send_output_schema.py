"""EL-7679 AC-8 (Slice 1): the agent, skill and project-context draft endpoints send their response
model's schema as ``output_schema``, and still parse the answer as before (AC-11).

Reuses the synthetic-package fixture of ``test_6416_draft_llm_failure_surfacing``.
"""
import json

from integration.test_6416_draft_llm_failure_surfacing import (  # noqa: F401  (fixture)
    APPLICATION_DRAFT, PROJECT_CONTEXT_DRAFT, SKILL_DRAFT, _call, _thinking_result, endpoints,
)


def _cases(endpoints):
    return [
        (endpoints.application, APPLICATION_DRAFT, 'GenerateApplicationDraftResponse'),
        (endpoints.skill, SKILL_DRAFT, 'GenerateSkillDraftResponse'),
        (endpoints.project_context, PROJECT_CONTEXT_DRAFT, 'GenerateProjectContextDraftResponse'),
    ]


def test_output_schema_is_sent_and_matches_the_response_model(endpoints):
    for module, draft, model_name in _cases(endpoints):
        payload, status, handler = _call(module, {'user_description': 'x'}, _thinking_result(json.dumps(draft)))

        assert status == 200, (model_name, payload)
        schema = handler.calls[0]['output_schema']
        model = getattr(module, model_name)
        assert set(schema['properties']) == set(model.model_json_schema()['properties']), model_name
        assert set(schema.get('required', [])) <= set(model.model_json_schema().get('required', []))


def test_sent_schemas_are_valid_and_accept_todays_drafts(endpoints):
    """AC-11: a draft that passes the endpoint today has every field the SDK's shape check requires."""
    from jsonschema import Draft202012Validator

    for module, draft, model_name in _cases(endpoints):
        _payload, _status, handler = _call(module, {'user_description': 'x'}, _thinking_result(json.dumps(draft)))
        schema = handler.calls[0]['output_schema']

        Draft202012Validator.check_schema(schema)
        assert list(Draft202012Validator(schema).iter_errors(draft)) == [], model_name


def test_output_schema_is_a_server_argument_not_request_data(endpoints):
    """Review #4: a socket/REST client must not be able to set output_schema in the request body."""
    for module, draft, model_name in _cases(endpoints):
        _payload, _status, handler = _call(module, {'user_description': 'x'}, _thinking_result(json.dumps(draft)))

        assert 'output_schema' not in handler.calls[0]['data'], model_name
