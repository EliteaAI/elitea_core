"""Pins the failure reporting the AI draft generators depend on (#6416, #6415).

Re-broken by rejecting a model that only exists in the public project, by blocking generation
when the availability lookup itself fails, or by flattening a worker failure back into a single
generic string.
"""
import json
import pathlib
import sys
import types

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_utils_module

PROJECT = 2
PUBLIC_PROJECT = 1

TRACEBACK = (
    'Traceback (most recent call last):\n'
    '  File "/data/methods/indexer_predict_agent.py", line 520, in predict\n'
    '    response = agent.invoke(...)\n'
    'litellm.exceptions.BadRequestError: LLM Provider NOT provided for global.anthropic.claude\n'
)


@pytest.fixture(scope='module')
def draft_llm_utils():
    utils_stub = types.ModuleType('utils')
    utils_stub.get_public_project_id = lambda: PUBLIC_PROJECT

    return load_utils_module(
        TESTS_DIR.parent / 'utils',
        'draft_llm_utils',
        extra_stubs={'plugins.elitea_core.utils.utils': utils_stub},
    )


@pytest.fixture
def available_models(draft_llm_utils, monkeypatch):
    def _configure(available=None, fails=False, resolvable=(), unreadable=()):
        class _Caller:
            def timeout(self, _seconds):
                return self

            def configurations_get_available_models(self, project_id, section='llm', include_shared=True):
                if fails:
                    raise RuntimeError('rpc unavailable')
                return available

            def configurations_get_configuration_model(self, project_id, model_name):
                if project_id in unreadable:
                    raise RuntimeError(f'no schema for project {project_id}')
                return {'name': model_name} if (project_id, model_name) in resolvable else {}

        monkeypatch.setattr(
            draft_llm_utils,
            'rpc_tools',
            types.SimpleNamespace(
                RpcMixin=lambda: types.SimpleNamespace(rpc=_Caller())
            ),
        )

    return _configure


DEFAULT_MODEL_NAME = 'gpt-5-mini'

PRIVATE_AND_SHARED = {
    (PROJECT, DEFAULT_MODEL_NAME): {'supports_reasoning': False},
    (PUBLIC_PROJECT, 'claude-sonnet-5'): {'supports_reasoning': True, 'shared': True},
}


def test_model_in_own_project_resolves_to_it(draft_llm_utils, available_models):
    available_models(PRIVATE_AND_SHARED)

    assert draft_llm_utils.resolve_model(PROJECT, 'gpt-5-mini') == (None, PROJECT)


def test_public_shared_model_resolves_to_the_public_project(draft_llm_utils, available_models):
    """#6416 Issue B: unstamped, generate_predict_payload looks this up in the caller's own
    project, finds nothing, and defaults every capability - openai_compatible included."""
    available_models(PRIVATE_AND_SHARED)

    assert draft_llm_utils.resolve_model(PROJECT, 'claude-sonnet-5') == (None, PUBLIC_PROJECT)


def test_a_name_both_projects_have_resolves_to_the_caller(draft_llm_utils, available_models):
    """Private before public, the order fetch_private_configurations establishes."""
    available_models({
        (PROJECT, 'shared-name'): {},
        (PUBLIC_PROJECT, 'shared-name'): {'shared': True},
    })

    assert draft_llm_utils.resolve_model(PROJECT, 'shared-name') == (None, PROJECT)


def test_a_supplied_model_project_id_is_left_alone(draft_llm_utils, available_models):
    """Nothing to resolve - the caller already said where the model lives."""
    available_models(PRIVATE_AND_SHARED)

    assert draft_llm_utils.resolve_model(PROJECT, 'claude-sonnet-5', PUBLIC_PROJECT) == (None, None)


def test_unknown_model_names_itself_and_the_alternatives(draft_llm_utils, available_models):
    available_models(PRIVATE_AND_SHARED)

    reason, _owner = draft_llm_utils.resolve_model(PROJECT, 'nonexistent-model-xyz-999')

    assert 'nonexistent-model-xyz-999' in reason
    assert 'not available' in reason
    assert 'gpt-5-mini' in reason
    assert _owner is None


def test_wrong_model_project_id_points_at_the_owning_project(draft_llm_utils, available_models):
    available_models(PRIVATE_AND_SHARED)

    reason, _owner = draft_llm_utils.resolve_model(PROJECT, 'claude-sonnet-5', PROJECT)

    assert f'available in project {PUBLIC_PROJECT}' in reason
    assert 'model_project_id' in reason


def test_a_third_project_that_really_has_the_model_is_accepted(draft_llm_utils, available_models):
    """The available set says nothing about a project that is neither the caller's nor public."""
    available_models(PRIVATE_AND_SHARED, resolvable={(7, 'partner-model')})

    assert draft_llm_utils.resolve_model(PROJECT, 'partner-model', 7) == (None, None)


def test_model_project_id_is_verified_before_being_blamed(draft_llm_utils, available_models):
    """A name the caller's project also has must not make a valid model_project_id look wrong."""
    available_models(PRIVATE_AND_SHARED, resolvable={(7, DEFAULT_MODEL_NAME)})

    assert draft_llm_utils.resolve_model(PROJECT, DEFAULT_MODEL_NAME, 7) == (None, None)


def test_a_model_owned_by_another_project_names_that_project(draft_llm_utils, available_models):
    available_models(PRIVATE_AND_SHARED, resolvable=())

    reason, _owner = draft_llm_utils.resolve_model(PROJECT, 'claude-sonnet-5', 999)

    assert f'available in project {PUBLIC_PROJECT}' in reason


def test_an_unopenable_model_project_id_is_named_not_waved_through(draft_llm_utils, available_models):
    """A project with no schema throws; that is a bad argument, and it is fatal further down."""
    available_models(PRIVATE_AND_SHARED, unreadable={999})

    reason, _owner = draft_llm_utils.resolve_model(PROJECT, 'claude-sonnet-5', 999)

    assert 'not configured in project 999' in reason


def test_an_unreadable_public_project_does_not_cost_a_working_model(draft_llm_utils, available_models):
    available_models(PRIVATE_AND_SHARED, unreadable={PUBLIC_PROJECT})

    assert draft_llm_utils.resolve_model(PROJECT, 'externally-managed') == (None, None)


def test_an_unresolvable_public_project_id_does_not_cost_a_working_model(draft_llm_utils,
                                                                         available_models,
                                                                         monkeypatch):
    """No public project id is an unanswered question, not a definitive absence."""
    available_models(PRIVATE_AND_SHARED, resolvable=())
    monkeypatch.setattr(draft_llm_utils, '_public_project_id', lambda: None)

    assert draft_llm_utils.resolve_model(PROJECT, 'externally-managed') == (None, None)


def test_unshared_public_model_is_allowed_but_not_stamped(draft_llm_utils, available_models):
    """_map_model_name falls back to the public project, so this must not be rejected - but the
    per-project lookup ignores `shared`, so a hit does not prove the caller may name that project."""
    available_models(PRIVATE_AND_SHARED, resolvable={(PUBLIC_PROJECT, 'unshared-public-model')})

    assert draft_llm_utils.resolve_model(PROJECT, 'unshared-public-model') == (None, None)


def test_lookup_failure_does_not_block_generation(draft_llm_utils, available_models):
    available_models(fails=True)

    assert draft_llm_utils.resolve_model(PROJECT, 'anything') == (None, None)


def test_model_list_is_capped(draft_llm_utils, available_models):
    available_models({(PROJECT, f'model-{i:02d}'): {} for i in range(25)})

    reason, _owner = draft_llm_utils.resolve_model(PROJECT, 'missing')

    assert '+5 more' in reason


def test_project_without_models_says_so(draft_llm_utils, available_models):
    available_models({})

    reason, _owner = draft_llm_utils.resolve_model(PROJECT, 'gpt-5-mini')

    assert 'No LLM models are configured' in reason


def test_successful_result_reports_no_failure(draft_llm_utils):
    result = {'result': {'thinking_steps': [{'text': '{"name": "x"}'}]}}

    assert draft_llm_utils.describe_predict_failure(result) is None


def test_join_timeout_is_a_504_not_a_failed_generation(draft_llm_utils):
    """The model took too long; that is not the same condition as the model failing."""
    body, status = draft_llm_utils.timeout_response({'task_id': 'abc'}, 60)

    assert status == 504
    # a sentence, like every other error these endpoints return - the draft modals render `error`
    assert 'timed out after 60s' in body['error']
    # an identical retry would take just as long, and cancellation is best-effort
    assert 'try again' not in body['error'].lower()


def test_a_timeout_envelope_reaching_the_failure_path_is_not_described(draft_llm_utils):
    assert draft_llm_utils.describe_predict_failure({'task_id': 'abc'}) is None


def test_worker_message_leads_and_the_exception_line_qualifies_it(draft_llm_utils):
    """human_readable is often the catch-all, so the exception line must survive alongside it."""
    result = {'result': {
        'chat_history': [],
        'error': TRACEBACK,
        'human_readable': 'An unexpected error occurred while processing your request',
    }}

    reason = draft_llm_utils.describe_predict_failure(result)

    assert 'An unexpected error occurred' in reason
    assert 'LLM Provider NOT provided' in reason
    assert 'Traceback' not in reason


def test_exception_line_is_not_repeated_when_already_quoted(draft_llm_utils):
    result = {'result': {
        'error': 'ValueError: bad model',
        'human_readable': 'Configuration problem: ValueError: bad model',
    }}

    reason = draft_llm_utils.describe_predict_failure(result)

    assert reason.count('ValueError: bad model') == 1


def test_traceback_falls_back_to_its_last_line(draft_llm_utils):
    reason = draft_llm_utils.describe_predict_failure({'result': {'error': TRACEBACK}})

    assert 'LLM Provider NOT provided' in reason
    assert 'indexer_predict_agent.py' not in reason


def test_long_error_line_is_truncated(draft_llm_utils):
    reason = draft_llm_utils.describe_predict_failure({'result': {'error': 'x' * 900}})

    assert reason.endswith('...')
    assert len(reason) < 400


def test_output_cut_off_by_max_tokens_says_so(draft_llm_utils):
    """A budget too small for any output leaves empty steps that all stopped on 'length'."""
    result = {'result': {'error': None, 'thinking_steps': [
        {'text': '', 'generation_info': {'finish_reason': 'length'}},
        {'text': '', 'generation_info': {'finish_reason': 'length'}},
    ]}}

    reason = draft_llm_utils.describe_predict_failure(result)

    assert 'cut off' in reason
    assert 'max_tokens' in reason


def test_any_step_reporting_the_token_limit_counts(draft_llm_utils):
    """Observed shape: the worker continues after a cut-off, so the run still ends 'stop'."""
    result = {'result': {'thinking_steps': [
        {'text': '', 'midturn_injection_id': 'inj-1'},
        {'text': '', 'generation_info': {'finish_reason': 'length'}},
        {'text': '', 'generation_info': {'finish_reason': 'stop'}},
    ]}}

    assert 'cut off' in draft_llm_utils.describe_predict_failure(result)


def test_hit_token_limit_is_false_when_nothing_was_cut_off(draft_llm_utils):
    assert draft_llm_utils.hit_token_limit(
        {'result': {'thinking_steps': [{'generation_info': {'finish_reason': 'stop'}}]}}
    ) is False
    assert draft_llm_utils.hit_token_limit({'task_id': 'abc'}) is False
    assert draft_llm_utils.hit_token_limit(None) is False


def test_normally_finished_empty_steps_are_not_a_budget_problem(draft_llm_utils):
    result = {'result': {'thinking_steps': [{'text': '', 'generation_info': {'finish_reason': 'stop'}}]}}

    assert draft_llm_utils.describe_predict_failure(result) is None


@pytest.mark.parametrize('result, expected', [
    ({'result': {'thinking_steps': [{'text': 'first'}, {'text': 'last'}]}}, 'last'),
    ({'result': {'thinking_steps': [{'text': 'kept'}, {'text': ''}]}}, 'kept'),
    ({'result': {'thinking_steps': []}}, ''),
    ({'result': None}, ''),
    ({'task_id': 'abc'}, ''),
    (None, ''),
])
def test_extract_draft_text(draft_llm_utils, result, expected):
    assert draft_llm_utils.extract_draft_text(result) == expected


def test_specific_worker_message_is_not_padded_with_the_exception(draft_llm_utils):
    """The worker's specific branches interpolate the exception; repeating it adds only noise."""
    result = {'result': {
        'error': 'Traceback…\nAuthenticationError: model access denied for team',
        'human_readable': (
            'Authentication error with the AI provider: model access denied for team. '
            'Please check your model configuration and API credentials.'
        ),
    }}

    reason = draft_llm_utils.describe_predict_failure(result)

    assert 'AuthenticationError' not in reason
    assert reason.endswith('API credentials.')


def test_top_level_envelope_prefers_its_message(draft_llm_utils):
    result = {'error': 'some_platform_state', 'message': 'The platform said no.'}

    reason = draft_llm_utils.describe_predict_failure(result)

    assert reason == 'LLM generation failed: The platform said no.'


def test_maintenance_is_named_by_the_generic_path(draft_llm_utils):
    """Not special-cased: the platform's envelope carries a sentence, and that is what surfaces."""
    envelope = {'error': 'maintenance_in_progress', 'message': 'The platform is in maintenance mode.'}

    assert draft_llm_utils.timeout_response(envelope, 60) is None
    assert 'maintenance mode' in draft_llm_utils.describe_predict_failure(envelope)


def test_timeout_response_ignores_everything_else(draft_llm_utils):
    assert draft_llm_utils.timeout_response({'result': {'thinking_steps': []}}, 60) is None
    assert draft_llm_utils.timeout_response({'error': 'temporarily_unavailable'}, 60) is None
    assert draft_llm_utils.timeout_response(None, 60) is None


@pytest.mark.parametrize('settings, field, chosen', [
    (None, 'max_tokens', False),
    (types.SimpleNamespace(model_fields_set=set(), max_tokens=2048), 'max_tokens', False),
    (types.SimpleNamespace(model_fields_set={'max_tokens'}, max_tokens=None), 'max_tokens', False),
    (types.SimpleNamespace(model_fields_set={'max_tokens'}, max_tokens=512), 'max_tokens', True),
    (types.SimpleNamespace(model_fields_set={'temperature'}, temperature=0), 'temperature', True),
])
def test_caller_chose(draft_llm_utils, settings, field, chosen):
    assert draft_llm_utils.caller_chose(settings, field) is chosen


def test_non_dict_result_is_reported(draft_llm_utils):
    assert draft_llm_utils.describe_predict_failure(None)


def _decode_error(candidate):
    try:
        json.loads(candidate)
    except json.JSONDecodeError as exc:
        return exc
    raise AssertionError(f'{candidate!r} parsed cleanly')


def _stopped(*reasons):
    return {'result': {'thinking_steps': [
        {'text': '', 'generation_info': {'finish_reason': reason}} for reason in reasons
    ]}}


# a real cut-off loses a *long* value, so the opening quote `pos` reports sits nowhere near the
# cut - the shape that makes a naive "chars from the end" figure read as a mid-draft failure
CUT_OFF = (
    '{"name": "release-notes", "description": "Summarizes release notes.", '
    '"instructions": "' + 'Summarize each change and its impact. ' * 20
)
STRAY_CONTROL = '{"name": "release-notes", "instructions": "Line one\nLine two", "description": "ok"}'


def test_a_cut_off_draft_is_named_by_its_message_and_unclosed_tail(draft_llm_utils):
    """`pos` points at the opening quote, not the cut, so the tail is what shows the draft simply
    stopping - it ends mid-prose with nothing closing it."""
    error = _decode_error(CUT_OFF)
    reason = draft_llm_utils.describe_parse_failure(CUT_OFF, CUT_OFF, error, _stopped('stop'))

    assert 'Unterminated string' in reason
    assert f'pos {error.pos}, never closed' in reason
    assert f'{len(CUT_OFF)} extracted chars' in reason
    assert reason.endswith("its impact. '")


def test_an_unterminated_string_is_not_reported_as_a_distance_from_the_end(draft_llm_utils):
    """That offset would claim a mid-draft failure for a draft that stopped at the very end."""
    error = _decode_error(CUT_OFF)
    assert len(CUT_OFF) - error.pos > 2 * draft_llm_utils.PARSE_FAILURE_WINDOW

    reason = draft_llm_utils.describe_parse_failure(CUT_OFF, CUT_OFF, error, _stopped('stop'))

    assert 'from the end' not in reason


def test_a_stray_control_character_is_described_as_failing_mid_draft(draft_llm_utils):
    """The decisive difference from a cut-off: the rest of the draft is still behind the failure."""
    error = _decode_error(STRAY_CONTROL)
    reason = draft_llm_utils.describe_parse_failure(
        STRAY_CONTROL, STRAY_CONTROL, error, _stopped('stop'),
    )

    assert 'Invalid control character' in reason
    assert f'pos {error.pos}, {len(STRAY_CONTROL) - error.pos} chars from the end' in reason
    # repr'd, or the log formatter renders the newline as ordinary whitespace and hides the cause
    assert '\\n' in reason


def test_the_window_is_bounded_and_centred_on_the_failure(draft_llm_utils):
    """Bounded before repr, so nothing is claimed about the rendered length - an escape expands."""
    filler = 'x' * (3 * draft_llm_utils.PARSE_FAILURE_WINDOW)
    candidate = f'{{"a": "FARLEFT{filler}NEARLEFT\nNEARRIGHT{filler}FARRIGHT"}}'

    reason = draft_llm_utils.describe_parse_failure(
        candidate, candidate, _decode_error(candidate), _stopped('stop'),
    )
    window = reason.split('window=')[1].split('; tail=')[0]

    assert 'NEARLEFT' in window and 'NEARRIGHT' in window
    assert 'FARLEFT' not in window and 'FARRIGHT' not in window


def test_the_diagnostic_carries_what_the_model_thought_it_did(draft_llm_utils):
    reason = draft_llm_utils.describe_parse_failure(
        CUT_OFF, CUT_OFF, _decode_error(CUT_OFF), _stopped('length', 'stop'),
    )

    assert "finish_reasons=['length', 'stop']" in reason


def test_extraction_that_trimmed_the_output_reports_both_lengths(draft_llm_utils):
    raw = 'Here is your draft:\n```json\n' + CUT_OFF + '\n```'

    reason = draft_llm_utils.describe_parse_failure(
        raw, CUT_OFF, _decode_error(CUT_OFF), _stopped('stop'),
    )

    assert f'{len(CUT_OFF)} extracted chars' in reason
    assert f'{len(raw)} raw' in reason


@pytest.mark.parametrize('result, expected', [
    ({'result': {'thinking_steps': [{'generation_info': {'finish_reason': 'stop'}}]}}, ['stop']),
    ({'result': {'thinking_steps': [{'text': 'x'}]}}, [None]),
    ({'result': {'thinking_steps': []}}, []),
    ({'task_id': 'abc'}, []),
    (None, []),
    # a diagnostic that raises inside the failure handler costs the caller its described 422
    ({'result': {'thinking_steps': [{'generation_info': 'stop'}]}}, [None]),
    ({'result': {'thinking_steps': [{'generation_info': []}]}}, [None]),
])
def test_finish_reasons(draft_llm_utils, result, expected):
    assert draft_llm_utils.finish_reasons(result) == expected


def test_a_malformed_generation_info_does_not_break_the_token_limit_check(draft_llm_utils):
    assert draft_llm_utils.hit_token_limit(
        {'result': {'thinking_steps': [{'generation_info': 'length'}]}}
    ) is False


@pytest.mark.parametrize('raw_text, truncated', [
    ('{"a": 1}', False),
    ('{"a": {"b": 1}', True),
    ('{"a": [1, 2', True),
    ('prose {"a": 1} more prose', False),
])
def test_is_truncated_json(draft_llm_utils, raw_text, truncated):
    assert draft_llm_utils.is_truncated_json(raw_text) is truncated
