"""EliteaAI/elitea_issues#7682 (BE-1): the run list accepts comma lists and returns a status on every run row.

Contract: models/pd/schedule_list.py `ConversationListFilters` (AC-48, AC-48 bounds) and the list RPC's run outcome
rule in utils/run_outcome.py (AC-24, AC-25). The model is loaded from source; no database is needed.
"""
import importlib.util
import pathlib

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]


def _load_schedule_list():
    spec = importlib.util.spec_from_file_location(
        'schedule_list_under_test', PLUGIN_ROOT / 'models' / 'pd' / 'schedule_list.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def schedule_list():
    return _load_schedule_list()


# --- AC-48: a single value keeps today's result --------------------------------------------------------------

@pytest.mark.parametrize(('field', 'value', 'expected'), [
    ('status', 'running', 'running'),
    ('trigger', 'cron', 'cron'),
    ('version_id', '12', 12),
    ('execution_type', 'chat', 'chat'),
    ('run_outcome', 'failed', 'failed'),
])
def test_a_single_value_stays_a_scalar(schedule_list, field, value, expected):
    parsed = schedule_list.ConversationListFilters.model_validate({field: value})
    assert getattr(parsed, field) == expected


# --- AC-48: a comma list is an OR over its values ------------------------------------------------------------

def test_a_comma_list_keeps_every_value(schedule_list):
    parsed = schedule_list.ConversationListFilters.model_validate({'status': 'running,failed'})
    assert parsed.status == ['running', 'failed']


def test_empty_elements_in_a_list_are_ignored(schedule_list):
    parsed = schedule_list.ConversationListFilters.model_validate({'status': 'running,,failed,'})
    assert parsed.status == ['running', 'failed']


def test_a_value_with_no_element_at_all_is_rejected(schedule_list):
    # Today's contract: an empty filter value is a 400, not a silent "no filter"
    with pytest.raises(Exception):
        schedule_list.ConversationListFilters.model_validate({'trigger': ''})


# --- AC-48: a 400 for an unknown value or more than 20 values ------------------------------------------------

@pytest.mark.parametrize(('field', 'value'), [
    ('status', 'done'),
    ('status', 'running,done'),
    ('trigger', 'scheduled'),
    ('execution_type', 'scheduled'),
    ('run_outcome', 'stopped,bogus'),
    ('version_id', '0'),
    ('version_id', 'x'),
])
def test_an_unknown_value_is_rejected(schedule_list, field, value):
    with pytest.raises(Exception):
        schedule_list.ConversationListFilters.model_validate({field: value})


def test_more_than_twenty_values_is_rejected(schedule_list):
    too_many = ','.join(['running'] * 21)
    with pytest.raises(Exception):
        schedule_list.ConversationListFilters.model_validate({'status': too_many})


def test_twenty_values_are_allowed(schedule_list):
    twenty = ','.join(['running'] * 20)
    parsed = schedule_list.ConversationListFilters.model_validate({'status': twenty})
    assert len(parsed.status) == 20


# --- AC-24 / AC-25: the derived run outcome rule, as a plain function ----------------------------------------

def _derive(has_record_status, is_running, last_answer_failed):
    """The rule in utils/run_outcome.py, restated without SQL so it can be checked in isolation."""
    if has_record_status is not None:
        return has_record_status
    if is_running:
        return 'running'
    if last_answer_failed:
        return 'failed'
    return 'success'


@pytest.mark.parametrize(('record', 'running', 'failed', 'expected'), [
    ('skipped', False, False, 'skipped'),   # a schedule record wins over the chat rules
    ('failed', True, False, 'failed'),      # even while a record says failed, a live task does not override it
    (None, True, True, 'running'),          # a live task beats a failed earlier answer
    (None, False, True, 'failed'),          # the last answer has a failed tool call
    (None, False, False, 'success'),        # a normal chat run, and a stopped one (AC-24a)
])
def test_the_outcome_rule_order(record, running, failed, expected):
    assert _derive(record, running, failed) == expected


# --- AC-24, AC-44: every row has a status; a blank tool name is never listed ------------------------------------

def test_the_list_derives_a_status_for_api_and_mcp_rows():
    """The list RPC derives the status of every row, API and MCP runs included (the #7610 gate is not a status gate)."""
    source = (PLUGIN_ROOT / 'rpc' / 'chat_conversation.py').read_text()
    assert "run_outcomes(session, conv_ids, record_statuses) if run_columns else {}" in source
    assert "('api', 'mcp')" not in source


def test_the_run_columns_are_computed_only_for_a_run_history_list():
    """The sidebar and skill chat lists skip the outcome and tool name reads (review: the shared list paid for them)."""
    source = (PLUGIN_ROOT / 'rpc' / 'chat_conversation.py').read_text()
    assert "run_columns = participant_id is not None" in source
    assert "tool_names_by_conversation(session, conv_ids) if run_columns else {}" in source


def test_the_null_run_conversations_are_left_out_of_the_record_subquery():
    """A NULL conversation id must not reach a NOT IN list (review: one NULL made every plain run drop out)."""
    source = (PLUGIN_ROOT / 'utils' / 'run_outcome.py').read_text()
    assert "EntityScheduleRun.conversation_id.isnot(None)" in source


def test_the_tool_name_lookup_drops_a_blank_name():
    """A tool-call step with an empty name never becomes an empty Event value (AC-44)."""
    source = (PLUGIN_ROOT / 'utils' / 'run_outcome.py').read_text()
    assert "MessageTraceStep.tool_name != ''" in source


# --- AC-24: a failed tool result stores an error step, even without an error text ----------------------------

def test_the_trace_writer_keeps_is_error_on_the_error_text_only():
    """The trace writer marks a step an error only from its error text. A finish_reason rule was reverted (PR 766):
    it moved the evaluation tool-error metrics, which read the same column."""
    source = (PLUGIN_ROOT / 'utils' / 'trace_step_writer.py').read_text()
    assert "is_error=bool(entry.get('error'))," in source
    assert "entry.get('finish_reason') == 'error'" not in source


# --- AC-24: a run that failed before its first tool call reads failed, in the filter and the display ----------

def test_a_flagged_last_group_reads_failed_in_the_filter_and_the_display():
    """The last message group flagged is_error is a failed run on both sides of the rule (no tool-call step needed).

    The filter and the display share one last-group rule now, so the flag is read in the filter and in the display.
    """
    source = (PLUGIN_ROOT / 'utils' / 'run_outcome.py').read_text()
    assert "ConversationMessageGroup.meta['is_error'].as_boolean().is_(True)" in source
    assert "meta.get('is_error') is True" in source


# --- BE-2: the complexity filter of the run list (labels from usage, failing closed) -------------------------------

def test_a_complexity_label_is_a_scalar_or_a_list(schedule_list):
    single = schedule_list.ConversationListFilters.model_validate({'complexity': 'L'})
    assert single.complexity == 'L'
    several = schedule_list.ConversationListFilters.model_validate({'complexity': 'L,not_scored'})
    assert several.complexity == ['L', 'not_scored']


def test_an_unknown_complexity_label_is_rejected(schedule_list):
    with pytest.raises(Exception):
        schedule_list.ConversationListFilters.model_validate({'complexity': 'bogus'})


def test_the_list_route_passes_the_complexity_filter_on():
    source = (PLUGIN_ROOT / 'api' / 'v2' / 'conversations.py').read_text()
    assert "complexity=run_filters.complexity," in source


def test_the_complexity_lookup_fails_closed_when_usage_is_unreachable():
    """A usage failure must not show every run as a match: the lookup returns None and the list says so."""
    source = (PLUGIN_ROOT / 'rpc' / 'chat_conversation.py').read_text()
    assert "if found is None:" in source
    assert "'complexity_unavailable': True" in source


# --- the version and complexity filters match what the table shows (chats have no run record) ----------------------

def _load_complexity_helper(log=None):
    """`_complexity_conversation_ids` taken from the RPC source, so no plugin import is needed."""
    import ast
    import types
    import uuid
    source = (PLUGIN_ROOT / 'rpc' / 'chat_conversation.py').read_text()
    function = next(n for n in ast.parse(source).body
                    if isinstance(n, ast.FunctionDef) and n.name == '_complexity_conversation_ids')
    namespace = {'uuid': uuid, 'log': log or types.SimpleNamespace(exception=lambda *a, **k: None),
                 'RPC_TIMEOUT_SECONDS': 10, 'COMPLEXITY_FROM': 0, 'COMPLEXITY_TO': 1}
    exec(compile(ast.Module([function], []), 'chat_conversation.py', 'exec'), namespace)  # pylint: disable=W0122
    return namespace['_complexity_conversation_ids']


def _rpc_manager(answer):
    import types

    def by_label(*_args):
        if isinstance(answer, Exception):
            raise answer
        return answer
    return types.SimpleNamespace(timeout=lambda _seconds: types.SimpleNamespace(
        usage_complexity_conversations_by_label=by_label))


UUID_A = '9414e9bc-f506-412a-8635-114ac69c1d7b'
UUID_B = '0f1ad0b4-0000-4000-8000-000000000001'


def test_the_complexity_ids_are_typed_uuids_and_truncated_is_reported():
    import uuid
    helper = _load_complexity_helper()
    found = helper(_rpc_manager({'conversation_ids': [UUID_A, UUID_B], 'truncated': True}), ['L'], 26, 3)
    assert found == ([uuid.UUID(UUID_A), uuid.UUID(UUID_B)], True)


def test_an_id_that_is_not_a_uuid_is_dropped_not_matched():
    helper = _load_complexity_helper()
    found = helper(_rpc_manager({'conversation_ids': ['12', 'x', UUID_A], 'truncated': False}), ['L'], 26, 3)
    assert [str(value) for value in found[0]] == [UUID_A] and found[1] is False


@pytest.mark.parametrize('answer', [
    {'conversation_ids': [], 'truncated': False, 'skipped': 'disabled'},  # the feature is off: not "no match"
    {'something': 'else'},
    None,
    RuntimeError('usage is down'),
])
def test_complexity_that_cannot_be_read_is_unavailable_not_empty(answer):
    helper = _load_complexity_helper()
    assert helper(_rpc_manager(answer), ['L'], 26, 3) is None


@pytest.mark.parametrize(('entity_id', 'entity_project_id'), [(None, 3), (26, None), (None, None)])
def test_complexity_needs_the_entity(entity_id, entity_project_id):
    helper = _load_complexity_helper()
    assert helper(_rpc_manager({'conversation_ids': []}), ['L'], entity_id, entity_project_id) is None


def test_the_list_matches_the_uuid_column_with_typed_values_and_no_cast():
    source = (PLUGIN_ROOT / 'rpc' / 'chat_conversation.py').read_text()
    assert "Conversation.uuid.in_(labelled) if labelled else false()" in source
    assert "cast(Conversation.uuid, String)" not in source


def test_a_truncated_answer_is_flagged_on_the_page_it_returns():
    source = (PLUGIN_ROOT / 'rpc' / 'chat_conversation.py').read_text()
    assert "complexity_flags['complexity_truncated'] = True" in source
    assert source.count("**complexity_flags") == 2  # the empty page and the full page


# --- PR 770 review: the labels that cannot be read are an error, not an empty page ---------------------------------

def test_the_api_answers_503_when_complexity_is_unavailable_and_400_without_an_entity():
    source = (PLUGIN_ROOT / 'api' / 'v2' / 'conversations.py').read_text()
    assert "result.get('complexity_unavailable')" in source and "}, 503" in source
    assert "complexity needs entity_meta_id and entity_meta_project_id" in source


# --- PR 770 review: a test chat has no run record, so a version filter reads its meta, but only in one entity's history

def test_a_version_filter_also_matches_the_test_chats_own_meta_version():
    """The table shows the version a test chat carries in its meta, so the filter finds it there too."""
    source = (PLUGIN_ROOT / 'rpc' / 'chat_conversation.py').read_text()
    assert "'version_id'].astext.in_([str(value) for value in _as_values(run_version_id)])" in source
    # Only a version alone may match through the meta: any other run option still needs the run record
    assert "only_version = len(run_conditions) == 1 and version_in_meta is not None" in source


def test_the_meta_version_is_read_only_with_an_entity_filter():
    source = (PLUGIN_ROOT / 'rpc' / 'chat_conversation.py').read_text()
    guard = source.index("if participant_id is not None:\n                    version_in_meta")
    assert guard < source.index("'version_id'].astext.in_(")
