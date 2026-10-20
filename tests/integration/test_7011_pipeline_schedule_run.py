"""EL-7011 US-12 R1.2, R1.3, R1.4, R2.1, R2.2, R2.3, R3.1, R3.2: a pipeline schedule runs as its owner, end to end.

Same harness as the agent file (US-11): real run starter, pre-run check, run writers and schedule models over
SQLite tables; the predict RPC, the auth RPC and the credential step are faked. A pipeline schedule is the
agent fixture row with `entity_type='pipeline'`: the starter reads both through the same application tables.

Run via:
    python tests/run_tests.py -m integration -v integration/test_7011_pipeline_schedule_run.py
"""
import pytest
from integration.test_7011_agent_schedule_run import (  # noqa: F401 -- `world` is a fixture
    AGENT_ID,
    OWNER,
    PROMPT,
    VERSION_ID,
    VERSION_VARIABLES,
    world,
)

ENTRY_POINT_MESSAGE = "[Scheduled execution triggered]"


@pytest.fixture
def pipeline(world):  # noqa: F811
    """The harness with its schedule turned into a pipeline schedule in prompt mode."""
    def run(**changes):
        world.run(entity_type="pipeline", **{"run_mode": "prompt", **changes})

    def history(source="pipeline"):
        return world.conversations(source=source)

    return type("Pipeline", (), {"run": staticmethod(run), "history": staticmethod(history), "world": world})


# TC-US12-01 / R1.2, R1.3
def test_7011_r1_2_r1_3_cron_run_creates_a_pipeline_conversation_with_schedule_meta(pipeline):
    pipeline.run()

    (conversation,) = pipeline.history()
    assert (conversation.source, conversation.author_id, conversation.is_private) == ("pipeline", OWNER, True)
    assert conversation.name == "Scheduled run: Daily report"
    assert conversation.meta["scheduled_run"] is True
    assert (conversation.meta["schedule_id"], conversation.meta["schedule_run_id"]) == (1, 1)
    assert conversation.meta["execution_type"] == "scheduled"
    assert pipeline.world.run_row().conversation_id == conversation.id


# TC-US12-02 / R1.5: a lost owner creates no conversation and no predict
def test_7011_r1_5_failed_prerun_creates_no_pipeline_conversation_and_no_predict(pipeline):
    pipeline.world.auth.members.clear()

    pipeline.run()

    assert pipeline.history() == []
    assert pipeline.world.predictions == []
    assert pipeline.world.run_row().status == "failed"


# TC-US12-03, TC-US12-04 / R1.4
def test_7011_r1_4_pipeline_predict_runs_as_the_owner_not_the_system_user(pipeline):
    pipeline.run()

    (predicted,) = pipeline.world.predictions
    assert (predicted["is_system_user"], predicted["user_id"], predicted["sid"]) == (False, OWNER, None)
    assert predicted["data"]["application_id"] == AGENT_ID and predicted["data"]["version_id"] == VERSION_ID


# TC-US12-04, TC-US12-05 / R1.4: a predict error closes the run and still never uses the system user
def test_7011_r1_4_predict_error_closes_the_run_as_failed_and_the_call_was_not_a_system_call(pipeline):
    pipeline.world.predict_errors.append(RuntimeError("rpc down"))

    pipeline.run()

    assert pipeline.world.run_row().status == "failed"
    assert [call["is_system_user"] for call in pipeline.world.predictions] == [False]


# TC-US12-06, TC-US12-07, TC-US12-09 / R2.1, R2.3
def test_7011_r2_1_r2_3_prompt_mode_sends_the_filled_prompt_and_keeps_version_variables(pipeline):
    pipeline.run(prompt="Check the {{env}} build", variables=[{"name": "env", "value": "prod"}])

    (predicted,) = pipeline.world.predictions
    assert pipeline.world.first_message() == predicted["data"]["user_input"] == "Check the prod build"
    assert "variables" not in predicted["data"]
    assert pipeline.world.agent_version() == VERSION_VARIABLES


def test_7011_r2_1_prompt_without_placeholders_is_sent_as_saved(pipeline):
    pipeline.run(prompt="Check the build", variables=[])

    assert pipeline.world.first_message() == pipeline.world.predictions[0]["data"]["user_input"] == "Check the build"


# TC-US12-14 / R2.1
def test_7011_r2_1_placeholders_with_spaces_and_repeats_are_all_filled(pipeline):
    pipeline.run(prompt="Deploy {{ env }} then test {{env}} in {{region}}",
                 variables=[{"name": "env", "value": "prod"}, {"name": "region", "value": "EU"}])

    assert pipeline.world.predictions[0]["data"]["user_input"] == "Deploy prod then test prod in EU"


# TC-US12-15 / R2.1, R2.3: version variables never fill the prompt
def test_7011_r2_1_r2_3_a_value_that_looks_like_a_placeholder_is_not_filled_again(pipeline):
    pipeline.run(prompt="Check {{target}}", variables=[{"name": "target", "value": "{{region}}"}])

    assert pipeline.world.predictions[0]["data"]["user_input"] == "Check {{region}}"
    assert "variables" not in pipeline.world.predictions[0]["data"]


# TC-US12-08, TC-US12-16, TC-US12-17 / R1.4, R2.2, R2.3: the run mode, not the prompt column, decides the input
def test_7011_r2_2_entry_point_mode_sends_an_empty_input_as_the_owner_with_the_default_first_message(pipeline):
    pipeline.run(run_mode="entry_point", prompt="Stale text", variables=[{"name": "env", "value": "prod"}])

    (predicted,) = pipeline.world.predictions
    assert predicted["data"]["user_input"] == ""
    assert (predicted["is_system_user"], predicted["user_id"]) == (False, OWNER)
    assert pipeline.world.first_message() == ENTRY_POINT_MESSAGE
    assert "variables" not in predicted["data"]


# TC-US12-10 / R3.1, R3.2
def test_7011_r3_1_r3_2_pipeline_run_sends_trigger_source_scheduled_and_lists_in_pipeline_history(pipeline):
    pipeline.run()

    assert pipeline.world.predictions[0]["trigger_source"] == "scheduled"
    assert [item.meta["schedule_run_id"] for item in pipeline.history()] == [1]


# TC-US12-11 / R3.2
def test_7011_r3_2_pipeline_run_is_not_listed_in_agent_history(pipeline):
    pipeline.run()

    assert pipeline.history(source="agent") == []


# TC-US12-12 / R1.2: a missing version row fails the run before any conversation
def test_7011_r1_2_missing_version_fails_the_run_and_creates_no_conversation(pipeline):
    pipeline.run(version_id=999)

    assert pipeline.history() == []
    assert (pipeline.world.run_row().status, pipeline.world.run_row().reason) == ("failed", "schedule or version not found")
