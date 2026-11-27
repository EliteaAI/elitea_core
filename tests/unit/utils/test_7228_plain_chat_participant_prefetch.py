"""#7228 follow-up 5: under enforce a plain chat's participants are prefetched, so admission decides each with the run.

Off and audit only build the payload they built before (no prefetch). The builder runs from source with the seam of
``test_7064_predict_expansion.py``.
"""
from unit.utils.test_7064_predict_expansion import Chat, env  # noqa: F401  pylint: disable=W0611


def chat_request(env):  # pylint: disable=W0621
    participant = {"type": "application", "name": "helper", "participant_id": 3,
                   "settings": {"application_id": 9, "application_version_id": 90}}
    return Chat(project_id=7, user_input="hi", chat_history=[], thread_id="t", tools=[participant],
                llm_settings=env.llm.LLMSettingsModel(model_name="m", max_tokens=10))


def test_a_governed_plain_chat_prefetches_its_participants_as_the_person(env):  # pylint: disable=W0621
    payload = env.build(chat_request(env), 2, prefetch_participants=True)
    assert env.calls.prefetch == [(7, 2)] and env.calls.expand == []
    assert payload["application"]["subagent_version_details"] == {}


def test_off_and_audit_only_send_the_plain_chat_payload_as_before(env):  # pylint: disable=W0621
    payload = env.build(chat_request(env), 2)
    assert env.calls.prefetch == [] and "subagent_version_details" not in payload["application"]
