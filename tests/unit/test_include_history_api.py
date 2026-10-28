"""The schedule handlers save `include_history` of the delivery target and refuse a value outside 0 to 20.

Reuses the handler harness of the delivery save tests: the route module, the item handlers, the service and the
body models are real, rows live in SQLite. The column read here is the one the run starter reads at the start
of a delivered run.

Run via:
    python tests/run_tests.py unit/test_include_history_api.py -v
"""
import importlib.util
import pathlib

import pytest

# The unit folder is no package: load the sibling module by path and bind its fixture as an attribute
_path = pathlib.Path(__file__).with_name('test_schedule_delivery_api.py')
_spec = importlib.util.spec_from_file_location('delivery_api_fixtures', _path)
base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(base)
harness = base.harness


def chat_target(**changes):
    return base.to_chat() | changes


def create(harness, **changes):
    return harness.call('post', base.agent_body(delivery=chat_target(**changes)))


def stored_history(harness, schedule_id):
    return harness.row(schedule_id).delivery_include_history


def test_create_stores_the_count_with_the_chat_target(harness):
    body, status = create(harness, include_history=7)
    assert status == 201
    assert stored_history(harness, body['id']) == 7


def test_create_without_the_count_stores_zero(harness):
    body, status = create(harness)
    assert status == 201
    assert stored_history(harness, body['id']) == 0


def test_create_with_run_history_delivery_stores_zero(harness):
    body, status = harness.call('post', base.agent_body())
    assert status == 201
    assert stored_history(harness, body['id']) == 0


@pytest.mark.parametrize('value', [0, 20])
def test_create_accepts_the_edges_of_the_range(harness, value):
    body, status = create(harness, include_history=value)
    assert status == 201
    assert stored_history(harness, body['id']) == value


@pytest.mark.parametrize('value', [-1, 21])
def test_create_outside_the_range_is_400_and_stores_nothing(harness, value):
    base.assert_400(create(harness, include_history=value))
    assert harness.stored_count() == 0


def test_create_with_a_text_count_is_400_and_stores_nothing(harness):
    base.assert_400(create(harness, include_history='many'))
    assert harness.stored_count() == 0


def test_edit_replaces_the_stored_count(harness):
    saved, _ = create(harness, include_history=7)
    _, status = harness.call('put', {'delivery': chat_target(include_history=20)}, saved['id'])
    assert status == 200
    assert stored_history(harness, saved['id']) == 20


@pytest.mark.parametrize('value', [-1, 21])
def test_edit_outside_the_range_is_400_and_keeps_the_stored_count(harness, value):
    saved, _ = create(harness, include_history=7)
    base.assert_400(harness.call('put', {'delivery': chat_target(include_history=value)}, saved['id']))
    assert stored_history(harness, saved['id']) == 7


def test_edit_without_delivery_keeps_the_stored_count(harness):
    saved, _ = create(harness, include_history=7)
    _, status = harness.call('put', {'name': 'Renamed'}, saved['id'])
    assert status == 200
    assert stored_history(harness, saved['id']) == 7


def test_edit_to_run_history_resets_the_stored_count(harness):
    saved, _ = create(harness, include_history=7)
    _, status = harness.call('put', {'delivery': base.NO_DELIVERY}, saved['id'])
    assert status == 200
    assert stored_history(harness, saved['id']) == 0
