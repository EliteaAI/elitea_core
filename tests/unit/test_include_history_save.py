"""The schedule service stores how many recent chat messages a delivered run sends.

Reuses the stand-in chat tables of the delivery save tests.

Run via:
    python tests/run_tests.py unit/test_include_history_save.py -v
"""
import importlib.util
import pathlib

import pytest
from pydantic import ValidationError

# The unit folder is no package: load the sibling module by path and bind its fixture as an attribute
_path = pathlib.Path(__file__).with_name('test_schedule_delivery_save.py')
_spec = importlib.util.spec_from_file_location('delivery_save_fixtures', _path)
base = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(base)
svc = base.svc


def chat_target(**changes):
    return base.to_chat() | changes


def test_save_stores_include_history_with_the_target(svc):
    row = base.create(svc, delivery=chat_target(include_history=7))
    assert row.delivery_include_history == 7


def test_save_without_include_history_stores_zero(svc):
    row = base.create(svc, delivery=chat_target())
    assert row.delivery_include_history == 0


def test_edit_replaces_stored_include_history(svc):
    row = base.create(svc, delivery=chat_target(include_history=7))
    update = svc.pd.ScheduleUpdate(delivery=chat_target(include_history=20))
    svc.service.update_schedule(svc.session, base.PROJECT_ID, row, update, svc.scope(base.OWNER_ID))
    assert row.delivery_include_history == 20


def test_edit_without_delivery_keeps_include_history(svc):
    row = base.create(svc, delivery=chat_target(include_history=7))
    svc.service.update_schedule(
        svc.session, base.PROJECT_ID, row, svc.pd.ScheduleUpdate(name='Renamed'), svc.scope(base.OWNER_ID))
    assert row.delivery_include_history == 7


def test_edit_delivery_without_include_history_keeps_the_stored_value(svc):
    row = base.create(svc, delivery=chat_target(include_history=7))
    update = svc.pd.ScheduleUpdate(delivery=chat_target())
    svc.service.update_schedule(svc.session, base.PROJECT_ID, row, update, svc.scope(base.OWNER_ID))
    assert row.delivery_include_history == 7


def test_edit_delivery_with_zero_clears_the_stored_value(svc):
    row = base.create(svc, delivery=chat_target(include_history=7))
    update = svc.pd.ScheduleUpdate(delivery=chat_target(include_history=0))
    svc.service.update_schedule(svc.session, base.PROJECT_ID, row, update, svc.scope(base.OWNER_ID))
    assert row.delivery_include_history == 0


def test_edit_to_run_history_resets_include_history(svc):
    row = base.create(svc, delivery=chat_target(include_history=7))
    update = svc.pd.ScheduleUpdate(delivery={'mode': 'run_history'})
    svc.service.update_schedule(svc.session, base.PROJECT_ID, row, update, svc.scope(base.OWNER_ID))
    assert row.delivery_include_history == 0


@pytest.mark.parametrize('value', [-1, 21])
def test_value_outside_the_range_is_rejected_and_stores_nothing(svc, value):
    with pytest.raises(ValidationError):
        base.create(svc, delivery=chat_target(include_history=value))
    assert base.stored_count(svc) == 0
