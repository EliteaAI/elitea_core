"""Toolkit Index schedules in the governance readiness: only the toolkit's credentials count, no version, no mandate."""
import pytest

from unit import test_7269_readiness_requests as harness

MEMBER, OWNER = harness.MEMBER, harness.OWNER
credential, items, no_risks = harness.credential, harness.items, harness.no_risks
env, ns = harness.env, harness.ns  # the harness fixtures, bound here under their own names


def index_schedule(ns, env, toolkit_id=1, owner=OWNER, index_name="docs"):
    if ns.EntitySchedule is None:
        pytest.skip("the schedule model could not be loaded in this harness")
    with ns.sa_orm.Session(env.engine(1)) as session:
        row = ns.EntitySchedule(entity_type="toolkit_index", toolkit_id=toolkit_id, index_name=index_name,
                                name="Nightly index", cron="0 3 * * *", timezone="Etc/UTC", variables=[],
                                enabled=True, created_by=owner, principal={"kind": "delegation", "ref": owner})
        session.add(row)
        session.commit()
        return row.id


def test_an_index_schedule_lists_the_credentials_of_its_toolkit(ns, env, monkeypatch):
    no_risks(monkeypatch, ns)
    cred = credential(env, version_id=70)
    found = items(ns, env, user=OWNER, kind="schedule", entity=index_schedule(ns, env))
    assert list(found) == [f"reference:{cred.uuid}"]


def test_an_index_schedule_has_no_version_or_mandate_item(ns, env, monkeypatch):
    no_risks(monkeypatch, ns)
    credential(env, version_id=70)
    found = items(ns, env, user=OWNER, kind="schedule", entity=index_schedule(ns, env))
    assert not {"blocking:version", "mandate", "binding"} & set(found)


def test_an_index_schedule_is_its_owners(ns, env, monkeypatch):
    no_risks(monkeypatch, ns)
    credential(env, version_id=70)
    schedule_id = index_schedule(ns, env)
    with pytest.raises(ns.governance_readiness.EntityNotFound):
        items(ns, env, user=MEMBER, kind="schedule", entity=schedule_id)


def test_an_index_schedule_of_a_deleted_toolkit_is_not_found(ns, env, monkeypatch):
    no_risks(monkeypatch, ns)
    schedule_id = index_schedule(ns, env, toolkit_id=999)
    with pytest.raises(ns.governance_readiness.EntityNotFound):
        items(ns, env, user=OWNER, kind="schedule", entity=schedule_id)


def test_the_toolkit_kind_gives_the_same_items_as_its_index_schedule(ns, env, monkeypatch):
    no_risks(monkeypatch, ns)
    credential(env, version_id=70)
    by_kind = items(ns, env, user=OWNER, kind="toolkit", entity=1)
    by_schedule = items(ns, env, user=OWNER, kind="schedule", entity=index_schedule(ns, env))
    assert by_schedule == by_kind
