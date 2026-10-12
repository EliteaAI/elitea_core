import hashlib
import pathlib
import sys
import types

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_5803_skill_twin_catalog_invisibility import (  # noqa: E402  pylint: disable=C0413
    FakeSession,
    _fake_db,
    _fork_payload,
    _skill_info,
    pu,  # noqa: F401  pylint: disable=W0611
)

RUN_SETTINGS = {'llm_settings': {'model_name': 'gpt-4.1', 'model_project_id': 1}, 'ignore_project_context': True}


def test_versions_without_run_settings_keep_the_pre_6921_digest(pu):
    assert pu._skill_content_sha('x' * 120) == hashlib.sha256(('x' * 120).encode()).hexdigest()
    assert pu._skill_content_sha('x' * 120, None) == pu._skill_content_sha('x' * 120, {})


def test_run_settings_change_the_digest(pu):
    plain = pu._skill_content_sha('x' * 120)
    tuned = pu._skill_content_sha('x' * 120, RUN_SETTINGS)
    retuned = pu._skill_content_sha('x' * 120, {**RUN_SETTINGS, 'ignore_project_context': False})
    assert len({plain, tuned, retuned}) == 3


def test_digest_ignores_key_order(pu):
    reordered = {'ignore_project_context': True, 'llm_settings': {'model_project_id': 1, 'model_name': 'gpt-4.1'}}
    assert pu._skill_content_sha('x', RUN_SETTINGS) == pu._skill_content_sha('x', reordered)


def test_twin_is_stamped_with_the_run_settings_digest(pu, monkeypatch):
    captured = []

    def import_wizard(entities, project_id, user_id):
        captured.append(entities[0])
        return {'skills': [{'id': 77}]}, {}

    monkeypatch.setattr(pu, 'build_skill_fork_payload', lambda *a, **k: _fork_payload())
    monkeypatch.setattr(pu, 'this', types.SimpleNamespace(module=types.SimpleNamespace(import_wizard=import_wizard)))

    for run_settings in (None, RUN_SETTINGS):
        monkeypatch.setattr(pu, 'db', _fake_db([FakeSession([[]]), FakeSession([[(501,)]])]))
        pu._resolve_or_fork_skill_twin(2, 1, {**_skill_info(), 'run_settings': run_settings}, 3)

    shas = [payload['versions'][0]['meta'][pu._TWIN_CONTENT_SHA] for payload in captured]
    assert shas == [pu._skill_content_sha('x' * 120), pu._skill_content_sha('x' * 120, RUN_SETTINGS)]
    assert captured[0]['import_uuid'] != captured[1]['import_uuid']


class FakeMapping:
    def __init__(self, run_settings):
        self.skill_version = types.SimpleNamespace(run_settings=run_settings)


def test_attached_skill_run_settings_reach_the_twin_resolver(pu, monkeypatch):
    mapping = FakeMapping(RUN_SETTINGS)
    seen = []
    monkeypatch.setattr(pu, 'build_skill_mappings_list', lambda mappings: [{**_skill_info()}])
    monkeypatch.setattr(pu, '_resolve_or_fork_skill_twin', lambda *args: seen.append(args[2]) or (77, 501))
    monkeypatch.setattr(pu, 'attach_skill_to_public_copy', lambda **kwargs: None)
    monkeypatch.setattr(pu, 'db', _fake_db([FakeSession([[mapping]])]))

    assert pu.publish_attached_skills(
        source_project_id=2, public_project_id=1, source_version_id=5, public_version_id=6, user_id=3,
    ) is None
    assert seen[0]['run_settings'] == RUN_SETTINGS
