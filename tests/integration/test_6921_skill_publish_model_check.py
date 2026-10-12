import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_6071_publish_icon_check import pu, spu  # noqa: E402,F401  pylint: disable=C0413,W0611

PRIVATE = {'llm_settings': {'model_name': 'private', 'model_project_id': 2}}


def blocks_private_models(run_settings, public_project_id):
    model = ((run_settings or {}).get('llm_settings') or {})
    if model.get('model_name') and model.get('model_project_id') != public_project_id:
        return f"Model '{model['model_name']}' is not a shared model"
    return None


def checks(spu, run_settings):
    skill_data = {'skill': {'name': 'Reviewer', 'description': 'd' * 60}, 'run_settings': run_settings}
    return spu.run_skill_deterministic_checks(skill_data, 'v1')


def run_settings_issues(result):
    return [issue for issue in result['critical_issues'] if issue['field'] == 'run_settings']


def test_private_model_is_a_critical_publish_issue(spu, monkeypatch):
    monkeypatch.setattr(spu, 'unshared_model_issue', blocks_private_models)
    issues = run_settings_issues(checks(spu, PRIVATE))
    assert [(issue['issue'], issue['fix']) for issue in issues] == [
        ("Model 'private' is not a shared model", spu.SHARED_MODEL_REQUIRED),
    ]


def test_version_without_a_model_has_no_run_settings_issue(spu, monkeypatch):
    monkeypatch.setattr(spu, 'unshared_model_issue', blocks_private_models)
    assert run_settings_issues(checks(spu, None)) == []
    assert run_settings_issues(checks(spu, {'ignore_project_context': True})) == []
