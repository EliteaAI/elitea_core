"""Shared draft helpers added for PR #660 review: offered-origin matching and settings resolution.

Run via:
    python tests/run_tests.py unit/utils/test_7323_draft_shared_helpers.py -v
"""
import pathlib
import types

import pytest

from fixtures.helpers import load_utils_module

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
PROJECT = 6
PUBLIC_PROJECT = 1


@pytest.fixture(scope='module')
def utils():
    utils_stub = types.ModuleType('utils')
    utils_stub.get_public_project_id = lambda: PUBLIC_PROJECT
    return load_utils_module(
        TESTS_DIR.parent / 'utils',
        'draft_llm_utils',
        extra_stubs={'plugins.elitea_core.utils.utils': utils_stub},
    )


OWN_AGENT_5 = {'application_id': 5, 'name': 'own', 'type': 'agent'}
CATALOG_AGENT_5 = {'application_id': 5, 'name': 'catalog', 'type': 'agent', 'project_id': PUBLIC_PROJECT}
OWN_SKILL_3 = {'id': 3, 'name': 'own skill'}
CATALOG_SKILL_3 = {'id': 3, 'name': 'catalog skill', 'project_id': PUBLIC_PROJECT}


def _apply(utils, parsed, agents=(), pipelines=(), skills=()):
    utils.apply_offered_origins(parsed, PROJECT, list(agents), list(pipelines), list(skills))
    return parsed


class TestSameIdInBothProjects:
    """The reviewer's case: own agent 5 and catalog agent 5 are both on offer."""

    def test_own_item_echoed_with_current_project_stays_unstamped(self, utils):
        parsed = _apply(utils, {'suggested_agents': [{'application_id': 5, 'project_id': PROJECT}]},
                        agents=[OWN_AGENT_5, CATALOG_AGENT_5])
        assert parsed['suggested_agents'] == [{'application_id': 5, 'project_id': None, 'description': None}]

    def test_catalog_item_echoed_with_catalog_project_is_stamped(self, utils):
        parsed = _apply(utils, {'suggested_agents': [{'application_id': 5, 'project_id': PUBLIC_PROJECT}]},
                        agents=[OWN_AGENT_5, CATALOG_AGENT_5])
        assert parsed['suggested_agents'] == [{'application_id': 5, 'project_id': PUBLIC_PROJECT, 'description': None}]

    def test_ambiguous_id_without_project_is_dropped(self, utils):
        parsed = _apply(utils, {'suggested_agents': [{'application_id': 5}]},
                        agents=[OWN_AGENT_5, CATALOG_AGENT_5])
        assert parsed['suggested_agents'] == []

    def test_same_id_skills_resolve_by_pair(self, utils):
        parsed = _apply(utils, {'suggested_skills': [
            {'id': 3, 'project_id': PROJECT}, {'id': 3, 'project_id': PUBLIC_PROJECT},
        ]}, skills=[OWN_SKILL_3, CATALOG_SKILL_3])
        assert [s['project_id'] for s in parsed['suggested_skills']] == [None, PUBLIC_PROJECT]


class TestOfferedOnly:
    def test_unambiguous_id_without_project_is_accepted(self, utils):
        parsed = _apply(utils, {'suggested_agents': [{'application_id': 5}]}, agents=[CATALOG_AGENT_5])
        assert parsed['suggested_agents'] == [{'application_id': 5, 'project_id': PUBLIC_PROJECT, 'description': None}]

    def test_pair_that_was_never_offered_is_dropped(self, utils):
        parsed = _apply(utils, {'suggested_agents': [{'application_id': 5, 'project_id': 99}]},
                        agents=[OWN_AGENT_5])
        assert parsed['suggested_agents'] == []

    def test_unknown_id_is_dropped(self, utils):
        parsed = _apply(utils, {'suggested_pipelines': [{'application_id': 77, 'type': 'pipeline'}]},
                        pipelines=[{'application_id': 8, 'name': 'p', 'type': 'pipeline'}])
        assert parsed['suggested_pipelines'] == []

    def test_agents_and_pipelines_are_matched_separately(self, utils):
        parsed = _apply(utils, {'suggested_pipelines': [{'application_id': 5}]}, agents=[OWN_AGENT_5])
        assert parsed['suggested_pipelines'] == []

    @pytest.mark.parametrize('value', [None, [], ['not a dict']])
    def test_missing_or_junk_lists_become_empty(self, utils, value):
        parsed = _apply(utils, {'suggested_agents': value}, agents=[OWN_AGENT_5])
        assert parsed['suggested_agents'] == []


class _Requested:
    """Minimal stand-in for LLMSettingsRequest: tracks which fields the caller set."""

    def __init__(self, **fields):
        self.__dict__.update({'model_name': None, 'model_project_id': None, 'max_tokens': None, 'temperature': None})
        self.__dict__.update(fields)
        self.model_fields_set = set(fields)

    def model_dump(self, exclude_none=False, exclude=()):
        data = {k: v for k, v in self.__dict__.items() if k != 'model_fields_set' and k not in exclude}
        return {k: v for k, v in data.items() if v is not None} if exclude_none else data


@pytest.fixture
def default_model(utils, monkeypatch):
    def _set(model=None, fails=False):
        class _Rpc:
            def timeout(self, _s):
                return self

            def configurations_get_default_model(self, project_id, section='llm'):
                if fails:
                    raise RuntimeError('rpc down')
                return dict(model) if model else model

        monkeypatch.setattr(utils, 'rpc_tools', types.SimpleNamespace(RpcMixin=lambda: types.SimpleNamespace(rpc=_Rpc())))
    return _set


def _resolve(utils, requested=None, fallback=None):
    return utils.resolve_draft_llm_settings(
        PROJECT, requested, default_temperature=0.3, default_max_tokens=4096, fallback_settings=fallback,
    )


class TestResolveDraftLlmSettings:
    def test_project_default_gets_endpoint_defaults(self, utils, default_model):
        default_model({'model_name': 'gpt-5-mini', 'temperature': 1.0})
        assert _resolve(utils) == {'model_name': 'gpt-5-mini', 'temperature': 0.3, 'max_tokens': 4096}

    def test_caller_overrides_win_over_endpoint_defaults(self, utils, default_model):
        default_model({'model_name': 'gpt-5-mini'})
        settings = _resolve(utils, _Requested(temperature=0.9, max_tokens=500))
        assert (settings['temperature'], settings['max_tokens']) == (0.9, 500)

    def test_fallback_settings_used_before_project_default(self, utils, default_model):
        default_model(fails=True)
        settings = _resolve(utils, _Requested(temperature=0.5), fallback={'model_name': 'own-model', 'temperature': 1.0})
        assert settings['model_name'] == 'own-model' and settings['temperature'] == 0.5

    def test_no_default_model_is_a_400(self, utils, default_model):
        default_model(None)
        with pytest.raises(utils.DraftSettingsError) as err:
            _resolve(utils)
        assert err.value.status == 400 and 'No LLM model configured' in err.value.body['error']

    def test_default_model_lookup_failure_is_a_400(self, utils, default_model):
        default_model(fails=True)
        with pytest.raises(utils.DraftSettingsError) as err:
            _resolve(utils)
        assert err.value.status == 400

    def test_explicit_unavailable_model_is_a_400(self, utils, monkeypatch):
        monkeypatch.setattr(utils, 'resolve_model', lambda *a: ('Model gone', None))
        with pytest.raises(utils.DraftSettingsError) as err:
            _resolve(utils, _Requested(model_name='gone'))
        assert err.value.body == {'error': 'Model gone'}

    def test_explicit_model_is_stamped_with_owning_project(self, utils, monkeypatch):
        monkeypatch.setattr(utils, 'resolve_model', lambda *a: (None, PUBLIC_PROJECT))
        settings = _resolve(utils, _Requested(model_name='shared-model'))
        assert settings['model_project_id'] == PUBLIC_PROJECT
