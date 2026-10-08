import types

import pytest

from fixtures.helpers import load_module_with_stubs

PACKAGE = 'plugins.elitea_core'


def _package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    return module


@pytest.fixture
def modules(isolated_sys_modules, plugin_root):
    packages = {name: _package(name) for name in (
        'plugins', PACKAGE, f'{PACKAGE}.utils', f'{PACKAGE}.models', f'{PACKAGE}.models.pd',
        f'{PACKAGE}.models.enums',
    )}
    load_module_with_stubs(plugin_root / 'models/enums/all.py', f'{PACKAGE}.models.enums.all', packages)
    load_module_with_stubs(plugin_root / 'models/pd/llm.py', f'{PACKAGE}.models.pd.llm')
    settings = load_module_with_stubs(
        plugin_root / 'models/pd/participant_settings.py', f'{PACKAGE}.models.pd.participant_settings',
    )
    override = load_module_with_stubs(
        plugin_root / 'utils/skill_llm_override.py', f'{PACKAGE}.utils.skill_llm_override',
    )
    return types.SimpleNamespace(EntitySettingsLlm=settings.EntitySettingsLlm, override=override)


CHAT_CHOICE = {'model_name': 'opus', 'model_project_id': 1, 'max_tokens': 8192}


def _predict_payload(modules, llm_settings):
    return types.SimpleNamespace(llm_settings=modules.EntitySettingsLlm.model_validate(llm_settings))


class TestSelectLlmOverride:
    def test_a_message_without_settings_keeps_the_chat_model(self, modules):
        payload = _predict_payload(modules, {})
        assert modules.override.select_llm_override({'llm_settings': CHAT_CHOICE}, payload) == CHAT_CHOICE

    def test_explicit_message_settings_still_win(self, modules):
        payload = _predict_payload(modules, {'model_name': 'haiku', 'model_project_id': 1})
        override = modules.override.select_llm_override({'llm_settings': CHAT_CHOICE}, payload)
        assert override['model_name'] == 'haiku'

    def test_settings_survive_the_socket_handlers_second_validation(self, modules):
        first = modules.EntitySettingsLlm.model_validate({'model_name': 'haiku', 'model_project_id': 1})
        payload = types.SimpleNamespace(llm_settings=modules.EntitySettingsLlm.model_validate(first.model_dump()))
        override = modules.override.select_llm_override({'llm_settings': CHAT_CHOICE}, payload)
        assert override['model_name'] == 'haiku'

    def test_no_choice_anywhere_means_no_override(self, modules):
        assert modules.override.select_llm_override({}, _predict_payload(modules, {})) is None
        assert modules.override.select_llm_override({}, types.SimpleNamespace(llm_settings=None)) is None
