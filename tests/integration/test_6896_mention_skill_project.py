"""#6896 - a ~mentioned Catalog chat skill keeps its own project for usage attribution."""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_5955_published_mapping_guards import skill_utils_module  # noqa: E402,F401


def test_a_mentioned_chat_skill_keeps_its_project(skill_utils_module):  # noqa: F811
    catalog_skill = {
        'skill_id': 10, 'skill_version_id': 200, 'skill_project_id': 1,
        'name': 'ui-review', 'instructions': 'Review the screenshot.',
    }

    _cleaned, invoked = skill_utils_module.consume_invoked_skills('use ~ui-review', [catalog_skill])

    assert [(s['skill_project_id'], s['skill_id']) for s in invoked] == [(1, 10)]


def test_an_own_skill_has_no_project_override(skill_utils_module):  # noqa: F811
    own_skill = {'skill_id': 9, 'skill_version_id': 90, 'name': 'reviewer', 'instructions': 'Review.'}

    _cleaned, invoked = skill_utils_module.consume_invoked_skills('use ~reviewer', [own_skill])

    assert invoked[0]['skill_project_id'] is None
