"""#7185: a skill invoked with ``~name`` carries its file list, so the per-turn section can show the skill's files.

Found in the option C live proof: the body of a ``~pdf`` turn rendered without the files block because the invoked
entry dropped ``files`` while the registry entry kept it.
"""
import sys

import pytest

sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent))

from test_6926_runtime_skill_registry import skill_utils  # noqa: E402,F401  pylint: disable=C0413

FORMS = {'path': 'forms.md', 'size': 12, 'content_type': 'text/markdown', 'is_text': True}


def _pdf(files):
    return {'skill_id': 1, 'skill_version_id': 10, 'name': 'pdf', 'description': 'PDF', 'instructions': 'Read forms.md', 'files': files}


def test_an_invoked_skill_carries_its_runtime_file_list(skill_utils):
    _, invoked = skill_utils.consume_invoked_skills('~pdf fill this form', [_pdf([FORMS])])

    assert invoked[0]['files'] == [FORMS]


@pytest.mark.parametrize('files', [[], None])
def test_a_file_less_invoked_skill_carries_no_file_list(skill_utils, files):
    _, invoked = skill_utils.consume_invoked_skills('~pdf go', [_pdf(files)])

    assert 'files' not in invoked[0]
