"""Issue #6408 - the "only version" guard counted, but keyed on the name.

`if version.name == 'base' and other_versions == 0` let a skill whose sole
version was named anything else be deleted down to zero versions, leaving an
orphan Skill row. And nothing moved meta['default_version_id'] off the row it
had just deleted, so get_default_version() lost its pointer arm as well as its
'base' arm and returned None.

Run via:
    python tests/run_tests.py integration/test_6408_delete_last_version.py -v
"""
import pathlib
import sys
import types

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from test_5955_published_mapping_guards import skill_utils_module  # noqa: E402,F401


class _Query:
    def __init__(self, session, model):
        self._session = session
        self._model = model

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def options(self, *args, **kwargs):
        return self

    def _pop(self):
        queued = self._session.results.get(self._model)
        if isinstance(queued, list) and queued:
            return queued.pop(0)
        return queued

    def first(self):
        return self._pop()

    def all(self):
        result = self._pop()
        return result if isinstance(result, list) else ([] if result is None else [result])

    def count(self):
        return self._session.counts.get(self._model, 0)


class _Session:
    """Answers each query(Model) from a queue, so one model can reply differently per call."""

    def __init__(self, results, counts=None):
        self.results = {k: list(v) for k, v in results.items()}
        self.counts = dict(counts or {})
        self.deleted = []

    def query(self, model):
        return _Query(self, model)

    def delete(self, obj):
        self.deleted.append(obj)

    def flush(self):
        pass


class _SkillSession:
    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


def _version(version_id, name, created_at=0, status='draft'):
    return types.SimpleNamespace(
        id=version_id, name=name, created_at=created_at, status=status,
    )


def _skill(skill_id=1, default_version_id=None):
    meta = {'default_version_id': default_version_id} if default_version_id else {}
    return types.SimpleNamespace(id=skill_id, meta=meta)


@pytest.fixture
def bind_session(skill_utils_module, monkeypatch):  # noqa: F811
    def _bind(session):
        monkeypatch.setattr(
            skill_utils_module,
            '_skill_session',
            lambda *args, **kwargs: _SkillSession(session),
        )
        return session

    return _bind


def _models():
    models_skill = sys.modules['plugins.elitea_core.models.skill']
    return models_skill.SkillVersion, models_skill.Skill, models_skill.EntitySkillMapping


class TestTheOnlyVersionCannotBeDeleted:
    @pytest.mark.parametrize('sole_version_name', ['base', 'v1-custom'])
    def test_the_guard_counts_rather_than_checking_the_name(
        self, skill_utils_module, bind_session, sole_version_name  # noqa: F811
    ):
        """A skill created before the 'base' rule has a differently-named sole
        version; it must be just as undeletable."""
        skill_version, _, _ = _models()
        doomed = _version(10, sole_version_name)
        bind_session(_Session({skill_version: [doomed, []]}))

        with pytest.raises(skill_utils_module.SkillVersionNotUpdatableError) as exc:
            skill_utils_module.delete_skill_version(
                project_id=2, skill_id=1, version_id=10,
            )

        assert 'only version' in str(exc.value)


class TestDefaultVersionPointerIsRepointed:
    def test_deleting_the_default_version_moves_the_pointer(
        self, skill_utils_module, bind_session  # noqa: F811
    ):
        skill_version, skill_model, mapping = _models()
        doomed = _version(10, 'v2', created_at=2)
        survivor = _version(9, 'base', created_at=1)
        owner = _skill(default_version_id=10)
        session = bind_session(_Session(
            {skill_version: [doomed, [survivor]], skill_model: [owner]},
            counts={mapping: 0},
        ))

        skill_utils_module.delete_skill_version(
            project_id=2, skill_id=1, version_id=10,
        )

        assert session.deleted == [doomed]
        assert owner.meta['default_version_id'] == 9

    def test_deleting_a_non_default_version_leaves_the_pointer_alone(
        self, skill_utils_module, bind_session  # noqa: F811
    ):
        skill_version, skill_model, mapping = _models()
        doomed = _version(10, 'v2', created_at=2)
        survivor = _version(9, 'base', created_at=1)
        owner = _skill(default_version_id=9)
        bind_session(_Session(
            {skill_version: [doomed, [survivor]], skill_model: [owner]},
            counts={mapping: 0},
        ))

        skill_utils_module.delete_skill_version(
            project_id=2, skill_id=1, version_id=10,
        )

        assert owner.meta['default_version_id'] == 9

    def test_the_successor_is_the_oldest_when_no_base_survives(
        self, skill_utils_module, bind_session  # noqa: F811
    ):
        """Matches the order models/pd/skill.py already uses to repair a null
        pointer on read, so the stored value agrees with the reported one."""
        skill_version, skill_model, mapping = _models()
        doomed = _version(10, 'v3', created_at=3)
        newer = _version(12, 'v2', created_at=2)
        oldest = _version(11, 'v1', created_at=1)
        owner = _skill(default_version_id=10)
        bind_session(_Session(
            {skill_version: [doomed, [newer, oldest]], skill_model: [owner]},
            counts={mapping: 0},
        ))

        skill_utils_module.delete_skill_version(
            project_id=2, skill_id=1, version_id=10,
        )

        assert owner.meta['default_version_id'] == 11
