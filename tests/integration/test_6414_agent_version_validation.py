"""Issue #6414 - a skill mapping must not reference an agent version that is not there.

EntitySkillMapping.entity_version_id carries no foreign key, and every guard on
it tested truthiness, so a missing row fell THROUGH the guard rather than
failing it: the read answered `{"skills": [], "max_skills": 5}` - identical to a
real, empty agent version - and the write inserted a mapping onto a phantom.

Reuses the stub tree from test_5955_published_mapping_guards; the session fake
here is local because these paths need per-model call ordering and `count()`.

Run via:
    python tests/run_tests.py integration/test_6414_agent_version_validation.py -v
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

    def filter_by(self, *args, **kwargs):
        return self

    def options(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def _next(self):
        results = self._session.results.get(self._model)
        if isinstance(results, list) and results and isinstance(results[0], _Result):
            return results.pop(0).value if len(results) > 1 else results[0].value
        return results

    def first(self):
        return self._next()

    def all(self):
        result = self._next()
        if result is None:
            return []
        return result if isinstance(result, list) else [result]

    def count(self):
        return self._session.counts.get(self._model, 0)

    def delete(self):
        return None


class _Result:
    """Marks a queued per-call result, so one model can answer twice differently."""

    def __init__(self, value):
        self.value = value


class _Session:
    def __init__(self, results=None, counts=None):
        self.results = dict(results or {})
        self.counts = dict(counts or {})
        self.added = []
        self.deleted = []

    def query(self, model):
        return _Query(self, model)

    def add(self, obj):
        self.added.append(obj)

    def delete(self, obj):
        self.deleted.append(obj)

    def flush(self):
        pass


def _draft_agent_version(version_id=5):
    return types.SimpleNamespace(id=version_id, status='draft')


class _SkillSession:
    def __init__(self, session):
        self._session = session

    def __enter__(self):
        return self._session

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


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


def _models(skill_utils_module):  # noqa: F811
    import sys

    models_all = sys.modules['plugins.elitea_core.models.all']
    models_skill = sys.modules['plugins.elitea_core.models.skill']
    return models_all.ApplicationVersion, models_skill.EntitySkillMapping, models_skill.Skill


class TestReadRejectsAMissingAgentVersion:
    def test_a_phantom_version_is_a_404_not_an_empty_list(
        self, skill_utils_module, bind_session  # noqa: F811
    ):
        application_version, _, _ = _models(skill_utils_module)
        bind_session(_Session({application_version: None}))

        with pytest.raises(skill_utils_module.AgentVersionNotFoundError) as exc:
            skill_utils_module.get_available_skills_for_agent(
                project_id=2, entity_version_id=999999,
            )

        assert exc.value.http_status == 404
        assert '999999' in str(exc.value)

    def test_a_real_version_with_no_skills_still_returns_an_empty_list(
        self, skill_utils_module, bind_session  # noqa: F811
    ):
        application_version, mapping, _ = _models(skill_utils_module)
        bind_session(_Session({
            application_version: _draft_agent_version(),
            mapping: [],
        }))

        skills = skill_utils_module.get_available_skills_for_agent(
            project_id=2, entity_version_id=5,
        )

        assert skills == []


class TestWriteRejectsAMissingAgentVersion:
    def test_attach_no_longer_falls_through_the_guard(
        self, skill_utils_module, bind_session  # noqa: F811
    ):
        application_version, _, _ = _models(skill_utils_module)
        session = bind_session(_Session({application_version: None}))

        with pytest.raises(skill_utils_module.AgentVersionNotFoundError):
            skill_utils_module.attach_skill_to_agent(
                project_id=2, entity_version_id=999999,
                skill_id=1, skill_version_id=1,
            )

        assert session.added == [], 'a mapping was written onto a phantom agent version'

    def test_detach_blames_the_agent_version_not_the_skill(
        self, skill_utils_module, bind_session  # noqa: F811
    ):
        application_version, _, _ = _models(skill_utils_module)
        bind_session(_Session({application_version: None}))

        with pytest.raises(skill_utils_module.AgentVersionNotFoundError):
            skill_utils_module.detach_skill_from_agent(
                project_id=2, entity_version_id=999999, skill_id=1,
            )


class _Col:
    """Accepts the comparisons SQLAlchemy columns support; the fake query ignores them."""

    def __eq__(self, other):
        return self

    def __ne__(self, other):
        return self

    def __getitem__(self, item):
        return self

    def in_(self, values):
        return self

    def distinct(self):
        return self

    @property
    def astext(self):
        return self

    def asc(self):
        return self


class _ColumnModel:
    def __getattr__(self, name):
        return _Col()


class _OrderedQuery:
    """Answers each terminal call from a flat queue, in the order the code asks."""

    def __init__(self, queue):
        self._queue = queue

    def _pop(self):
        return self._queue.pop(0) if self._queue else None

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def distinct(self, *args, **kwargs):
        return self

    def first(self):
        return self._pop()

    def all(self):
        return self._pop() or []


class _Savepoint:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


class _BatchSession:
    def __init__(self, queue):
        self._queue = list(queue)

    def query(self, *args, **kwargs):
        return _OrderedQuery(self._queue)

    def begin_nested(self):
        return _Savepoint()

    def flush(self):
        pass


class TestBatchAttachIsolatesOneBadAgent:
    """The per-agent savepoint exists so one failure cannot take the batch with it.

    A published agent version raises AgentVersionNotUpdatableError, which escaped
    the loop: the caller never reaches its commit, so every attachment that had
    already succeeded is rolled back too.
    """

    def _run(self, skill_utils_module, monkeypatch, failures):  # noqa: F811
        export_import = types.ModuleType(
            'plugins.elitea_core.utils.skill_export_import'
        )
        export_import.build_skill_fork_payload = lambda *args, **kwargs: None
        monkeypatch.setitem(
            sys.modules, 'plugins.elitea_core.utils.skill_export_import', export_import
        )
        monkeypatch.setattr(
            skill_utils_module, 'auth',
            types.SimpleNamespace(current_user=lambda: {'id': 1}),
        )
        monkeypatch.setattr(skill_utils_module, 'get_public_project_id', lambda: 1)
        for model in ('ApplicationVersion', 'Application', 'SkillVersion',
                      'EntitySkillMapping'):
            monkeypatch.setattr(skill_utils_module, model, _ColumnModel())
        monkeypatch.setattr(
            skill_utils_module, 'AgentTypes',
            types.SimpleNamespace(pipeline=types.SimpleNamespace(value='pipeline')),
        )

        def _attach(project_id, entity_version_id, **kwargs):
            if entity_version_id in failures:
                raise failures[entity_version_id]

        monkeypatch.setattr(skill_utils_module, 'attach_skill_to_agent', _attach)

        session = _BatchSession([
            [(146,), (147,)],   # (0) owned agent versions
            (10, 20),           # (1) local copy of the public skill resolves
            [(10,)],            # (2) lineage skill ids
            [],                 # (3) nothing already attached
        ])
        return skill_utils_module.attach_public_skill_to_agents(
            project_id=2, public_skill_id=99, public_version_id=98,
            agent_version_ids=[146, 147], session=session,
        )

    def test_a_published_agent_is_reported_without_aborting_the_batch(
        self, skill_utils_module, monkeypatch  # noqa: F811
    ):
        results = self._run(skill_utils_module, monkeypatch, {
            146: skill_utils_module.AgentVersionNotUpdatableError(146, 'published'),
        })

        by_id = {r['agent_version_id']: r for r in results}
        assert by_id[146]['ok'] is False
        assert by_id[147]['ok'] is True, 'the healthy agent lost its attachment'

    def test_the_refusal_is_not_reported_as_already_attached(
        self, skill_utils_module, monkeypatch  # noqa: F811
    ):
        """AttachToAgentDialog buckets any 409 as a benign "already added" no-op,
        so a refused attach must not use that status or the user is told the skill
        was added when it was not."""
        results = self._run(skill_utils_module, monkeypatch, {
            146: skill_utils_module.AgentVersionNotUpdatableError(146, 'published'),
        })

        refused = next(r for r in results if r['agent_version_id'] == 146)
        assert refused['http_status'] != 409

    def test_all_agents_still_attach_when_none_are_published(
        self, skill_utils_module, monkeypatch  # noqa: F811
    ):
        results = self._run(skill_utils_module, monkeypatch, {})

        assert [r['ok'] for r in results] == [True, True]
