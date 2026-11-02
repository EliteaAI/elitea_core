"""#7185 harness: the real skill file modules over an in-memory session and an in-memory MinioClient.

The session evaluates query predicates in Python, one session per project schema. The MinioClient keeps objects in
a dict keyed by ``(bucket name with its project prefix, key)``, so a test can see which project's bucket holds what.
"""
import contextlib
import datetime
import importlib.util
import pathlib
import sys
import types
from unittest import mock

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = 'plugins.elitea_core'
BUCKET = 'skill-files'


class Predicate:
    def __init__(self, test):
        self.test = test

    def __invert__(self):
        return Predicate(lambda row: not self.test(row))


class Column:
    def __init__(self, name):
        self.name = name
        self.owner = None

    def __eq__(self, other):
        return Predicate(lambda row: getattr(row, self.name) == other)

    def __ne__(self, other):
        return Predicate(lambda row: getattr(row, self.name) != other)

    def in_(self, values):
        wanted = list(values)
        return Predicate(lambda row: getattr(row, self.name) in wanted)

    def asc(self):
        return self

    def desc(self):
        descending = Column(self.name)
        descending.owner, descending.descending = self.owner, True
        return descending

    def __hash__(self):
        return id(self)


class Lower:
    def __init__(self, column):
        self.column = column

    def __eq__(self, other):
        return Predicate(lambda row: getattr(row, self.column.name).lower() == other)


class Row(tuple):
    """A one-column result: unpacks like a SQLAlchemy row and answers the column name as an attribute."""

    def __new__(cls, name, value):
        row = super().__new__(cls, (value,))
        row.column_name = name
        return row

    def __getattr__(self, name):
        if name == getattr(self, 'column_name', None):
            return self[0]
        raise AttributeError(name)


class FakeQuery:
    def __init__(self, session, model, target, predicates=(), order=(), distinct=False, for_update=False):
        self.session, self.model, self.target = session, model, target
        self.predicates, self.order, self.is_distinct = predicates, order, distinct
        self.for_update = for_update

    def _copy(self, **changes):
        values = {'predicates': self.predicates, 'order': self.order, 'distinct': self.is_distinct,
                  'for_update': self.for_update, **changes}
        return FakeQuery(self.session, self.model, self.target, **values)

    def with_for_update(self):
        return self._copy(for_update=True)

    def filter(self, *predicates):
        return self._copy(predicates=self.predicates + predicates)

    def order_by(self, *columns):
        return self._copy(order=columns)

    def distinct(self):
        return self._copy(distinct=True)

    def options(self, *args):
        return self

    def _rows(self):
        rows = [row for row in self.session.rows[self.model] if all(p.test(row) for p in self.predicates)]
        if self.for_update:
            self.session.locked_rows.extend(rows)
        for column in reversed(self.order):
            rows.sort(key=lambda row, name=column.name: getattr(row, name), reverse=getattr(column, 'descending', False))
        return rows

    def _results(self):
        if isinstance(self.target, type):
            return self._rows()
        results = [Row(self.target.name, getattr(row, self.target.name)) for row in self._rows()]
        if self.is_distinct:
            results = list(dict.fromkeys(results))
        return results

    def first(self):
        results = self._results()
        return results[0] if results else None

    def get(self, row_id):
        return next((row for row in self.session.rows[self.model] if row.id == row_id), None)

    def all(self):
        return self._results()

    def count(self):
        return len(self._rows())

    def delete(self, synchronize_session=False):
        rows = self._rows()
        for row in rows:
            self.session.rows[self.model].remove(row)
        return len(rows)


def _model(name, columns):
    cls = type(name, (), {})
    for column in columns:
        col = Column(column)
        col.owner = cls
        setattr(cls, column, col)
    return cls


FakeSkill = _model('Skill', ['id', 'owner_id', 'shared_owner_id', 'shared_id'])
FakeSkillVersion = _model('SkillVersion', ['id', 'skill_id', 'name', 'status', 'created_at', 'tags'])
FakeSkillVersionFile = _model('SkillVersionFile', ['id', 'skill_version_id', 'path', 'sha256', 'size'])
FakeEntitySkillMapping = _model(
    'EntitySkillMapping', ['skill_version_id', 'skill_id', 'entity_version_id', 'entity_type'],
)


class FakeSession:
    def __init__(self, project_id):
        self.project_id = project_id
        self.rows = {FakeSkill: [], FakeSkillVersion: [], FakeSkillVersionFile: [], FakeEntitySkillMapping: []}
        self.next_id = 100
        self.commits = 0
        self.locked_rows = []
        self.file_reads = []

    def query(self, target):
        model = target if isinstance(target, type) else target.owner
        return FakeQuery(self, model, target)

    def add(self, row):
        row.session = self
        if getattr(row, 'id', None) is None:
            row.id = self.next_id
            self.next_id += 1
        if row not in self.rows[type(row)]:
            self.rows[type(row)].append(row)

    def delete(self, row):
        self.rows[type(row)].remove(row)
        if type(row) is FakeSkill:
            for version in [v for v in self.rows[FakeSkillVersion] if v.skill_id == row.id]:
                self.delete(version)
        if type(row) is FakeSkillVersion:
            self.rows[FakeSkillVersionFile] = [
                f for f in self.rows[FakeSkillVersionFile] if f.skill_version_id != row.id]

    def flush(self):
        pass

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def close(self):
        pass


def _init_version(self, id=None, skill_id=3, name='base', status='draft', instructions='Body.',
                  meta=None, run_settings=None, author_id=1, created_at=None):
    self.id, self.skill_id, self.name, self.status = id, skill_id, name, status
    self.instructions, self.meta, self.run_settings, self.author_id = instructions, meta or {}, run_settings, author_id
    self.created_at = created_at or datetime.datetime(2026, 1, 1)
    self.tags = []


def _version_files(self):
    self.session.file_reads.append((self.id, self in self.session.locked_rows))
    return sorted(
        (f for f in self.session.rows[FakeSkillVersionFile] if f.skill_version_id == self.id),
        key=lambda f: f.path,
    )


def _init_file(self, skill_version_id=None, path=None, size=None, sha256=None, content_type=None, is_text=None):
    self.id = None
    self.skill_version_id, self.path, self.size, self.sha256 = skill_version_id, path, size, sha256
    self.content_type, self.is_text = content_type, is_text
    self.created_at = datetime.datetime(2026, 2, 2)


def _init_skill(self, id=3, owner_id=7, meta=None, name='forms', description='Fill forms.', author_id=1):
    self.id, self.owner_id, self.meta, self.name, self.description = id, owner_id, meta or {}, name, description
    self.author_id, self.shared_owner_id, self.shared_id = author_id, None, None


def _skill_versions(self):
    return [v for v in self.session.rows[FakeSkillVersion] if v.skill_id == self.id]


def _init_mapping(self, skill_version_id, skill_id=3):
    self.id, self.skill_version_id, self.skill_id = None, skill_version_id, skill_id


FakeSkillVersion.__init__ = _init_version
FakeSkillVersion.files = property(_version_files)
FakeSkillVersionFile.__init__ = _init_file
FakeSkill.__init__ = _init_skill
FakeSkill.versions = property(_skill_versions)
FakeEntitySkillMapping.__init__ = _init_mapping


class Storage:
    """Every bucket and object the fake MinioClient instances of one test share."""

    def __init__(self):
        self.buckets = {}
        self.objects = {}
        self.modified = {}
        self.clients = []
        self.bucket_listings = 0
        self.lifecycle_reads = 0
        self.broken_reads = False
        self.broken_deletes = False

    def keys(self, project_id):
        """The keys of a project's skill-files bucket."""
        return sorted(key for bucket, key in self.objects if bucket == f'p--{project_id}.{BUCKET}')


class FakeMinioClient:
    storage = None

    def __init__(self, project_id):
        self.project_id = project_id
        self.storage.clients.append(project_id)

    @classmethod
    def from_project_id(cls, project_id, **kwargs):
        return cls(project_id)

    @property
    def bucket_prefix(self):
        return f'p--{self.project_id}.'

    def format_bucket_name(self, bucket):
        return bucket if bucket.startswith(self.bucket_prefix) else f'{self.bucket_prefix}{bucket}'

    def list_bucket(self):
        self.storage.bucket_listings += 1
        return [name[len(self.bucket_prefix):] for name in self.storage.buckets if name.startswith(self.bucket_prefix)]

    def get_bucket_lifecycle(self, bucket):
        self.storage.lifecycle_reads += 1
        days = self.storage.buckets[self.format_bucket_name(bucket)]['retention_days']
        return {'Rules': [{'Expiration': {'Days': days}}]} if days else {}

    def _load_meta(self, bucket):
        days = self.storage.buckets[self.format_bucket_name(bucket)]['retention_days']
        return {'lifecycle': days} if days else {}

    def _save_meta(self, bucket, meta):
        self.storage.buckets[self.format_bucket_name(bucket)]['retention_days'] = meta.get('lifecycle')

    def create_bucket(self, bucket, bucket_type=None, retention_days=None):
        self.storage.buckets[self.format_bucket_name(bucket)] = {'type': bucket_type, 'retention_days': retention_days}

    def _require_bucket(self, name):
        if name not in self.storage.buckets:
            raise RuntimeError(f'NoSuchBucket: {name}')

    def is_file_exist(self, bucket, file_name):
        self._require_bucket(bucket)
        return (bucket, file_name) in self.storage.objects

    def upload_file(self, bucket, file_obj, file_name):
        name = self.format_bucket_name(bucket)
        assert name in self.storage.buckets, f'upload into missing bucket {name}'
        self.storage.objects[(name, file_name)] = file_obj
        self.storage.modified[(name, file_name)] = '2026-01-01T00:00:00+00:00'

    def get_file_size(self, bucket, filename):
        return len(self.storage.objects.get((self.format_bucket_name(bucket), filename), b''))

    def download_file(self, bucket, file_name, project_id=None):
        if self.storage.broken_reads:
            raise RuntimeError('storage unavailable')
        return self.storage.objects[(self.format_bucket_name(bucket), file_name)]

    def remove_file(self, bucket, file_name):
        if self.storage.broken_deletes:
            raise RuntimeError('storage unavailable')
        del self.storage.objects[(self.format_bucket_name(bucket), file_name)]

    def list_files(self, bucket, next_continuation_token=None):
        name = self.format_bucket_name(bucket)
        self._require_bucket(name)
        return [
            {'name': key, 'size': len(data), 'modified': self.storage.modified[(bucket_name, key)]}
            for (bucket_name, key), data in self.storage.objects.items() if bucket_name == name
        ]


class PublishStatus:
    draft = 'draft'
    published = 'published'
    embedded = 'embedded'


class VersionDetail:
    """Stands in for ``SkillVersionDetailModel``: what create_skill_version returns is the new version's id and name."""

    @classmethod
    def model_validate(cls, version):
        return types.SimpleNamespace(model_dump=lambda mode=None: {'id': version.id, 'name': version.name})


class AnyModule(types.ModuleType):
    """Answers every `from x import name` so a module imports without its collaborators."""

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return mock.MagicMock(name=f'{self.__name__}.{name}')


class StubEverythingElse:
    """A meta path finder that answers any other module of the plugin package with an ``AnyModule``, so a module
    with many collaborators loads with only the ones a test cares about made real."""

    def find_spec(self, fullname, path=None, target=None):
        if not fullname.startswith(f'{PKG}.'):
            return None
        return importlib.util.spec_from_loader(fullname, self)

    def create_module(self, spec):
        return AnyModule(spec.name)

    def exec_module(self, module):
        pass


def stub_everything_else(monkeypatch):
    monkeypatch.setattr(sys, 'meta_path', [*sys.meta_path, StubEverythingElse()])


def load(monkeypatch, relative_path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    return module


def install(monkeypatch, limits: dict):
    """Load the real store, skill_utils, skill_files and skill_export_import over the fakes, after setting aside
    whatever plugin modules earlier tests left behind (monkeypatch puts them back)."""
    for name in [name for name in sys.modules if name == 'plugins' or name.startswith(f'{PKG}')]:
        monkeypatch.delitem(sys.modules, name)
    storage = Storage()
    FakeMinioClient.storage = storage
    sessions = {}

    def session_of(project_id):
        return sessions.setdefault(project_id, FakeSession(project_id))

    @contextlib.contextmanager
    def get_session(project_id):
        yield session_of(project_id)

    for name in (
        'pylon', 'pylon.core', 'pylon.core.tools',
        f'{PKG}.utils.authors', f'{PKG}.utils.utils', f'{PKG}.utils.like_utils',
        f'{PKG}.utils.folder_access', f'{PKG}.utils.skill_run_settings',
        f'{PKG}.models.all', f'{PKG}.models.enums.events',
        f'{PKG}.models.pd.skill', f'{PKG}.models.pd.skill_version', f'{PKG}.models.pd.skill_run_settings',
    ):
        monkeypatch.setitem(sys.modules, name, AnyModule(name))
    for name in ('plugins', PKG, f'{PKG}.utils', f'{PKG}.models', f'{PKG}.models.enums', f'{PKG}.models.pd'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)

    sqlalchemy = types.ModuleType('sqlalchemy')
    sqlalchemy.func = types.SimpleNamespace(lower=Lower)
    for name in ('or_', 'asc', 'desc', 'and_', 'distinct'):
        setattr(sqlalchemy, name, lambda *a, **k: None)
    sqlalchemy_orm = types.ModuleType('sqlalchemy.orm')
    sqlalchemy_orm.selectinload = lambda *a, **k: types.SimpleNamespace(selectinload=lambda *a, **k: None)
    sqlalchemy_orm.joinedload = sqlalchemy_orm.selectinload
    sqlalchemy_exc = types.ModuleType('sqlalchemy.exc')
    sqlalchemy_exc.IntegrityError = type('IntegrityError', (Exception,), {})
    sqlalchemy.exc, sqlalchemy.orm = sqlalchemy_exc, sqlalchemy_orm
    monkeypatch.setitem(sys.modules, 'sqlalchemy', sqlalchemy)
    monkeypatch.setitem(sys.modules, 'sqlalchemy.orm', sqlalchemy_orm)
    monkeypatch.setitem(sys.modules, 'sqlalchemy.exc', sqlalchemy_exc)

    tools = types.ModuleType('tools')
    tools.serialize = lambda model: model.model_dump(mode='json')
    tools.db = types.SimpleNamespace(get_session=get_session, with_project_schema_session=get_session)
    tools.auth = types.SimpleNamespace(current_user=lambda: {'id': 1})
    tools.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config={'skills': limits}))
    tools.rpc_tools = mock.MagicMock()
    tools.context = mock.MagicMock()
    tools.MinioClient = FakeMinioClient
    monkeypatch.setitem(sys.modules, 'tools', tools)

    models_skill = types.ModuleType(f'{PKG}.models.skill')
    models_skill.Skill, models_skill.SkillVersion = FakeSkill, FakeSkillVersion
    models_skill.SkillVersionFile = FakeSkillVersionFile
    models_skill.EntitySkillMapping = FakeEntitySkillMapping
    monkeypatch.setitem(sys.modules, models_skill.__name__, models_skill)
    sys.modules[f'{PKG}.models.pd.skill_version'].SkillVersionDetailModel = VersionDetail
    enums = AnyModule(f'{PKG}.models.enums.all')
    enums.PublishStatus = PublishStatus
    monkeypatch.setitem(sys.modules, enums.__name__, enums)
    export_utils = types.ModuleType(f'{PKG}.utils.export_import_utils')
    export_utils.slugify = lambda text: text.lower().strip().replace(' ', '-')[:50]
    monkeypatch.setitem(sys.modules, export_utils.__name__, export_utils)

    load(monkeypatch, 'models/pd/skill_file.py', f'{PKG}.models.pd.skill_file')
    load(monkeypatch, 'utils/skill_frontmatter.py', f'{PKG}.utils.skill_frontmatter')
    load(monkeypatch, 'utils/platform_buckets.py', f'{PKG}.utils.platform_buckets')
    store = load(monkeypatch, 'utils/skill_file_store.py', f'{PKG}.utils.skill_file_store')
    skill_utils = load(monkeypatch, 'utils/skill_utils.py', f'{PKG}.utils.skill_utils')
    skill_files = load(monkeypatch, 'utils/skill_files.py', f'{PKG}.utils.skill_files')
    skill_export = load(monkeypatch, 'utils/skill_export_import.py', f'{PKG}.utils.skill_export_import')
    return types.SimpleNamespace(
        store=store, utils=skill_utils, files=skill_files, export=skill_export, tools=tools,
        storage=storage, session_of=session_of,
    )
