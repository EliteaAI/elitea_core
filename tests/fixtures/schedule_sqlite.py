"""Shared SQLite set-up of the EL-7011 schedule tests: real sqlalchemy, parent tables, schedule table, rows."""
import sys
import types
from unittest.mock import MagicMock


def _real_sqlalchemy():
    """Real sqlalchemy modules, even when an earlier suite left stubs (no `__file__`) in sys.modules."""
    stubs = {n: m for n, m in sys.modules.items()
             if (n == 'sqlalchemy' or n.startswith('sqlalchemy.')) and not hasattr(m, '__file__')}
    for name in stubs:
        del sys.modules[name]
    try:
        import sqlalchemy.dialects.postgresql
        import sqlalchemy.exc
        import sqlalchemy.ext.compiler
        import sqlalchemy.orm  # noqa: F401
        return {n: m for n, m in sys.modules.items() if n == 'sqlalchemy' or n.startswith('sqlalchemy.')}
    finally:
        sys.modules.update(stubs)


REAL_SQLALCHEMY = _real_sqlalchemy()
sa = REAL_SQLALCHEMY['sqlalchemy']
orm = REAL_SQLALCHEMY['sqlalchemy.orm']


@REAL_SQLALCHEMY['sqlalchemy.ext.compiler'].compiles(REAL_SQLALCHEMY['sqlalchemy.dialects.postgresql'].JSONB, 'sqlite')
def _jsonb_as_json(type_, compiler, **kw):
    return 'JSON'


class LenientModule(types.ModuleType):
    """A module whose unknown names are mocks, for imports the code under test never exercises."""

    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return MagicMock(name=name)


SCHEDULE_HANDLERS = {
    'schedule_item_handlers': 'ScheduleItemHandlers',
    'schedule_project_handlers': 'ScheduleProjectHandlers',
    'schedule_draft_handler': 'ScheduleDraftHandler',
}


def handler_stubs(plugin_package, real):
    """Modules with an empty handler class, so `api/v2/schedules.py` loads with only the `real` helper module."""
    stubs = {}
    for name, class_name in SCHEDULE_HANDLERS.items():
        if name != real:
            module = types.ModuleType(f'{plugin_package}.utils.{name}')
            setattr(module, class_name, type(class_name, (), {}))
            stubs[module.__name__] = module
    return stubs


def package(name):
    module = types.ModuleType(name)
    module.__path__ = []
    sys.modules[name] = module
    return module


def install_parent_models(plugin_package):
    """Stub the plugin's model base and add the Application and ApplicationVersion tables; returns them."""

    class Base(orm.DeclarativeBase):
        pass

    class Application(Base):
        __tablename__ = 'applications'
        __table_args__ = ({'schema': 'tenant'},)
        id = sa.Column(sa.Integer, primary_key=True)
        name = sa.Column(sa.String, default='Entity')

    class ApplicationVersion(Base):
        __tablename__ = 'application_versions'
        __table_args__ = ({'schema': 'tenant'},)
        id = sa.Column(sa.Integer, primary_key=True)
        application_id = sa.Column(sa.Integer, sa.ForeignKey('tenant.applications.id'))
        name = sa.Column(sa.String, default='base')
        agent_type = sa.Column(sa.String, default='openai')

    models = sys.modules[f'{plugin_package}.models']
    models.db = types.SimpleNamespace(Base=Base)
    models.db_tools = types.SimpleNamespace(AbstractBaseMixin=type('Mixin', (), {}))
    models.config = types.SimpleNamespace(POSTGRES_TENANT_SCHEMA='tenant')
    all_models = types.ModuleType(f'{plugin_package}.models.all')
    all_models.Application, all_models.ApplicationVersion = Application, ApplicationVersion
    sys.modules[all_models.__name__] = all_models
    sys.modules['pylon.core.tools'] = types.SimpleNamespace(
        log=types.SimpleNamespace(debug=print, info=print, warning=print, error=print, exception=print),
    )
    return Base, Application, ApplicationVersion


class _Tracer:
    """Records each started span as (name, attributes)."""

    def __init__(self, started):
        self.started = started

    def start_span(self, name, attributes=None):
        self.started.append((name, dict(attributes or {})))
        return types.SimpleNamespace(end=lambda: None)


def install_trace_span(span, started=None):
    """The current span is `span`; a span the code starts is appended to `started`."""
    otel = types.ModuleType('opentelemetry')
    tracer = _Tracer([] if started is None else started)
    otel.trace = types.SimpleNamespace(get_current_span=lambda: span, get_tracer=lambda _name: tracer)
    sys.modules['opentelemetry'] = otel
    sys.modules['opentelemetry.trace'] = otel.trace


def sqlite_engine():
    return sa.create_engine('sqlite://').execution_options(schema_translate_map={'tenant': None})


def create_schedule_table(load, base, engine):
    """Load the real schedule model and create every table; returns EntitySchedule."""
    load('models/enums/all.py', 'models.enums.all')
    entity_schedule = load('models/entity_schedule.py', 'models.entity_schedule').EntitySchedule
    entity_schedule.__table__.c.variables.server_default = None  # `::jsonb` is not SQLite
    base.metadata.create_all(engine)
    return entity_schedule


def add_agent_and_pipeline(session_factory, application, application_version):
    """Agent 1 with version 10 and pipeline 2 with version 11."""
    with session_factory() as session:
        session.add_all([application(id=1), application(id=2)])
        session.flush()
        session.add_all([
            application_version(id=10, application_id=1, agent_type='openai'),
            application_version(id=11, application_id=2, agent_type='pipeline'),
        ])
        session.commit()


def seed_schedule(session_factory, entity_schedule, created_by, **fields):
    """Store one agent schedule of `created_by`; returns its id."""
    values = {
        'entity_type': 'agent', 'application_id': 1, 'version_id': 10, 'name': 'Nightly',
        'cron': '0 9 * * *', 'timezone': 'Etc/UTC', 'prompt': 'Say hi', 'variables': [], 'enabled': True,
        'created_by': created_by, 'principal': {'kind': 'delegation', 'ref': created_by},
    }
    with session_factory() as session:
        row = entity_schedule(**{**values, **fields})
        session.add(row)
        session.commit()
        return row.id
