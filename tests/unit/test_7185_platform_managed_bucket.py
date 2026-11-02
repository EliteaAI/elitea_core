"""#7185: a chat attachment path naming the ``skill-files`` bucket reaches neither storage nor a message, and the
project setting ``default_attachment_bucket`` cannot point chat uploads into it.

Skill file content is only served through the skill file endpoints, which check the skill's folder access. The chat
attachment paths take a bucket-qualified ``/{bucket}/{name}``, so each of them is driven here with the real
``utils/attachments.py`` and ``utils/platform_buckets.py``: the attachment DELETE endpoint, the chat send RPC
(``process_attachment_message_items``) and the stream end event (files the run reports as modified). The other
plugin modules are stand-ins.
"""
import importlib.util
import pathlib
import sys
import types
from unittest import mock

import pytest
from werkzeug.datastructures import MultiDict

TESTS_DIR = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.identity import fresh_sqlalchemy  # noqa: E402  pylint: disable=C0413
from fixtures.skill_file_env import AnyModule, PLUGIN_ROOT, StubEverythingElse  # noqa: E402  pylint: disable=C0413
from unit.events.test_7011_stream_end_schedule_step import Session, make_world  # noqa: E402,F401  pylint: disable=C0413

PKG = 'plugins.elitea_core'
MANAGED_PATHS = ['/skill-files/' + 'a' * 64, '/p--7.skill-files/' + 'a' * 64, '/SKILL-FILES/x', '/p--9.skill-files/x']


class StubEveryOtherPackage(StubEverythingElse):
    """Like ``StubEverythingElse``, and each stand-in is also a package, so ``..models.pd.x`` resolves too."""

    def create_module(self, spec):
        module = super().create_module(spec)
        module.__path__ = []
        return module


def _passthrough(*args, **kwargs):
    return lambda func: func


def _load(monkeypatch, relative_path):
    name = f'{PKG}.' + relative_path[:-3].replace('/', '.')
    spec = importlib.util.spec_from_file_location(name, PLUGIN_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


class AttachmentItem:
    __mapper_args__ = {'polymorphic_identity': 'attachment_message'}

    def __init__(self, **fields):
        self.fields = fields


class Processors:
    """Stands in for ``FileToAIProcessorCollector``: records each file a processor was asked to read."""

    def __init__(self):
        self.read = []

    def __call__(self):
        return self

    def process_file(self, bucket_name, filename, **kwargs):
        self.read.append((bucket_name, filename))
        return {'type': 'document', 'content': 'text'}


@pytest.fixture
def plugin(monkeypatch, isolated_sys_modules):
    fresh_sqlalchemy()
    for name in [name for name in sys.modules if name == 'plugins' or name.startswith(PKG)]:
        monkeypatch.delitem(sys.modules, name)
    for name in ('plugins', PKG, f'{PKG}.utils', f'{PKG}.api', f'{PKG}.api.v2', f'{PKG}.rpc', f'{PKG}.events'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    monkeypatch.setattr(sys, 'meta_path', [*sys.meta_path, StubEveryOtherPackage()])
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = mock.MagicMock()
    pylon_tools.web = types.SimpleNamespace(rpc=_passthrough, event=_passthrough, method=_passthrough)
    for name, module in (('pylon', AnyModule('pylon')), ('pylon.core', AnyModule('pylon.core')),
                         ('pylon.core.tools', pylon_tools), ('redis', AnyModule('redis'))):
        monkeypatch.setitem(sys.modules, name, module)
    tools = AnyModule('tools')
    tools.api_tools = types.SimpleNamespace(
        APIModeHandler=object, APIBase=object, endpoint_metrics=lambda func: func, with_modes=lambda params: params)
    tools.auth = types.SimpleNamespace(decorators=types.SimpleNamespace(check_api=_passthrough))
    tools.register_openapi = _passthrough
    tools.config = types.SimpleNamespace(ADMINISTRATION_MODE='administration', DEFAULT_MODE='default')
    monkeypatch.setitem(sys.modules, 'tools', tools)

    _load(monkeypatch, 'utils/platform_buckets.py')
    attachments = _load(monkeypatch, 'utils/attachments.py')
    processors = Processors()
    monkeypatch.setattr(attachments, 'FileToAIProcessorCollector', processors)
    monkeypatch.setattr(attachments, 'AttachmentMessageItem', AttachmentItem)
    return types.SimpleNamespace(attachments=attachments, processors=processors, tools=tools)


class TestTheBucketCheck:
    @pytest.mark.parametrize('bucket', ['skill-files', 'p--7.skill-files', 'p--123.skill-files', ' Skill-Files'])
    def test_the_skill_files_bucket_is_managed_however_it_is_spelled(self, plugin, bucket):
        assert sys.modules[f'{PKG}.utils.platform_buckets'].is_platform_managed_bucket(bucket)

    @pytest.mark.parametrize('bucket', ['attachments', 'p--7.attachments', 'skill-files-2', 'my-skill-files', '', None])
    def test_other_buckets_are_not(self, plugin, bucket):
        assert not sys.modules[f'{PKG}.utils.platform_buckets'].is_platform_managed_bucket(bucket)


class TestAttachmentProcessing:
    @pytest.mark.parametrize('filepath', MANAGED_PATHS)
    def test_a_managed_bucket_path_is_refused_before_any_processor_reads_it(self, plugin, filepath):
        with pytest.raises(RuntimeError):
            plugin.attachments.process_single_attachment_file(
                session=None, project_id=7, msg_group=mock.MagicMock(), filepath=filepath, order_index=1,
            )
        assert plugin.processors.read == []

    def test_an_attachment_bucket_path_is_still_read(self, plugin):
        plugin.attachments.process_single_attachment_file(
            session=None, project_id=7, msg_group=mock.MagicMock(), filepath='/attachments/a.txt', order_index=1,
        )
        assert plugin.processors.read == [('attachments', 'a.txt')]


class TestChatSend:
    @pytest.fixture
    def chat(self, plugin, monkeypatch):
        return _load(monkeypatch, 'rpc/chat_all.py')

    @pytest.mark.parametrize('filepath', MANAGED_PATHS)
    def test_a_message_naming_a_managed_bucket_attaches_nothing(self, plugin, chat, filepath):
        session = mock.MagicMock()

        with pytest.raises(RuntimeError, match='All attachments failed'):
            chat.process_attachment_message_items(
                session, 7, mock.MagicMock(), [types.SimpleNamespace(filepath=filepath)], pipeline_mode=True,
            )
        assert plugin.processors.read == [] and session.add.call_count == 0

    def test_a_message_with_an_attachment_bucket_path_attaches_it(self, plugin, chat):
        session = mock.MagicMock()

        chat.process_attachment_message_items(
            session, 7, mock.MagicMock(), [types.SimpleNamespace(filepath='/attachments/a.txt')], pipeline_mode=True,
        )
        assert plugin.processors.read == [('attachments', 'a.txt')] and session.add.call_count == 1


class TestStreamEnd:
    @pytest.fixture
    def stream(self, plugin, make_world, monkeypatch):
        added = []
        monkeypatch.setattr(Session, 'add', lambda session, item: added.append(item))
        attached = lambda: [item for item in added if isinstance(item, AttachmentItem)]
        world = make_world({})
        events = sys.modules[f'{PKG}.events.message_stream']
        monkeypatch.setattr(events, 'process_single_attachment_file',
                            plugin.attachments.process_single_attachment_file)
        return types.SimpleNamespace(world=world, attached=attached)

    @pytest.mark.parametrize('filepath', MANAGED_PATHS)
    def test_a_run_reporting_a_managed_bucket_file_attaches_nothing(self, plugin, stream, filepath):
        stream.world.end({'files_modified': [{'filepath': filepath}]})

        assert plugin.processors.read == [] and stream.attached() == []

    def test_a_run_reporting_an_attachment_bucket_file_attaches_it(self, plugin, stream):
        stream.world.end({'files_modified': [{'filepath': '/attachments/a.txt'}]})

        assert plugin.processors.read == [('attachments', 'a.txt')] and len(stream.attached()) == 1


class TestAttachmentDelete:
    @pytest.fixture
    def endpoint(self, plugin, monkeypatch):
        flask = types.ModuleType('flask')
        flask.request = types.SimpleNamespace(args=MultiDict())
        monkeypatch.setitem(sys.modules, 'flask', flask)
        storage = mock.MagicMock()
        plugin.tools.MinioClient = types.SimpleNamespace(from_project_id=lambda project_id: storage)
        plugin.tools.db = mock.MagicMock()
        api = _load(monkeypatch, 'api/v2/attachments.py')
        return types.SimpleNamespace(api=api, request=flask.request, storage=storage, db=plugin.tools.db)

    @pytest.mark.parametrize('keep_in_storage', ['false', 'true'])
    @pytest.mark.parametrize('filepath', MANAGED_PATHS)
    def test_a_managed_bucket_path_is_a_400_before_storage_or_the_database(self, endpoint, filepath, keep_in_storage):
        endpoint.request.args = MultiDict([('filename', filepath), ('keep_in_storage', keep_in_storage)])

        response, status = endpoint.api.PromptLibAPI().delete(project_id=7, conversation_id=1)

        assert status == 400 and 'managed by the platform' in response['error']
        assert endpoint.storage.mock_calls == [] and endpoint.db.mock_calls == []

    def test_a_managed_path_after_an_attachment_path_is_refused_too(self, endpoint):
        endpoint.request.args = MultiDict([('filename', '/attachments/a.txt'), ('filename', MANAGED_PATHS[0])])

        response, status = endpoint.api.PromptLibAPI().delete(project_id=7, conversation_id=1)

        assert status == 400 and endpoint.storage.mock_calls == []


class TestDefaultAttachmentBucket:
    @pytest.fixture
    def setting(self, plugin, monkeypatch):
        internal_tools = _load(monkeypatch, 'utils/internal_tools.py')
        secrets = {}
        monkeypatch.setattr(internal_tools, 'VaultClient',
                            lambda project_id: types.SimpleNamespace(get_all_secrets=lambda: secrets))
        return types.SimpleNamespace(read=lambda: internal_tools.get_default_attachment_bucket(7), secrets=secrets)

    @pytest.mark.parametrize('bucket', ['skill-files', 'p--7.skill-files', 'Skill-Files'])
    def test_a_setting_naming_the_skill_files_bucket_falls_back_to_attachments(self, setting, bucket):
        setting.secrets['default_attachment_bucket'] = bucket

        assert setting.read() == 'attachments'

    def test_any_other_setting_is_kept(self, setting):
        setting.secrets['default_attachment_bucket'] = 'team-uploads'

        assert setting.read() == 'team-uploads'
