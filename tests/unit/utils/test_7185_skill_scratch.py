"""#7185: a conversation's skill-script scratch is removed with the conversation and swept after it idles.

Runs the real ``utils/skill_scratch.py`` over a fake storage client; the conversation activity lookup is injected.
"""
import datetime
import pathlib
import sys
import types
import uuid
from unittest import mock

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from fixtures.skill_file_env import load, stub_everything_else  # noqa: E402  pylint: disable=C0413

UTC = datetime.timezone.utc
NOW = datetime.datetime(2026, 11, 4, 12, 0, tzinfo=UTC)
BUCKET = 'attachments'
ACTIVE = 'aaaaaaaa-1111-4111-8111-aaaaaaaaaaaa'
IDLE = 'bbbbbbbb-2222-4222-8222-bbbbbbbbbbbb'
GONE = 'cccccccc-3333-4333-8333-cccccccccccc'
CHILD_THREAD = 'cccccccc-3333-4333-8333-ccccccccccccdispatch_ab12MyAgentcall_1'


class FakeStorage:
    """An S3-style client: paged, prefix-scoped listing and per-object deletes."""

    def __init__(self, names, page_size=2, buckets=(BUCKET,)):
        self.names = list(names)
        self.removed = []
        self.listings = []
        self.page_size = page_size
        self.buckets = list(buckets)
        self.s3_client = self

    def list_bucket(self):
        return list(self.buckets)

    def format_bucket_name(self, bucket):
        return f'p--7.{bucket}'

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        assert Bucket == f'p--7.{BUCKET}'
        self.listings.append(Prefix)
        matching = [name for name in self.names if name.startswith(Prefix)]
        start = int(ContinuationToken or 0)
        page = matching[start:start + self.page_size]
        response = {'Contents': [{'Key': name} for name in page]}
        if start + self.page_size < len(matching):
            response['NextContinuationToken'] = str(start + self.page_size)
        return response

    def remove_file(self, bucket, name):
        assert bucket == BUCKET
        self.removed.append(name)
        self.names.remove(name)


class FakeLibcloudStorage:
    """The libcloud and filesystem engines: a whole-bucket listing, no prefix filter, and None for any other attribute."""

    def __init__(self, names, buckets=(BUCKET,)):
        self.names = list(names)
        self.removed = []
        self.listed = 0
        self.buckets = list(buckets)

    def __getattr__(self, name):
        return None

    def list_bucket(self):
        return list(self.buckets)

    def list_files(self, bucket, next_continuation_token=None):
        assert bucket == BUCKET
        self.listed += 1
        return [{'name': name, 'size': 1, 'modified': ''} for name in self.names]

    def remove_file(self, bucket, name):
        assert bucket == BUCKET
        self.removed.append(name)
        self.names.remove(name)


@pytest.fixture
def scratch(monkeypatch, isolated_sys_modules):
    tools = types.ModuleType('tools')
    tools.MinioClient = mock.MagicMock(name='MinioClient')
    tools.db = mock.MagicMock(name='db')
    monkeypatch.setitem(sys.modules, 'tools', tools)
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = mock.MagicMock(name='log')
    monkeypatch.setitem(sys.modules, 'pylon', types.ModuleType('pylon'))
    monkeypatch.setitem(sys.modules, 'pylon.core', types.ModuleType('pylon.core'))
    monkeypatch.setitem(sys.modules, 'pylon.core.tools', pylon_tools)
    sqlalchemy = types.ModuleType('sqlalchemy')
    sqlalchemy.func = mock.MagicMock(name='func')
    monkeypatch.setitem(sys.modules, 'sqlalchemy', sqlalchemy)
    for name in [name for name in sys.modules if name == 'plugins' or name.startswith('plugins.elitea_core')]:
        monkeypatch.delitem(sys.modules, name)
    for name in ('plugins', 'plugins.elitea_core', 'plugins.elitea_core.utils', 'plugins.elitea_core.models'):
        package = types.ModuleType(name)
        package.__path__ = []
        monkeypatch.setitem(sys.modules, name, package)
    stub_everything_else(monkeypatch)
    module = load(monkeypatch, 'utils/skill_scratch.py', 'plugins.elitea_core.utils.skill_scratch')
    module.bucket_lookups = []
    monkeypatch.setattr(module, 'get_default_attachment_bucket', lambda project_id: module.bucket_lookups.append(project_id) or BUCKET)
    return module


def _storage():
    return FakeStorage([
        f'_skill_scratch/{ACTIVE}/scratch.zip', f'_skill_scratch/{ACTIVE}/outputs/c1.txt',
        f'_skill_scratch/{IDLE}/scratch.zip', f'_skill_scratch/{GONE}/scratch.zip',
        f'{ACTIVE}/deck.pptx', f'{ACTIVE}/c1/deck-2.pptx', f'{IDLE}/notes.txt',
    ])


def test_the_prefix_is_the_runtimes_hidden_folder_for_the_conversation(scratch):
    assert scratch.scratch_prefix(ACTIVE) == f'_skill_scratch/{ACTIVE}/'
    assert scratch.SCRATCH_ROOT_PREFIX == '_skill_scratch'


def test_deleting_a_conversation_lists_only_its_own_prefix_and_removes_only_its_scratch(scratch):
    storage = _storage()

    removed = scratch.remove_conversation_scratch(7, ACTIVE, mc=storage)

    assert removed == 2
    assert storage.listings == [f'_skill_scratch/{ACTIVE}/']
    assert storage.removed == [f'_skill_scratch/{ACTIVE}/scratch.zip', f'_skill_scratch/{ACTIVE}/outputs/c1.txt']
    assert f'{ACTIVE}/deck.pptx' in storage.names and f'{ACTIVE}/c1/deck-2.pptx' in storage.names
    assert f'_skill_scratch/{IDLE}/scratch.zip' in storage.names


def test_a_listing_is_followed_through_its_pages(scratch):
    storage = FakeStorage([f'_skill_scratch/{ACTIVE}/{name}' for name in ('a', 'b', 'c', 'd', 'e')], page_size=2)

    assert scratch.remove_conversation_scratch(7, ACTIVE, mc=storage) == 5
    assert storage.names == []


def test_a_project_without_the_attachment_bucket_has_nothing_to_remove(scratch):
    storage = FakeStorage([f'_skill_scratch/{ACTIVE}/scratch.zip'], buckets=())

    assert scratch.remove_conversation_scratch(7, ACTIVE, mc=storage) == 0
    assert storage.listings == [] and storage.removed == []


def test_on_the_libcloud_and_filesystem_engines_the_bucket_listing_is_filtered_by_prefix(scratch):
    storage = FakeLibcloudStorage([
        f'_skill_scratch/{ACTIVE}/scratch.zip', f'_skill_scratch/{IDLE}/scratch.zip', f'{ACTIVE}/deck.pptx',
    ])

    removed = scratch.remove_conversation_scratch(7, ACTIVE, mc=storage)

    assert removed == 1 and storage.listed == 1
    assert storage.removed == [f'_skill_scratch/{ACTIVE}/scratch.zip']
    swept = scratch.sweep_idle_scratch(
        7, idle_days=30, now=NOW, mc=storage, last_activity_of=lambda project_id, uuids: {IDLE: None},
    )
    assert swept == {'removed': 1, 'kept': 0}
    assert storage.names == [f'{ACTIVE}/deck.pptx']


def test_a_bulk_delete_lists_the_scratch_once_and_removes_per_conversation(scratch):
    storage = FakeLibcloudStorage([
        f'_skill_scratch/{ACTIVE}/scratch.zip', f'_skill_scratch/{IDLE}/scratch.zip', f'_skill_scratch/{IDLE}/outputs/c.txt',
        f'{ACTIVE}/deck.pptx',
    ])

    index = scratch.index_conversation_scratch(storage, BUCKET)
    removed = [scratch.remove_indexed_scratch(storage, BUCKET, index, uuid) for uuid in (ACTIVE, IDLE, GONE)]

    assert removed == [1, 2, 0] and storage.listed == 1
    assert storage.names == [f'{ACTIVE}/deck.pptx'] and index == {}
    assert scratch.bucket_lookups == []


def test_a_storage_error_is_not_mistaken_for_an_empty_scratch(scratch):
    storage = FakeStorage([f'_skill_scratch/{ACTIVE}/scratch.zip'])
    storage.list_objects_v2 = mock.Mock(side_effect=RuntimeError('throttled'))

    with pytest.raises(RuntimeError):
        scratch.remove_conversation_scratch(7, ACTIVE, mc=storage)


def test_the_sweep_removes_idle_and_missing_conversations_and_keeps_active_ones(scratch):
    storage = _storage()
    asked = []

    def activity(project_id, uuids):
        asked.append((project_id, uuids))
        return {ACTIVE: NOW - datetime.timedelta(days=29), IDLE: NOW - datetime.timedelta(days=31), GONE: None}

    swept = scratch.sweep_idle_scratch(7, idle_days=30, now=NOW, mc=storage, last_activity_of=activity)

    assert swept == {'removed': 2, 'kept': 1}
    assert asked == [(7, [ACTIVE, IDLE, GONE])]
    assert set(storage.listings) == {'_skill_scratch/'}
    assert sorted(storage.removed) == sorted([f'_skill_scratch/{GONE}/scratch.zip', f'_skill_scratch/{IDLE}/scratch.zip'])
    assert f'{IDLE}/notes.txt' in storage.names


def test_a_scratch_keyed_by_something_other_than_a_conversation_is_removed_without_a_database_lookup(scratch):
    storage = FakeStorage([f'_skill_scratch/{CHILD_THREAD}/scratch.zip', f'_skill_scratch/{ACTIVE}/scratch.zip'])
    asked = []

    def activity(project_id, uuids):
        asked.append(uuids)
        return {ACTIVE: NOW}

    swept = scratch.sweep_idle_scratch(7, now=NOW, mc=storage, last_activity_of=activity)

    assert swept == {'removed': 1, 'kept': 1}
    assert asked == [[ACTIVE]]
    assert storage.removed == [f'_skill_scratch/{CHILD_THREAD}/scratch.zip']


def test_a_naive_last_activity_is_read_as_utc(scratch):
    storage = FakeStorage([f'_skill_scratch/{ACTIVE}/scratch.zip'])

    swept = scratch.sweep_idle_scratch(
        7, idle_days=30, now=NOW, mc=storage,
        last_activity_of=lambda project_id, uuids: {ACTIVE: datetime.datetime(2026, 11, 3, 12, 0)},
    )

    assert swept == {'removed': 0, 'kept': 1}
    assert storage.removed == []


def test_a_project_with_no_scratch_asks_nothing_of_the_database(scratch):
    storage = FakeStorage([f'{ACTIVE}/deck.pptx'])

    swept = scratch.sweep_idle_scratch(7, now=NOW, mc=storage, last_activity_of=lambda *args: pytest.fail('asked'))

    assert swept == {'removed': 0, 'kept': 0}


def _conversation_lookup(scratch, monkeypatch, conversation, denied=None):
    session = mock.MagicMock(name='session')
    session.query.return_value.filter.return_value.first.return_value = conversation
    monkeypatch.setattr(scratch.db, 'get_session', mock.MagicMock(name='get_session'))
    scratch.db.get_session.return_value.__enter__.return_value = session
    checked = []
    monkeypatch.setattr(scratch, 'check_post_access', lambda project_id, found, user_id: checked.append((project_id, user_id)) or denied)
    return session, checked


def test_a_run_in_a_conversation_its_user_may_post_in_keeps_its_files_under_that_conversation(scratch, monkeypatch):
    _session, checked = _conversation_lookup(scratch, monkeypatch, types.SimpleNamespace(uuid=uuid.UUID(ACTIVE)))

    assert scratch.skill_scratch_for_run(7, ACTIVE, 9) == {'conversation_uuid': ACTIVE, 'bucket': BUCKET}
    assert checked == [(7, 9)]


@pytest.mark.parametrize('conversation, denied', [
    (None, None),
    (types.SimpleNamespace(uuid=uuid.UUID(ACTIVE)), ({'error': 'Conversation not found'}, 404)),
])
def test_a_conversation_missing_from_the_project_or_closed_to_the_user_gets_a_scratch_of_its_own(scratch, monkeypatch, conversation, denied):
    _conversation_lookup(scratch, monkeypatch, conversation, denied)

    first = scratch.skill_scratch_for_run(7, ACTIVE, 9)['conversation_uuid']
    second = scratch.skill_scratch_for_run(7, ACTIVE, 9)['conversation_uuid']

    assert len({first, second, ACTIVE}) == 3
    assert str(uuid.UUID(first)) == first


@pytest.mark.parametrize('key', [None, '', 'some-stream', '_skill_scratch', f'{ACTIVE}/../x', 42])
def test_a_key_that_is_not_a_uuid_is_never_looked_up(scratch, monkeypatch, key):
    session, _checked = _conversation_lookup(scratch, monkeypatch, types.SimpleNamespace(uuid=uuid.UUID(ACTIVE)))

    run_scratch = scratch.skill_scratch_for_run(7, key, 9)

    assert run_scratch['conversation_uuid'] != ACTIVE
    assert str(uuid.UUID(run_scratch['conversation_uuid'])) == run_scratch['conversation_uuid']
    assert session.query.call_count == 0


def test_a_conversation_written_in_another_form_is_keyed_by_its_canonical_uuid(scratch, monkeypatch):
    _conversation_lookup(scratch, monkeypatch, types.SimpleNamespace(uuid=uuid.UUID(ACTIVE)))

    assert scratch.skill_scratch_for_run(7, ACTIVE.upper().replace('-', ''), 9)['conversation_uuid'] == ACTIVE


def test_a_failed_verification_gives_the_run_a_scratch_of_its_own_instead_of_failing_it(scratch, monkeypatch):
    _conversation_lookup(scratch, monkeypatch, types.SimpleNamespace(uuid=uuid.UUID(ACTIVE)))
    monkeypatch.setattr(scratch, 'check_post_access', mock.MagicMock(side_effect=TimeoutError('admin check')))

    assert scratch.skill_scratch_for_run(7, ACTIVE, 9)['conversation_uuid'] != ACTIVE


class RecordingStorage:
    """A storage client over several buckets that records which bucket each call used."""

    def __init__(self, objects):
        self.objects = {bucket: list(names) for bucket, names in objects.items()}
        self.removed = []

    def __getattr__(self, name):
        return None

    def list_bucket(self):
        return list(self.objects)

    def list_files(self, bucket, next_continuation_token=None):
        return [{'name': name} for name in self.objects[bucket]]

    def remove_file(self, bucket, name):
        self.removed.append((bucket, name))
        self.objects[bucket].remove(name)


def test_with_a_non_default_bucket_the_run_writes_where_delete_and_sweep_remove(scratch, monkeypatch):
    monkeypatch.setattr(scratch, 'get_default_attachment_bucket', lambda project_id: 'team-files')
    _conversation_lookup(scratch, monkeypatch, types.SimpleNamespace(uuid=uuid.UUID(ACTIVE)))
    storage = RecordingStorage({
        'team-files': [f'_skill_scratch/{ACTIVE}/scratch.zip', f'_skill_scratch/{IDLE}/scratch.zip'],
        'attachments': [f'_skill_scratch/{ACTIVE}/scratch.zip'],
    })

    written_to = scratch.skill_scratch_for_run(7, ACTIVE, 9)['bucket']
    scratch.remove_conversation_scratch(7, ACTIVE, mc=storage)
    scratch.sweep_idle_scratch(7, now=NOW, mc=storage, last_activity_of=lambda project_id, uuids: {})

    assert written_to == 'team-files'
    assert storage.removed == [('team-files', f'_skill_scratch/{ACTIVE}/scratch.zip'), ('team-files', f'_skill_scratch/{IDLE}/scratch.zip')]
