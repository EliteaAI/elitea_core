"""#6886: input guardrail checkpoints A (chat pre-persist) and B (start_task).

The detector is a twin of elitea-sdk's runtime/guardrails/input_detector.py; both run
the shared vectors file here, so a rule that drifts in either repo fails one suite.
Every planted value is a generated fake, and every test that plants one also asserts
it never reaches an audit span, an exception or a body.
"""
import importlib.util
import json
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[3]
PACKAGE_DIR = PLUGIN_ROOT / 'utils' / 'input_guardrail'
VECTORS = json.loads((PACKAGE_DIR / 'input_guardrail_vectors.json').read_text())
ALL_ON = {'enabled': True, 'categories': {'credentials': 'block', 'personal_data': 'block'}}
# Assembled at runtime so no scanner reads a token in this file
FAKE_GITHUB_TOKEN = 'gh' + 'p_' + 'Q7wZ' * 9
FAKE_EMAIL = 'jane.fake' + '@' + 'example.test'


class _Tracer:
    def __init__(self, spans):
        self.spans = spans

    def start_span(self, name, attributes):
        self.spans.append({'name': name, **attributes})
        return types.SimpleNamespace(end=lambda: None)


@pytest.fixture()
def guardrail(monkeypatch):
    """The input_guardrail package loaded under a throwaway parent, with pylon/tools stubbed."""
    log = types.SimpleNamespace(**{n: (lambda *a, **k: None) for n in ('debug', 'info', 'warning', 'error', 'exception')})
    pylon_tools = types.ModuleType('pylon.core.tools')
    pylon_tools.log = log
    config = {}
    tools = types.ModuleType('tools')
    tools.this = types.SimpleNamespace(descriptor=types.SimpleNamespace(config=config))
    spans = []
    otel = types.ModuleType('opentelemetry')
    otel.trace = types.SimpleNamespace(get_tracer=lambda _name: _Tracer(spans))
    for name, module in (
        ('pylon', types.ModuleType('pylon')), ('pylon.core', types.ModuleType('pylon.core')),
        ('pylon.core.tools', pylon_tools), ('tools', tools), ('opentelemetry', otel),
    ):
        monkeypatch.setitem(sys.modules, name, module)

    parent = types.ModuleType('ig_core_under_test')
    parent.__path__ = [str(PLUGIN_ROOT / 'utils')]
    monkeypatch.setitem(sys.modules, 'ig_core_under_test', parent)

    def load(name, path, package_path=None):
        spec = importlib.util.spec_from_file_location(
            name, path, submodule_search_locations=package_path,
        )
        module = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, module)
        spec.loader.exec_module(module)
        return module

    exceptions = load('ig_core_under_test.exceptions', PLUGIN_ROOT / 'utils' / 'exceptions.py')
    package = load(
        'ig_core_under_test.input_guardrail', PACKAGE_DIR / '__init__.py', [str(PACKAGE_DIR)],
    )
    package.config = config
    package.spans = spans
    package.exceptions = exceptions
    package.detector = sys.modules['ig_core_under_test.input_guardrail.input_detector']
    return package


def _enable(guardrail, **overrides):
    guardrail.config['input_guardrail'] = {'enabled': True, **overrides}


def _text(vector):
    return ''.join(vector['parts'])


def _assert_no_value(value, *carriers):
    for carrier in carriers:
        assert value not in json.dumps(carrier, default=str)


# --- twin detector ------------------------------------------------------------------


@pytest.mark.parametrize('vector', VECTORS['positive'], ids=lambda v: v['rule_id'])
def test_positive_vector_is_detected_by_its_rule(guardrail, vector):
    d = guardrail.detector
    result = d.inspect([d.Segment('current_turn', 0, _text(vector))], d.policy_from_dict(ALL_ON))

    assert [(x.category, x.rule_id) for x in result.detections] == [(vector['category'], vector['rule_id'])]


@pytest.mark.parametrize('vector', VECTORS['negative'], ids=lambda v: _text(v)[:40])
def test_negative_vector_is_not_detected(guardrail, vector):
    d = guardrail.detector
    result = d.inspect([d.Segment('current_turn', 0, _text(vector))], d.policy_from_dict(ALL_ON))

    assert result.detections == ()


def test_rule_set_matches_the_vectors_file(guardrail):
    assert {r.rule_id for r in guardrail.detector.RULES} == {v['rule_id'] for v in VECTORS['positive']}


def test_twin_matches_the_sdk_copy_when_checked_out_alongside():
    sdk_dir = PLUGIN_ROOT.parent / 'elitea-sdk'
    relative = 'elitea_sdk/runtime/guardrails/input_detector.py'
    copies = sorted([*sdk_dir.glob(relative), *sdk_dir.glob(f'*/{relative}')]) if sdk_dir.is_dir() else []
    if not copies:
        pytest.skip('elitea-sdk is not checked out next to this plugin')

    def body(path):
        # The module docstrings name each other and differ by design
        text = path.read_text()
        return text[text.index('"""', 3) + 3:]

    for sdk in copies:
        assert body(PACKAGE_DIR / 'input_detector.py') == body(sdk), sdk
        assert (PACKAGE_DIR / 'input_guardrail_vectors.json').read_text() == (sdk.parent / 'input_guardrail_vectors.json').read_text(), sdk


# --- checkpoint A ----------------------------------------------------------------------


def test_disabled_guardrail_returns_input_untouched(guardrail):
    text = f'token {FAKE_GITHUB_TOKEN}'

    assert guardrail.check_chat_input(text, 1, 7) == (text, None)
    assert guardrail.spans == []


def test_exempt_project_is_not_inspected(guardrail):
    _enable(guardrail, exempt_project_ids=[1])

    assert guardrail.check_chat_input(f'token {FAKE_GITHUB_TOKEN}', '1', 7)[1] is None


def test_chat_block_raises_without_the_value(guardrail):
    _enable(guardrail)

    with pytest.raises(guardrail.exceptions.InputGuardrailBlockedError) as caught:
        guardrail.check_chat_input(f'my token {FAKE_GITHUB_TOKEN}', 1, 7)

    error = caught.value
    assert error.rules == ['credentials.github_token']
    assert error.body()['error'] == 'input_guardrail_blocked'
    assert [s['audit.guardrail.action'] for s in guardrail.spans] == ['input:credentials.github_token:block']
    assert guardrail.spans[0]['audit.tool.name'] == 'current_turn'
    _assert_no_value(FAKE_GITHUB_TOKEN, str(error), error.body(), error.verdict(), guardrail.spans)


def test_chat_redact_returns_placeholder_and_verdict(guardrail):
    _enable(guardrail, categories={'credentials': 'redact'})

    text, verdict = guardrail.check_chat_input(f'my token {FAKE_GITHUB_TOKEN} thanks', 1, 7)

    assert text == 'my token [REDACTED:credentials] thanks'
    assert verdict['action'] == 'redact'
    assert set(verdict) == set(guardrail.VERDICT_KEYS)
    assert len(guardrail.spans) == 1
    _assert_no_value(FAKE_GITHUB_TOKEN, verdict, guardrail.spans)


def test_chat_redact_rewrites_text_blocks_and_counts_images(guardrail):
    _enable(guardrail, categories={'credentials': 'redact'})
    blocks = [{'type': 'text', 'text': FAKE_GITHUB_TOKEN}, {'type': 'image_url', 'image_url': {'url': 'x'}}]

    text, verdict = guardrail.check_chat_input(blocks, 1, 7)

    assert text[0]['text'] == '[REDACTED:credentials]'
    assert verdict['not_inspected'] == ['image_url']


@pytest.mark.parametrize('action', ['record', 'warn'])
def test_chat_record_and_warn_are_left_to_checkpoint_b_to_audit(guardrail, action):
    """Checkpoint B inspects the rebuilt payload again; auditing here too would double the row."""
    _enable(guardrail, categories={'credentials': action})
    text = f'my token {FAKE_GITHUB_TOKEN}'

    result, verdict = guardrail.check_chat_input(text, 1, 7)

    assert result == text
    assert verdict['action'] == action
    assert guardrail.spans == []


def test_chat_fail_closed_blocks_on_detector_error(guardrail, monkeypatch):
    _enable(guardrail, on_error='fail_closed')
    monkeypatch.setattr(guardrail, '_run', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('boom')))

    with pytest.raises(guardrail.exceptions.InputGuardrailBlockedError) as caught:
        guardrail.check_chat_input('anything', 1, 7)

    assert caught.value.categories == ['error']


def test_chat_fail_open_passes_on_detector_error(guardrail, monkeypatch):
    _enable(guardrail)
    monkeypatch.setattr(guardrail, '_run', lambda *a, **k: (_ for _ in ()).throw(RuntimeError('boom')))

    assert guardrail.check_chat_input('anything', 1, 7) == ('anything', None)


def test_resume_redacts_input_and_hitl_values_in_place(guardrail):
    _enable(guardrail, categories={'credentials': 'redact', 'personal_data': 'redact'})
    parsed = types.SimpleNamespace(
        user_input=f'retry with {FAKE_GITHUB_TOKEN}',
        hitl_value={'note': f'mail {FAKE_EMAIL}', 'n': 3},
        hitl_decisions=[{'value': FAKE_GITHUB_TOKEN}, {'value': None}],
    )

    verdict = guardrail.check_resume_input(parsed, 1, 7)

    assert parsed.user_input == 'retry with [REDACTED:credentials]'
    assert parsed.hitl_value == {'note': 'mail [REDACTED:personal_data]', 'n': 3}
    assert parsed.hitl_decisions == [{'value': '[REDACTED:credentials]'}, {'value': None}]
    assert verdict['detection_count'] == 3
    # On a HITL resume the input is the reviewer's text, so it is labelled hitl_edit
    assert verdict['segments'] == ['hitl_edit']
    _assert_no_value(FAKE_GITHUB_TOKEN, verdict, guardrail.spans)


def test_resume_input_mirroring_the_hitl_edit_is_audited_once_as_hitl_edit(guardrail):
    _enable(guardrail)
    edit = f'use {FAKE_GITHUB_TOKEN}'
    parsed = types.SimpleNamespace(user_input=edit, hitl_value=edit, hitl_decisions=None)

    with pytest.raises(guardrail.exceptions.InputGuardrailBlockedError) as caught:
        guardrail.check_resume_input(parsed, 1, 7)

    assert caught.value.segments == ['hitl_edit']
    assert [s['audit.tool.name'] for s in guardrail.spans] == ['hitl_edit']


def test_resume_mirrored_input_takes_the_redacted_hitl_value(guardrail):
    _enable(guardrail, categories={'credentials': 'redact'})
    edit = f'use {FAKE_GITHUB_TOKEN}'
    parsed = types.SimpleNamespace(user_input=edit, hitl_value=edit, hitl_decisions=None)

    guardrail.check_resume_input(parsed, 1, 7)

    assert parsed.user_input == parsed.hitl_value == 'use [REDACTED:credentials]'


def test_resume_without_hitl_labels_the_input_current_turn(guardrail):
    _enable(guardrail)
    parsed = types.SimpleNamespace(user_input=f'use {FAKE_GITHUB_TOKEN}', hitl_value=None, hitl_decisions=None)

    with pytest.raises(guardrail.exceptions.InputGuardrailBlockedError) as caught:
        guardrail.check_resume_input(parsed, 1, 7)

    assert caught.value.segments == ['current_turn']


def test_resume_block_in_a_hitl_edit_raises(guardrail):
    _enable(guardrail)
    parsed = types.SimpleNamespace(user_input=None, hitl_value=f'use {FAKE_GITHUB_TOKEN}', hitl_decisions=None)

    with pytest.raises(guardrail.exceptions.InputGuardrailBlockedError) as caught:
        guardrail.check_resume_input(parsed, 1, 7)

    assert caught.value.segments == ['hitl_edit']


# --- checkpoint B ----------------------------------------------------------------------


def _call(user_input, **payload):
    return {
        'kwargs': {'user_input': user_input, **payload},
        'meta': {'project_id': 1, 'user_id': 7, 'user_input_preview': user_input if isinstance(user_input, str) else None},
    }


def test_dispatch_block_raises_and_audits_once(guardrail):
    _enable(guardrail)

    with pytest.raises(guardrail.exceptions.InputGuardrailBlockedError):
        guardrail.apply_input_guardrail(_call(f'token {FAKE_GITHUB_TOKEN}'))

    assert len(guardrail.spans) == 1
    _assert_no_value(FAKE_GITHUB_TOKEN, guardrail.spans)


def test_dispatch_redacts_payload_preview_and_lists_hashes(guardrail):
    _enable(guardrail, categories={'credentials': 'redact'})
    call = _call(
        f'token {FAKE_GITHUB_TOKEN}',
        chat_history=[
            {'role': 'user', 'content': f'earlier {FAKE_GITHUB_TOKEN}'},
            {'role': 'assistant', 'content': f'echo {FAKE_GITHUB_TOKEN}'},
        ],
        instructions=f'use {FAKE_GITHUB_TOKEN}',
    )

    verdict = guardrail.apply_input_guardrail(call)

    payload, meta = call['kwargs'], call['meta']
    assert payload['user_input'] == 'token [REDACTED:credentials]'
    assert payload['chat_history'][0]['content'] == 'earlier [REDACTED:credentials]'
    # Assistant output is outside input-guardrail scope
    assert FAKE_GITHUB_TOKEN in payload['chat_history'][1]['content']
    assert payload['instructions'] == 'use [REDACTED:credentials]'
    assert meta['user_input_preview'] == 'token [REDACTED:credentials]'
    assert verdict['segments'] == ['current_turn', 'history', 'instructions']
    assert payload['input_guardrail'] == verdict
    hash_of = guardrail.detector.segment_hash
    assert hash_of('token [REDACTED:credentials]') in payload['input_guardrail_inspected']


def test_dispatch_masks_the_preview_even_when_the_action_is_record(guardrail):
    _enable(guardrail, categories={'credentials': 'record'})
    call = _call(f'token {FAKE_GITHUB_TOKEN}')

    guardrail.apply_input_guardrail(call)

    assert call['kwargs']['user_input'] == f'token {FAKE_GITHUB_TOKEN}'
    assert call['meta']['user_input_preview'] == 'token [REDACTED:credentials]'
    assert [s['audit.guardrail.action'] for s in guardrail.spans] == ['input:credentials.github_token:record']


def test_dispatch_skips_segments_already_inspected(guardrail):
    _enable(guardrail, categories={'credentials': 'record'})
    text = f'token {FAKE_GITHUB_TOKEN}'
    call = _call(text, input_guardrail_inspected=[guardrail.detector.segment_hash(text)])

    assert guardrail.apply_input_guardrail(call) is None
    assert guardrail.spans == []


def test_history_record_hit_is_not_reaudited_on_later_turns(guardrail):
    _enable(guardrail, categories={'credentials': 'record'})
    call = _call('hello', chat_history=[{'role': 'user', 'content': f'earlier {FAKE_GITHUB_TOKEN}'}])

    verdict = guardrail.apply_input_guardrail(call)

    assert verdict['segments'] == ['history']
    assert guardrail.spans == []


def test_platform_context_block_is_not_inspected(guardrail):
    _enable(guardrail)
    call = _call('<runtime_context>' + FAKE_GITHUB_TOKEN + '</runtime_context>')

    assert guardrail.apply_input_guardrail(call) is None


def test_unreadable_config_means_disabled(guardrail):
    guardrail.config['input_guardrail'] = 'not a mapping'

    assert guardrail.apply_input_guardrail(_call(f'token {FAKE_GITHUB_TOKEN}')) is None


def test_redact_text_replaces_every_category_not_off(guardrail):
    _enable(guardrail, categories={'credentials': 'block', 'personal_data': 'off'})

    assert guardrail.redact_text(f'{FAKE_GITHUB_TOKEN} {FAKE_EMAIL}') == f'[REDACTED:credentials] {FAKE_EMAIL}'


# --- stored turns -------------------------------------------------------------------


def _group(action, *contents):
    return types.SimpleNamespace(
        meta={'input_guardrail': {'action': action}} if action else {},
        message_items=[types.SimpleNamespace(content=c) for c in contents],
    )


def test_accepted_history_hashes_lists_record_and_warn_turns_only(guardrail):
    hashes = guardrail.accepted_history_hashes([
        _group('record', 'recorded'), _group('warn', 'warned'),
        _group('redact', 'redacted'), _group(None, 'stored while off'),
    ])

    assert hashes == sorted({guardrail.detector.segment_hash('recorded'), guardrail.detector.segment_hash('warned')})


def test_accepted_history_turn_does_not_block_a_later_turn(guardrail):
    _enable(guardrail, categories={'credentials': 'block'})
    earlier = f'earlier {FAKE_GITHUB_TOKEN}'
    call = _call('hello', chat_history=[{'role': 'user', 'content': earlier}],
                 input_guardrail_inspected=guardrail.accepted_history_hashes([_group('warn', earlier)]))

    assert guardrail.apply_input_guardrail(call) is None


def test_history_turn_without_a_verdict_is_still_inspected(guardrail):
    _enable(guardrail, categories={'credentials': 'block'})
    earlier = f'earlier {FAKE_GITHUB_TOKEN}'
    call = _call('hello', chat_history=[{'role': 'user', 'content': earlier}],
                 input_guardrail_inspected=guardrail.accepted_history_hashes([_group(None, earlier)]))

    with pytest.raises(guardrail.exceptions.InputGuardrailBlockedError):
        guardrail.apply_input_guardrail(call)


def test_redacted_input_masks_a_string_and_text_blocks_without_mutating(guardrail):
    _enable(guardrail, categories={'credentials': 'block'})
    blocks = [{'type': 'text', 'text': f'key {FAKE_GITHUB_TOKEN}'}, {'type': 'image_url', 'image_url': 'x'}]

    assert guardrail.redacted_input(f'key {FAKE_GITHUB_TOKEN}') == 'key [REDACTED:credentials]'
    redacted = guardrail.redacted_input(blocks)
    assert redacted[0]['text'] == 'key [REDACTED:credentials]'
    assert redacted[1] == blocks[1]
    assert FAKE_GITHUB_TOKEN in blocks[0]['text']
