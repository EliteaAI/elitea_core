"""Issue #6182 — placeholder regex false positives and contradictory summaries.

A real marketplace skill ("product-strategy-builder") was blocked with
"Description contains placeholder text" because PLACEHOLDER_RE matched the
substring TBD inside the acronym JTBD, and "Instructions contain placeholder
text" because the instructions legitimately describe placeholder sections of a
generated document. On top of that the merged report carried the AI's
"well-structured and publishable" summary next to status FAIL, because the
summary was taken from the AI verbatim without reconciling it with the merged
verdict. This suite pins the boundary-aware regex and demoted word check for
agents, the complete removal of deterministic placeholder findings for skills
(the AI's default rules take over draft detection), and the FAIL-summary
reconciliation.

Run via:
    python tests/run_tests.py integration/test_6182_placeholder_false_positive.py -v
"""

import importlib.util
import pathlib
import sys
import types

import pytest

PLUGIN_ROOT = pathlib.Path(__file__).resolve().parents[2]

REAL_SKILL_DESCRIPTION = (
    'Creates a self-contained interactive product strategy HTML document with '
    'research-backed market analysis, JTBD, personas, competitor mapping, '
    'roadmap, metrics, strategic options, and clearly separated primary vs '
    'AI-generated insights. Designed for building a structured strategy doc '
    'with sidebar navigation and source-dated evidence.'
)

REAL_SKILL_INSTRUCTIONS_EXCERPT = (
    'JTBD evolution table; placeholder custdev blocks (App Store, onboarding, '
    'tools, paywall); the section stays placeholder until the user provides '
    'primary data. Ask the user for target markets, segments and horizon, then '
    'build one self-contained HTML file with sidebar navigation and 14 sections.'
)


def _module(name, **attrs):
    mod = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(mod, key, value)
    return mod


def _load(module_name, filename):
    spec = importlib.util.spec_from_file_location(
        f'plugins.elitea_core.{module_name}', PLUGIN_ROOT / filename,
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope='module')
def pu():
    """publish_utils with its runtime/model deps stubbed."""
    noop = lambda *a, **k: None  # noqa: E731
    log = types.SimpleNamespace(info=noop, error=noop, warning=noop, debug=noop)

    sys.modules.setdefault('pylon', _module('pylon'))
    sys.modules.setdefault('pylon.core', _module('pylon.core'))
    sys.modules.setdefault('pylon.core.tools', _module('pylon.core.tools', log=log))
    if not hasattr(sys.modules['pylon.core.tools'], 'log'):
        sys.modules['pylon.core.tools'].log = log
    sys.modules.setdefault('tools', _module(
        'tools',
        db=types.SimpleNamespace(get_session=None),
        this=types.SimpleNamespace(module=None, descriptor=None),
        rpc_tools=types.SimpleNamespace(RpcMixin=object),
    ))

    col_model = lambda cls_name: type(cls_name, (), {})  # noqa: E731
    stubs = {
        'plugins.elitea_core.models.all': {
            'Application': col_model('Application'),
            'ApplicationVersion': col_model('ApplicationVersion'),
        },
        'plugins.elitea_core.models.elitea_tools': {
            'EliteATool': col_model('EliteATool'),
            'EntityToolMapping': col_model('EntityToolMapping'),
        },
        'plugins.elitea_core.models.enums.all': {
            'AgentTypes': types.SimpleNamespace(pipeline=types.SimpleNamespace(value='pipeline')),
            'NotificationEventTypes': object,
            'PublishStatus': types.SimpleNamespace(
                draft=types.SimpleNamespace(value='draft'),
                published=types.SimpleNamespace(value='published'),
                embedded=types.SimpleNamespace(value='embedded'),
            ),
            'SkillEntityTypes': types.SimpleNamespace(agent='agent'),
            'ToolEntityTypes': types.SimpleNamespace(agent='agent'),
        },
        'plugins.elitea_core.models.pd.application': {'ApplicationImportModel': object},
        'plugins.elitea_core.models.pd.version': {'ApplicationVersionForkCreateModel': object},
        'plugins.elitea_core.models.pd.publish': {'PublishAIResult': object},
        'plugins.elitea_core.models.skill': {
            'EntitySkillMapping': col_model('EntitySkillMapping'),
            'Skill': col_model('Skill'),
            'SkillVersion': col_model('SkillVersion'),
        },
        'plugins.elitea_core.utils.create_utils': {'create_application': noop, 'create_version': noop},
        'plugins.elitea_core.utils.utils': {'get_public_project_id': lambda: 1},
        'plugins.elitea_core.utils.category_utils': {
            'apply_category_to_tag_dicts': lambda tags, cat: tags,
            'is_valid_category': lambda name: True,
        },
        'plugins.elitea_core.utils.application_utils': {'build_skill_mappings_list': lambda ms: list(ms)},
        'plugins.elitea_core.utils.skill_export_import': {'build_skill_fork_payload': noop},
        'plugins.elitea_core.utils.skill_utils': {'attach_skill_to_public_copy': noop},
    }
    for modname, attrs in stubs.items():
        sys.modules[modname] = _module(modname, **attrs)

    sqla_orm = sys.modules.get('sqlalchemy.orm')
    if sqla_orm is not None and not hasattr(sqla_orm, 'selectinload'):
        sqla_orm.selectinload = lambda *a, **k: None
    tools_mod = sys.modules.get('tools')
    if tools_mod is not None:
        for attr, default in (
            ('db', types.SimpleNamespace(get_session=None)),
            ('this', types.SimpleNamespace(module=None, descriptor=None)),
            ('rpc_tools', types.SimpleNamespace(RpcMixin=object)),
        ):
            if not hasattr(tools_mod, attr):
                setattr(tools_mod, attr, default)

    return _load('utils.publish_utils', 'utils/publish_utils.py')


@pytest.fixture(scope='module')
def spu(pu):
    """skill_publish_utils — depends on the stubs pu installed."""
    version_pattern = r'^[a-zA-Z0-9._-]+$'
    stubs = {
        'plugins.elitea_core.models.pd.collection_base': {'TagBaseModel': object},
        'plugins.elitea_core.models.pd.publish': {
            'PublishAIResult': object, 'VERSION_NAME_PATTERN': version_pattern,
        },
        'plugins.elitea_core.models.pd.skill_publish': {'SkillPublishAIResult': object},
        'plugins.elitea_core.models.pd.skill_version': {'SkillVersionCreateModel': object},
        'plugins.elitea_core.utils.constants': {'DEFAULT_FALLBACK_CATEGORY': 'Other'},
        'plugins.elitea_core.utils.skill_category_utils': {
            'apply_skill_category_to_tag_dicts': lambda tags, cat: tags,
            'get_active_skill_categories': lambda: ['Other'],
            'validate_skill_category': lambda name: True,
        },
    }
    for modname, attrs in stubs.items():
        sys.modules[modname] = _module(modname, **attrs)
    sys.modules['plugins.elitea_core.models.all'].Tag = type('Tag', (), {})
    return _load('utils.skill_publish_utils', 'utils/skill_publish_utils.py')


class TestPlaceholderRegex:
    @pytest.mark.parametrize('text', [
        'JTBD evolution table with 5 JTBDs per persona',
        'Uncovered JTBD clusters flagged above 15%',
        'the jtbdanalysis module',
        'STODOR statue restoration guide',
        'read the loremaster notes',
    ], ids=['jtbd', 'jtbd-lower-context', 'jtbd-joined', 'todo-inside-word', 'lorem-inside-word'])
    def test_markers_inside_words_do_not_match(self, pu, text):
        assert pu.PLACEHOLDER_RE.search(text) is None

    @pytest.mark.parametrize('text', [
        'TODO: write the real instructions here',
        'Pricing section is TBD',
        'FIXME before shipping',
        'Lorem ipsum dolor sit amet',
        '[REPLACE] with your content',
        'insert here your steps',
        'todo: finish later',
    ], ids=['todo', 'tbd', 'fixme', 'lorem', 'replace', 'insert-here', 'todo-lower'])
    def test_real_stub_markers_still_match(self, pu, text):
        assert pu.PLACEHOLDER_RE.search(text) is not None

    def test_bare_placeholder_word_moved_to_word_regex(self, pu):
        assert pu.PLACEHOLDER_RE.search('placeholder custdev blocks') is None
        assert pu.PLACEHOLDER_WORD_RE.search('placeholder custdev blocks') is not None
        assert pu.PLACEHOLDER_WORD_RE.search('stays placeholder until data arrives') is not None
        assert pu.PLACEHOLDER_WORD_RE.search('two placeholders remain') is not None
        assert pu.PLACEHOLDER_WORD_RE.search('the placeholderish look') is None


class TestSkillCheckersHaveNoPlaceholderRule:
    """Draft detection is a semantic judgment, so for skills it lives entirely
    in the AI review: the deterministic checkers emit no placeholder findings
    at all — neither for the reported false-positive content nor for genuine
    TODO stubs (those are the AI's to flag via the draft-stub rule below)."""

    def _run(self, spu, description, instructions):
        return spu.run_skill_deterministic_checks(
            {'skill': {
                'name': 'product-strategy-builder',
                'description': description,
                'icon_meta': {'url': '/icons/x.png', 'name': 'x.png'},
                'tags': ['strategy'],
                'instructions': instructions,
            }},
            '1.0.0', 'Other',
        )

    def _placeholder_findings(self, res):
        return [
            f for bucket in ('critical_issues', 'warnings', 'recommendations')
            for f in res[bucket]
            if 'placeholder' in (f.get('issue') or f.get('suggestion') or '').lower()
        ]

    def test_reported_skill_content_is_clean(self, spu):
        res = self._run(spu, REAL_SKILL_DESCRIPTION, REAL_SKILL_INSTRUCTIONS_EXCERPT)
        assert self._placeholder_findings(res) == []
        assert res['critical_issues'] == []

    def test_even_genuine_todo_stub_yields_no_deterministic_finding(self, spu):
        res = self._run(
            spu,
            'TODO: describe this skill later. ' + 'x' * 40,
            'TODO: write the real instructions here. ' + 'x' * 120,
        )
        assert self._placeholder_findings(res) == []

    def test_default_ai_rules_take_over_draft_detection(self, spu):
        rules = spu._DEFAULT_SKILL_VALIDATION_RULES
        assert 'unfinished draft or template stub' in rules
        assert 'Do NOT flag format or length' in rules
        assert 'placeholder violations' not in rules


class TestSummaryReconciliation:
    DET_CRITICAL = {
        'critical_issues': [{
            'field': 'icon', 'issue': 'No custom icon set',
            'fix': 'Add a custom icon before publishing',
            'context': None, 'source': 'deterministic',
        }],
        'warnings': [], 'recommendations': [],
    }
    POSITIVE_AI = {
        'critical_issues': [], 'warnings': [], 'recommendations': [],
        'summary': 'The skill definition is clear, detailed, and actionable, '
                   'with no critical or warning issues found.',
    }

    def test_skill_fail_overrides_positive_ai_summary(self, spu):
        merged = spu.merge_skill_validation_results(self.DET_CRITICAL, self.POSITIVE_AI)
        assert merged['status'] == 'FAIL'
        assert merged['summary'] == (
            'Skill has 1 critical issue(s) that must be fixed before publishing.'
        )

    def test_skill_pass_keeps_ai_summary(self, spu):
        merged = spu.merge_skill_validation_results(
            {'critical_issues': [], 'warnings': [], 'recommendations': []},
            self.POSITIVE_AI,
        )
        assert merged['status'] == 'PASS'
        assert merged['summary'] == self.POSITIVE_AI['summary']

    def test_skill_warn_keeps_ai_summary(self, spu):
        merged = spu.merge_skill_validation_results(
            {'critical_issues': [],
             'warnings': [{'field': 'name', 'issue': 'x', 'fix': 'y',
                           'context': None, 'source': 'deterministic'}],
             'recommendations': []},
            self.POSITIVE_AI,
        )
        assert merged['status'] == 'WARN'
        assert merged['summary'] == self.POSITIVE_AI['summary']

    def test_agent_fail_overrides_positive_ai_summary(self, pu):
        merged = pu.merge_validation_results(self.DET_CRITICAL, self.POSITIVE_AI)
        assert merged['status'] == 'FAIL'
        assert merged['summary'] == (
            'Agent has 1 critical issue(s) that must be fixed before publishing.'
        )

    def test_agent_pass_keeps_ai_summary(self, pu):
        merged = pu.merge_validation_results(
            {'critical_issues': [], 'warnings': [], 'recommendations': []},
            self.POSITIVE_AI,
        )
        assert merged['status'] == 'PASS'
        assert merged['summary'] == self.POSITIVE_AI['summary']
