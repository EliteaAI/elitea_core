"""Unit tests for indexing_report.py — notification prose for indexing runs."""
import json
import pathlib
import sys

import pytest

TESTS_DIR = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(TESTS_DIR))

from fixtures.helpers import load_module_with_stubs


@pytest.fixture(scope='module')
def indexing_report(utils_path):
    return load_module_with_stubs(
        utils_path / 'indexing_report.py', 'elitea_core_indexing_report'
    )


def make_report(**totals_overrides):
    totals = {
        'indexed': 179, 'skipped': 0, 'not_indexed': 0, 'failed': 0,
        'unchanged': 0, 'dependent_not_indexed': 0, 'total': 179,
    }
    totals.update(totals_overrides)
    return {
        'version': 1,
        'status': 'ok',
        'operation': 'reindex',
        'item_labels': {'singular': 'page', 'plural': 'pages'},
        'dependent_labels': {'singular': 'attachment', 'plural': 'attachments'},
        'totals': totals,
    }


class TestParseIndexingReport:
    def test_a_dict_passes_through(self, indexing_report):
        assert indexing_report.parse_indexing_report({'totals': {}}) == {'totals': {}}

    def test_a_json_string_is_decoded(self, indexing_report):
        assert indexing_report.parse_indexing_report('{"totals": {}}') == {'totals': {}}

    @pytest.mark.parametrize('value', [None, '', '   ', 'not json', '[]', '"text"', 42])
    def test_anything_else_is_treated_as_absent(self, indexing_report, value):
        assert indexing_report.parse_indexing_report(value) is None


class TestSummarizeIndexingReport:
    def test_indexed_only(self, indexing_report):
        assert indexing_report.summarize_indexing_report(make_report()) == '179 pages indexed'

    def test_singular_noun_for_a_single_item(self, indexing_report):
        summary = indexing_report.summarize_indexing_report(make_report(indexed=1, total=1))

        assert summary == '1 page indexed'

    def test_every_non_zero_category_appears(self, indexing_report):
        report = make_report(skipped=12, not_indexed=4, failed=1, total=196)

        summary = indexing_report.summarize_indexing_report(report)

        assert summary == '179 pages indexed, 12 skipped, 4 not indexed, 1 failed'

    def test_zero_categories_are_omitted(self, indexing_report):
        summary = indexing_report.summarize_indexing_report(make_report(skipped=12, total=191))

        assert summary == '179 pages indexed, 12 skipped'

    def test_dependent_items_are_parenthesised_never_summed(self, indexing_report):
        report = make_report(dependent_not_indexed=4)

        summary = indexing_report.summarize_indexing_report(report)

        assert summary == '179 pages indexed (4 attachments not indexed)'

    def test_unchanged_items_are_not_reported_as_skipped(self, indexing_report):
        """An incremental reindex must not describe untouched documents as skipped —
        the chip beside this counts them as indexed."""
        report = make_report(indexed=5, skipped=1, unchanged=195, total=201)

        summary = indexing_report.summarize_indexing_report(report)

        assert summary == '5 pages indexed, 1 skipped, 195 unchanged'

    def test_unchanged_alone_needs_no_skipped_clause(self, indexing_report):
        report = make_report(indexed=5, skipped=0, unchanged=195, total=200)

        assert indexing_report.summarize_indexing_report(report) == '5 pages indexed, 195 unchanged'

    def test_nothing_changed_reads_as_up_to_date(self, indexing_report):
        report = make_report(indexed=0, unchanged=196, skipped=0, total=196)

        summary = indexing_report.summarize_indexing_report(report)

        assert summary == 'Up to date — 196 pages unchanged'

    def test_a_missing_report_has_no_summary(self, indexing_report):
        assert indexing_report.summarize_indexing_report(None) is None


class TestBuildIndexNotificationMessage:
    def test_failure_wins_over_everything(self, indexing_report):
        message = indexing_report.build_index_notification_message(
            {'index_name': 'docs', 'error': 'boom', 'report': make_report()}
        )

        assert message == 'Index [docs]() is failed.'

    def test_first_index_message(self, indexing_report):
        message = indexing_report.build_index_notification_message(
            {'index_name': 'docs', 'report': make_report()}
        )

        assert message == 'Index [docs]() is successfully created. 179 pages indexed.'

    def test_reindex_message(self, indexing_report):
        message = indexing_report.build_index_notification_message(
            {'index_name': 'docs', 'reindex': True, 'report': make_report(skipped=12, total=191)}
        )

        assert message == (
            'Index [docs]() is successfully reindexed. 179 pages indexed, 12 skipped.'
        )

    def test_scheduled_reindex_is_named_as_such(self, indexing_report):
        message = indexing_report.build_index_notification_message(
            {'index_name': 'docs', 'reindex': True, 'report': make_report()}, initiator='schedule'
        )

        assert message.startswith('Index [docs]() is successfully reindexed by schedule.')

    def test_scheduled_run_that_changed_nothing(self, indexing_report):
        report = make_report(indexed=0, unchanged=196, skipped=0, total=196)

        message = indexing_report.build_index_notification_message(
            {'index_name': 'docs', 'reindex': True, 'report': report}, initiator='schedule'
        )

        assert message == 'Index [docs]() is up to date by schedule — 196 pages unchanged.'
        assert '0 pages' not in message

    def test_report_is_accepted_as_a_json_string(self, indexing_report):
        message = indexing_report.build_index_notification_message(
            {'index_name': 'docs', 'report': json.dumps(make_report())}
        )

        assert message == 'Index [docs]() is successfully created. 179 pages indexed.'

    def test_pre_report_run_falls_back_to_the_persisted_count(self, indexing_report):
        message = indexing_report.build_index_notification_message(
            {'index_name': 'docs', 'reindex': True, 'indexed': 42}
        )

        assert message == 'Index [docs]() is successfully reindexed. 42 documents indexed.'

    def test_no_raw_json_reaches_the_reader(self, indexing_report):
        message = indexing_report.build_index_notification_message(
            {'index_name': 'docs', 'reindex': True, 'report': make_report(skipped=12, total=191)}
        )

        assert '{' not in message and '"' not in message

    def test_unnamed_index_still_produces_a_message(self, indexing_report):
        message = indexing_report.build_index_notification_message({'report': make_report()})

        assert message.startswith('Index [Index]()')
