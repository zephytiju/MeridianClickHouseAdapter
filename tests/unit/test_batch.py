# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from meridian_storage.adapters.clickhouse import ClickHouseSettings
from meridian_storage.adapters.clickhouse.ingestion import prepare_batch
from tests.conftest import build_binding, build_request, sample_record


def test_batch_is_bounded_scoped_and_retry_stable(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    first = prepare_batch(
        build_request(layout),
        layout,
        [sample_record()],
        settings,
        now=datetime(2026, 8, 25, tzinfo=UTC),
    )
    second = prepare_batch(
        build_request(layout),
        layout,
        [sample_record()],
        settings,
        now=datetime(2026, 8, 25, tzinfo=UTC),
    )

    assert first.batch_id == second.batch_id
    assert first.rows == second.rows
    assert len(first.rows[0][0]) == 64
    assert first.rows[0][1] == "tenant-a"
    assert first.rows[0][2] == first.batch_id


def test_scope_changes_stored_partition(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    first = prepare_batch(
        build_request(layout, scope={"region": "us-west"}),
        layout,
        [sample_record()],
        settings,
    )
    second = prepare_batch(
        build_request(layout, scope={"region": "eu-west"}),
        layout,
        [sample_record()],
        settings,
    )
    assert first.rows[0][0] != second.rows[0][0]


def test_unknown_and_oversized_batches_are_rejected(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    unknown = {**sample_record(), "clickhouseEngine": "MergeTree"}
    with pytest.raises(Exception, match="unknown Schema fields"):
        prepare_batch(build_request(layout), layout, [unknown], settings)
    with pytest.raises(Exception, match="row limit"):
        prepare_batch(
            build_request(layout),
            layout,
            [sample_record(series_id=str(index)) for index in range(101)],
            settings,
        )
