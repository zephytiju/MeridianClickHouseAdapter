# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from importlib import resources
from types import SimpleNamespace

import pytest
from meridian_storage.query import CursorSigner, TranslationContext

from meridian_storage.adapters.clickhouse import ClickHouseQueryTranslator, ClickHouseSettings
from meridian_storage.adapters.clickhouse.ingestion import BatchExecutor, prepare_batch
from meridian_storage.adapters.clickhouse.query import compile_simple_query
from tests.conftest import REGISTRY_FINGERPRINT, build_binding, build_request
from tests.fakes import FakeClient


def _vectors() -> dict[str, object]:
    root = resources.files("meridian_storage.adapters.clickhouse").joinpath(
        "contracts", "conformance"
    )
    return {
        document["caseId"]: document
        for document in (json.loads(item.read_text(encoding="utf-8")) for item in root.iterdir())
    }


def _translation(layout) -> TranslationContext:  # type: ignore[no-untyped-def]
    return TranslationContext(
        "clickhouse-test",
        "sha256:" + "4" * 64,
        REGISTRY_FINGERPRINT,
        {layout.resource.canonical: layout.schema_fingerprint},
        "sha256:" + "3" * 64,
        30_000,
    )


def test_metric_append_vector_executes(layout) -> None:  # type: ignore[no-untyped-def]
    vector = _vectors()["metric-append-eventual-v1"]
    assert isinstance(vector, dict)
    records = vector["records"]
    expected = vector["expected"]
    assert isinstance(expected, dict)
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    fake = FakeClient(layout)
    batch = prepare_batch(
        build_request(layout, scope={"region": "us-west"}),
        layout,
        records,
        settings,
        now=datetime(2026, 8, 25, 12, tzinfo=UTC),
    )
    receipt = BatchExecutor(fake, settings).execute(batch).to_dict()
    assert receipt["acceptedRows"] == expected["acceptedRows"]
    assert receipt["visibility"] == expected["visibility"]
    assert fake.inserts[0][-1]["insert_deduplication_token"] == receipt["batchId"]


def test_bounded_query_vector_executes_with_signed_keyset(layout) -> None:  # type: ignore[no-untyped-def]
    vector = _vectors()["bounded-scope-first-query-v1"]
    assert isinstance(vector, dict)
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    signer = CursorSigner({"key": b"k" * 32}, active_key_id="key")
    translator = ClickHouseQueryTranslator(settings, signer)
    compiled = compile_simple_query(
        "query",
        {
            "where": vector["where"],
            "orderBy": vector["orderBy"],
            "limit": vector["limit"],
        },
        layout,
        _translation(layout),
        settings,
        signer,
    )
    sql = compiled.command["sql"]
    assert isinstance(sql, str)
    where_clause = sql.split(" WHERE ", 1)[1]
    assert where_clause.index("_meridian_scope_fingerprint") < where_clause.index("observed_at")
    assert settings.max_time_range_seconds == vector["expected"]["maximumRangeSeconds"]

    columns = (
        "observed_at",
        "series_id",
        "service",
        "value",
        "__meridian_sort_0",
        "__meridian_sort_1",
        "__meridian_sort_2",
    )
    start = datetime(2026, 8, 25, 12, tzinfo=UTC)
    rows = tuple(
        (
            start - timedelta(seconds=index),
            f"series-{index:03}",
            "checkout",
            float(index),
            start - timedelta(seconds=index),
            f"series-{index:03}",
            f"{index:064x}",
        )
        for index in range(51)
    )
    normalized = translator.normalize_result(
        compiled, SimpleNamespace(column_names=columns, result_rows=rows)
    )
    assert normalized.cursor is not None
    assert len(normalized.data) == 50


def test_unbounded_query_vector_is_rejected(layout) -> None:  # type: ignore[no-untyped-def]
    vector = _vectors()["reject-unbounded-query-v1"]
    assert isinstance(vector, dict)
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    signer = CursorSigner({"key": b"k" * 32}, active_key_id="key")
    with pytest.raises(ValueError, match="bounded timestamp"):
        compile_simple_query(
            "query",
            {"where": vector["where"]},
            layout,
            _translation(layout),
            settings,
            signer,
        )
