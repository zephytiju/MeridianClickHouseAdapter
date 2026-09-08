# SPDX-License-Identifier: Apache-2.0
"""Primary timestamp precision across the public batch and query boundaries."""

from hashlib import sha256
from types import SimpleNamespace

import pytest
from meridian_storage.query import CursorSigner, Field, Literal, TimestampRange
from meridian_storage.semantics import canonical_json_bytes

from meridian_storage.adapters.clickhouse import ClickHouseSettings
from meridian_storage.adapters.clickhouse.ingestion import prepare_batch
from tests.conftest import build_binding, build_request, sample_record
from tests.unit.test_cursor_fingerprint import compile_page

NANOS = (1787659200123456789, 1787659200123456790, 1787659200123456791)
TIMES = tuple(f"2026-08-25T12:00:00.{n % 1_000_000_000:09d}Z" for n in NANOS)


def test_prepare_batch_preserves_adjacent_nanoseconds_and_original_identity(layout):
    records = [sample_record(observed_at=t, series_id=f"row-{i}") for i, t in enumerate(TIMES)]
    request = build_request(layout, records=records)
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    batch = prepare_batch(request, layout, records, settings)
    index = layout.insert_columns.index(layout.physical_column("observed_at"))
    assert tuple(row[index] for row in batch.rows) == NANOS
    assert [row[3] for row in batch.rows] == [
        sha256(canonical_json_bytes(record)).hexdigest() for record in records
    ]
    assert prepare_batch(request, layout, records, settings).batch_id == batch.batch_id


@pytest.mark.parametrize("wire", [False, True])
def test_nanosecond_results_and_signed_boundary_bind_exactly(layout, wire):
    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    translator, compiled = compile_page(layout, signer, wire=wire)
    assert compiled.command["columnFormats"]["observed_at"] == "int"
    assert compiled.command["columnFormats"]["__meridian_sort_0"] == "int"
    raw = SimpleNamespace(
        column_names=("observed_at", "__meridian_sort_0", "__meridian_sort_1", "__meridian_sort_2"),
        result_rows=[(n, n, "same", b"f" * 64) for n in reversed(NANOS)],
    )
    normalized = translator.normalize_result(compiled, raw)
    assert [row["observed_at"] for row in normalized.data] == list(reversed(TIMES))[:2]
    _, following = compile_page(layout, signer, wire=wire, cursor=normalized.cursor)
    assert "2026-08-25 12:00:00.123456790" in following.parameters.values()
    assert "f" * 64 in following.parameters.values()


@pytest.mark.parametrize("wire", [False, True])
def test_one_nanosecond_range_is_nonempty_and_parameters_remain_exact(layout, wire):
    changes = (
        {
            "filter": TimestampRange(
                Field("observed_at"),
                Literal(TIMES[0], "utcTimestamp"),
                Literal(TIMES[1], "utcTimestamp"),
            )
        }
        if wire
        else {"where": {"observed_at": {"gte": TIMES[0], "lt": TIMES[1]}}}
    )
    _, compiled = compile_page(
        layout,
        CursorSigner({"k1": b"1" * 32}, active_key_id="k1"),
        wire=wire,
        input_changes=changes,
    )
    assert "2026-08-25 12:00:00.123456789" in compiled.parameters.values()
    assert "2026-08-25 12:00:00.123456790" in compiled.parameters.values()


@pytest.mark.parametrize(
    ("value", "ticks", "canonical"),
    [
        ("2026-08-25T05:00:00.123456789-07:00", NANOS[0], TIMES[0]),
        ("2026-08-25T13:00:00.123456789+01:00", NANOS[0], TIMES[0]),
        ("2026-08-25T12:00:00.123456Z", NANOS[0] - 789, "2026-08-25T12:00:00.123456Z"),
        ("1969-12-31T23:59:59.999999999Z", -1, "1969-12-31T23:59:59.999999999Z"),
        ("1970-01-01T00:00:00.000000001Z", 1, "1970-01-01T00:00:00.000000001Z"),
        ("2262-04-11T23:47:16.854775807Z", 2**63 - 1, "2262-04-11T23:47:16.854775807Z"),
    ],
)
def test_lossless_offsets_microseconds_and_signed_boundaries(value, ticks, canonical):
    from meridian_storage.adapters.clickhouse._timestamps import (
        timestamp_nanoseconds,
        timestamp_text,
    )

    assert timestamp_nanoseconds(value) == ticks
    assert timestamp_text(ticks) == canonical
    assert timestamp_nanoseconds(timestamp_text(ticks)) == ticks


@pytest.mark.parametrize(
    "value",
    [
        "2026-08-25T12:00:00.1234567891Z",
        "2026-08-25T12:00:00",
        "not-a-time",
        "2262-04-11T23:47:16.854775808Z",
        "2026-08-25T12:00:00+24:00",
    ],
)
def test_invalid_or_unrepresentable_timestamps_fail_before_transmission(value):
    from meridian_storage.adapters.clickhouse._timestamps import timestamp_nanoseconds

    with pytest.raises(ValueError):
        timestamp_nanoseconds(value)


@pytest.mark.parametrize("wire", [False, True])
@pytest.mark.parametrize("change", ["scope", "schema", "registry", "plan", "page", "signature"])
def test_nanosecond_cursor_keeps_signed_context_validation(layout, wire, change):
    from meridian_storage.query import InvalidCursor, PageSpec, Sort

    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    translator, compiled = compile_page(layout, signer, wire=wire)
    result = translator.normalize_result(
        compiled,
        SimpleNamespace(
            column_names=("__meridian_sort_0", "__meridian_sort_1", "__meridian_sort_2"),
            result_rows=[(n, "same", b"f" * 64) for n in reversed(NANOS)],
        ),
    )
    contexts = {
        "scope": {"scope_fingerprint": "sha256:" + "9" * 64},
        "schema": {"schema_fingerprints": {layout.resource.canonical: "sha256:" + "9" * 64}},
        "registry": {"registry_fingerprint": "sha256:" + "9" * 64},
    }
    changes = None
    if change == "plan":
        changes = (
            {"order": (Sort(Field("series_id"), "desc"),)} if wire else {"select": ["series_id"]}
        )
    if change == "page":
        changes = {"page": PageSpec(1, result.cursor)} if wire else {"limit": 1}
    if change == "signature":
        signer = CursorSigner({"k1": b"2" * 32}, active_key_id="k1")
    with pytest.raises((ValueError, InvalidCursor)):
        compile_page(
            layout,
            signer,
            wire=wire,
            cursor=result.cursor,
            context_changes=contexts.get(change),
            input_changes=changes,
        )


def test_exact_range_limit_cannot_be_widened_by_fractional_truncation(layout):
    from meridian_storage.adapters.clickhouse.query.compiler import _validate_range

    with pytest.raises(ValueError, match="exceeds"):
        _validate_range("2026-08-25T00:00:00.123456789Z", "2026-08-26T00:00:00.123456790Z", 86400)


def test_timestamp_arrays_and_nulls_normalize_without_integer_leakage():
    from meridian_storage.adapters.clickhouse.query.compiler import _logical_json_value

    assert _logical_json_value(
        [NANOS[0], None, [NANOS[1]]],
        "times",
        {
            "timestampColumns": ["times"],
        },
    ) == [TIMES[0], None, [TIMES[1]]]


@pytest.mark.parametrize("wire", [False, True])
def test_valid_legacy_microsecond_cursor_resumes_its_original_boundary(layout, wire):
    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    _, compiled = compile_page(layout, signer, wire=wire)
    command = compiled.command
    cursor = signer.issue(
        plan_fingerprint=command["cursorPlanFingerprint"],
        schema_fingerprints=command["schemaFingerprints"],
        registry_fingerprint=command["registryFingerprint"],
        scope_fingerprint=command["scopeFingerprint"],
        sort_tuple=("2026-08-25T12:00:00.123456Z", "same", "f" * 64),
        page_size=command["pageSize"],
    )
    _, compiled = compile_page(layout, signer, wire=wire, cursor=cursor)
    assert "2026-08-25 12:00:00.123456000" in compiled.parameters.values()
