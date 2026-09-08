# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace
from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest

from meridian_storage.adapters.clickhouse import ClickHouseSettings, Topology
from meridian_storage.adapters.clickhouse.ingestion.batch import (
    BatchExecutor,
    _coerce,
    _records,
    prepare_batch,
)
from tests.conftest import build_binding, build_layout, build_request, sample_record
from tests.fakes import FakeClient


@pytest.mark.parametrize(
    ("value", "kind", "expected"),
    [
        (True, "boolean", True),
        (127, "int8", 127),
        (32_000, "int16", 32_000),
        (2_000_000_000, "int32", 2_000_000_000),
        (2**40, "int64", 2**40),
        (1000, "duration", 1000),
        (2, "float64", 2.0),
        ("text", "string", "text"),
        ("ready", "enum", "ready"),
        ("2026-08-25", "date", date(2026, 8, 25)),
        (
            "00000000-0000-0000-0000-000000000001",
            "uuid",
            UUID("00000000-0000-0000-0000-000000000001"),
        ),
        ("12.50", "decimal", Decimal("12.50")),
        ("aGVsbG8=", "bytes", b"hello"),
        ({"b": 2, "a": 1}, "json", '{"a":1,"b":2}'),
        ({"catalog": "object"}, "objectRef", '{"catalog":"object"}'),
        ([-122.4, 37.8], "wgs84Point", (-122.4, 37.8)),
    ],
)
def test_logical_values_are_coerced_deterministically(
    value: object, kind: str, expected: object
) -> None:
    assert _coerce(value, {"kind": kind}, many=False) == expected  # type: ignore[arg-type]


def test_utc_timestamp_and_many_values_are_coerced() -> None:
    assert _coerce("2026-08-25T12:00:00-07:00", "utcTimestamp", many=False) == 1787684400000000000
    assert _coerce(["a", "b"], "string", many=True) == ["a", "b"]
    assert _coerce([None, "a"], "string", many=True) == [None, "a"]


@pytest.mark.parametrize(
    ("value", "kind", "message"),
    [
        (1, "boolean", "boolean"),
        (True, "int8", "integer"),
        (128, "int8", "signed range"),
        (float("inf"), "float64", "finite"),
        (1, "string", "text"),
        ("2026-08-25T12:00:00", "utcTimestamp", "offset"),
        (1, "uuid", "canonical text"),
        (float("inf"), "decimal", "finite"),
        ("not-base64", "bytes", "base64"),
        ([181, 0], "wgs84Point", "valid range"),
        ([True, 0], "wgs84Point", "numbers"),
        ("value", "futureType", "unsupported"),
    ],
)
def test_invalid_logical_values_fail_before_the_driver(
    value: object, kind: str, message: str
) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        _coerce(value, {"kind": kind}, many=False)  # type: ignore[arg-type]


def test_record_shape_and_batch_byte_limits_are_enforced(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    request = build_request(layout)
    with pytest.raises(TypeError, match="keys must be strings"):
        _records({1: "bad"})
    with pytest.raises(TypeError, match="array of records"):
        _records("bad")
    with pytest.raises(TypeError, match="record objects"):
        _records(["bad"])
    with pytest.raises(Exception, match="cannot be empty"):
        prepare_batch(request, layout, [], settings)
    with pytest.raises(Exception, match="missing required field"):
        prepare_batch(request, layout, {"observed_at": "2026-08-25T00:00:00Z"}, settings)
    with pytest.raises(Exception, match="cannot be null"):
        prepare_batch(request, layout, {**sample_record(), "service": None}, settings)
    with pytest.raises(Exception, match="byte limit"):
        prepare_batch(request, layout, sample_record(), replace(settings, max_batch_bytes=10))


def test_batch_identity_is_partition_and_schema_bound(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    first = prepare_batch(
        build_request(layout, scope={"region": "us-west"}), layout, sample_record(), settings
    )
    second = prepare_batch(
        build_request(layout, scope={"region": "eu-west"}), layout, sample_record(), settings
    )
    changed_layout = replace(layout, schema_fingerprint="sha256:" + "8" * 64)
    changed_settings = ClickHouseSettings.from_binding(build_binding(changed_layout))
    third = prepare_batch(
        build_request(changed_layout, scope={"region": "us-west"}),
        changed_layout,
        sample_record(),
        changed_settings,
    )
    assert len({first.batch_id, second.batch_id, third.batch_id}) == 3


def test_replicated_batch_requests_insert_quorum() -> None:
    layout = build_layout(topology=Topology.REPLICATED)
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    fake = FakeClient(layout)
    batch = prepare_batch(build_request(layout), layout, sample_record(), settings)
    receipt = BatchExecutor(fake, settings).execute(batch)

    assert receipt.to_dict()["acceptedRows"] == 1
    assert fake.inserts[0][-1] == {
        "insert_deduplication_token": batch.batch_id,
        "insert_quorum": 1,
    }
