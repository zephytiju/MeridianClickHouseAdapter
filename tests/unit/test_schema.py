# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from meridian_storage.semantics import FieldDefinition, LogicalKind, LogicalType

from meridian_storage import ResourceRef
from meridian_storage.adapters.clickhouse import ClickHouseSchemaCompiler, ResourceLayout
from meridian_storage.adapters.clickhouse.schema import logical_type_to_clickhouse
from tests.conftest import RESOURCE_FINGERPRINT, build_schema


def test_schema_compilation_is_deterministic_and_round_trips() -> None:
    compiler = ClickHouseSchemaCompiler()
    kwargs = {
        "database": "meridian_adapter_test",
        "resource": ResourceRef("evidence", "observability", "metric_points"),
        "resource_fingerprint": RESOURCE_FINGERPRINT,
        "schema": build_schema(),
        "record_profile": "metric",
        "retention_seconds": 86_400,
    }
    first = compiler.compile(**kwargs)
    second = compiler.compile(**kwargs)

    assert first == second
    assert ResourceLayout.from_mapping(first.layout.to_dict()) == first.layout
    assert "ReplacingMergeTree" in first.create_table_sql
    assert "PARTITION BY toYYYYMM" in first.create_table_sql
    assert "TTL toDateTime" in first.create_table_sql
    assert first.layout.resource.catalog == "evidence"


@pytest.mark.parametrize(
    ("logical", "physical"),
    [
        (LogicalType(LogicalKind.BOOLEAN), "Bool"),
        (LogicalType(LogicalKind.UUID), "UUID"),
        (LogicalType(LogicalKind.UTC_TIMESTAMP), "DateTime64(9, 'UTC')"),
        (LogicalType(LogicalKind.DECIMAL, precision=76, scale=9), "Decimal(76, 9)"),
    ],
)
def test_logical_type_mapping(logical: LogicalType, physical: str) -> None:
    assert logical_type_to_clickhouse(logical) == physical


def test_decimal_precision_over_clickhouse_limit_is_rejected() -> None:
    field = FieldDefinition("too_wide", LogicalType(LogicalKind.DECIMAL, 77, 1))
    with pytest.raises(ValueError, match="at most 76"):
        ClickHouseSchemaCompiler().compile_column(field)


def test_strong_schema_is_rejected() -> None:
    schema = build_schema()
    object.__setattr__(schema, "consistency", "strong")
    with pytest.raises(ValueError, match="eventual"):
        ClickHouseSchemaCompiler().compile(
            database="meridian_adapter_test",
            resource=ResourceRef("evidence", "observability", "metric_points"),
            resource_fingerprint=RESOURCE_FINGERPRINT,
            schema=schema,
            record_profile="metric",
        )
