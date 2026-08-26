# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace

import pytest
from meridian_storage.semantics import Cardinality, FieldDefinition, LogicalKind, LogicalType

from meridian_storage import ResourceRef
from meridian_storage.adapters.clickhouse import (
    ClickHouseSchemaCompiler,
    ColumnLayout,
    ResourceLayout,
    Topology,
)
from meridian_storage.adapters.clickhouse.schema import (
    logical_type_to_clickhouse,
    metadata_table_ddl,
    physical_name,
)
from tests.conftest import RESOURCE_FINGERPRINT, build_schema


@pytest.mark.parametrize(
    ("logical", "physical"),
    [
        (LogicalType(LogicalKind.INT8), "Int8"),
        (LogicalType(LogicalKind.INT16), "Int16"),
        (LogicalType(LogicalKind.INT32), "Int32"),
        (LogicalType(LogicalKind.INT64), "Int64"),
        (LogicalType(LogicalKind.FLOAT64), "Float64"),
        (LogicalType(LogicalKind.STRING), "String"),
        (LogicalType(LogicalKind.BYTES), "String"),
        (LogicalType(LogicalKind.DATE), "Date32"),
        (LogicalType(LogicalKind.DURATION), "Int64"),
        (LogicalType(LogicalKind.ENUM, enum_values=("ready",)), "LowCardinality(String)"),
        (LogicalType(LogicalKind.JSON), "String"),
        (LogicalType(LogicalKind.RECORD_REF), "String"),
        (LogicalType(LogicalKind.OBJECT_REF), "String"),
        (LogicalType(LogicalKind.WGS84_POINT), "Tuple(Float64, Float64)"),
    ],
)
def test_all_v1_logical_types_have_closed_physical_mappings(
    logical: LogicalType, physical: str
) -> None:
    assert logical_type_to_clickhouse(logical) == physical


def test_nullable_and_many_columns_use_closed_nested_types() -> None:
    compiler = ClickHouseSchemaCompiler()
    nullable = compiler.compile_column(
        FieldDefinition("description", LogicalType(LogicalKind.STRING), nullable=True)
    )
    many_nullable = compiler.compile_column(
        FieldDefinition(
            "tags",
            LogicalType(LogicalKind.STRING),
            cardinality=Cardinality.MANY,
            nullable=True,
        )
    )
    assert nullable.clickhouse_type == "Nullable(String)"
    assert many_nullable.clickhouse_type == "Array(Nullable(String))"


@pytest.mark.parametrize(
    "physical_type",
    (
        "String) ENGINE = Memory --",
        "Decimal(77, 1)",
        "Decimal(5, 6)",
        "Nullable()",
        "Array(FutureType)",
    ),
)
def test_serialized_layout_cannot_inject_or_extend_clickhouse_types(
    physical_type: str,
) -> None:
    with pytest.raises(ValueError, match="closed V1 type grammar"):
        ColumnLayout("field", "c_field", "string", physical_type)


def test_resource_layout_rejects_invalid_roles_and_lifecycle_inputs(layout) -> None:  # type: ignore[no-untyped-def]
    mutations = (
        {"timestamp_field": "missing"},
        {"identity_fields": ()},
        {"dimension_fields": (layout.timestamp_field,)},
        {"retention_seconds": 0},
        {"partition_interval": "year"},
        {"schema_version": ""},
        {"query_final": "yes"},
        {"administrative_profiles": ("backup", "backup")},
        {"indexes": {"idx": ("missing",)}},
        {"columns": (layout.columns[0], layout.columns[0])},
    )
    for mutation in mutations:
        with pytest.raises((TypeError, ValueError)):
            replace(layout, **mutation)


def test_layout_mapping_is_closed_and_fingerprint_verified(layout) -> None:  # type: ignore[no-untyped-def]
    document = layout.to_dict()
    document["extra"] = True
    with pytest.raises(ValueError, match="unknown or missing"):
        ResourceLayout.from_mapping(document)

    document = layout.to_dict()
    document["layoutFingerprint"] = "sha256:" + "9" * 64
    with pytest.raises(ValueError, match="does not match"):
        ResourceLayout.from_mapping(document)


def test_schema_compiler_rejects_catalog_namespace_identity_and_role_mismatches() -> None:
    compiler = ClickHouseSchemaCompiler()
    schema = build_schema()
    common = {
        "database": "meridian_adapter_test",
        "resource_fingerprint": RESOURCE_FINGERPRINT,
        "schema": schema,
        "record_profile": "metric",
    }
    with pytest.raises(ValueError, match="structured or evidence"):
        compiler.compile(resource=ResourceRef("object", "observability", "metric_points"), **common)
    with pytest.raises(ValueError, match="share Catalog, Namespace"):
        compiler.compile(resource=ResourceRef("evidence", "other", "metric_points"), **common)
    with pytest.raises(ValueError, match="stable Schema identity"):
        compiler.compile(
            resource=ResourceRef("evidence", "observability", "metric_points"),
            identity_fields=(),
            **common,
        )
    with pytest.raises(ValueError, match="unknown Schema field"):
        compiler.compile(
            resource=ResourceRef("evidence", "observability", "metric_points"),
            timestamp_field="missing",
            **common,
        )


@pytest.mark.parametrize(
    ("partition", "fragment"),
    [("hour", "toStartOfHour"), ("day", "toDate("), ("month", "toYYYYMM")],
)
def test_partition_and_replicated_ddl_are_deterministic(partition: str, fragment: str) -> None:
    compilation = ClickHouseSchemaCompiler().compile(
        database="meridian_adapter_test",
        resource=ResourceRef("evidence", "observability", "metric_points"),
        resource_fingerprint=RESOURCE_FINGERPRINT,
        schema=build_schema(),
        record_profile="metric",
        topology=Topology.REPLICATED,
        partition_interval=partition,
    )
    assert fragment in compilation.create_table_sql
    assert "ReplicatedReplacingMergeTree" in compilation.create_table_sql
    assert "ReplicatedReplacingMergeTree" in compilation.metadata_table_sql
    assert compilation.statements == (
        compilation.metadata_table_sql,
        compilation.create_table_sql,
    )


def test_physical_names_are_stable_bounded_and_safe() -> None:
    first = physical_name("123 / Déjà Vu", prefix="c")
    second = physical_name("123 / Déjà Vu", prefix="c")
    assert first == second
    assert first.startswith("c_123_deja_vu_")
    assert len(physical_name("x" * 1000, prefix="c")) <= 127
    assert "ReplacingMergeTree" in metadata_table_ddl("meridian_adapter_test", Topology.STANDALONE)
