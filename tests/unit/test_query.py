# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from meridian_storage.query import (
    CursorSigner,
    Field,
    ImplementationMode,
    Literal,
    PageSpec,
    PlannedQuery,
    QueryOperation,
    QueryTarget,
    SafetyBudget,
    TimestampRange,
    TranslationContext,
    assert_translation_contract,
    infer_requirements,
)

from meridian_storage.adapters.clickhouse import ClickHouseQueryTranslator, ClickHouseSettings
from meridian_storage.adapters.clickhouse.query import compile_simple_query
from tests.conftest import REGISTRY_FINGERPRINT, build_binding


def _logical_query(layout, *, cursor=None):  # type: ignore[no-untyped-def]
    return QueryOperation(
        catalog="evidence",
        targets=(QueryTarget(layout.resource),),
        operation="scan",
        filter=TimestampRange(
            Field("observed_at"),
            Literal("2026-08-25T00:00:00Z", "utcTimestamp"),
            Literal("2026-08-26T00:00:00Z", "utcTimestamp"),
        ),
        page=PageSpec(2, cursor),
        consistency="eventual",
    )


def _translation(layout, fingerprint):  # type: ignore[no-untyped-def]
    return TranslationContext(
        binding_id="clickhouse-test",
        plan_fingerprint=fingerprint,
        registry_fingerprint=REGISTRY_FINGERPRINT,
        schema_fingerprints={layout.resource.canonical: layout.schema_fingerprint},
        scope_fingerprint="sha256:" + "3" * 64,
        deadline_ms=30_000,
    )


def test_query_is_scope_first_bounded_and_parameterized(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    translator = ClickHouseQueryTranslator(settings, signer)
    logical = _logical_query(layout)
    compiled = translator.compile_wire(logical, _translation(layout, logical.fingerprint))
    command = compiled.command
    assert isinstance(command, dict)
    sql = command["sql"]
    assert isinstance(sql, str)

    assert "WHERE `_meridian_scope_fingerprint` = {p" in sql
    assert "2026-08-25" not in sql
    assert "LIMIT 3" in sql
    assert "3" * 64 in compiled.parameters.values()


def test_unbounded_or_excessive_time_ranges_are_rejected(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    translator = ClickHouseQueryTranslator(
        settings,
        CursorSigner({"k1": b"1" * 32}, active_key_id="k1"),
    )
    unbounded = QueryOperation(
        catalog="evidence",
        targets=(QueryTarget(layout.resource),),
        operation="scan",
        consistency="eventual",
    )
    with pytest.raises(ValueError, match="bounded timestamp range"):
        translator.compile_wire(unbounded, _translation(layout, unbounded.fingerprint))

    excessive = QueryOperation(
        catalog="evidence",
        targets=(QueryTarget(layout.resource),),
        operation="scan",
        filter=TimestampRange(
            Field("observed_at"),
            Literal("2026-08-01T00:00:00Z", "utcTimestamp"),
            Literal("2026-08-26T00:00:00Z", "utcTimestamp"),
        ),
        consistency="eventual",
    )
    with pytest.raises(ValueError, match="exceeds"):
        translator.compile_wire(excessive, _translation(layout, excessive.fingerprint))


def test_live_keyset_cursor_round_trip(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    translator = ClickHouseQueryTranslator(settings, signer)
    logical = _logical_query(layout)
    compiled = translator.compile_wire(logical, _translation(layout, logical.fingerprint))
    columns = (
        "observed_at",
        "series_id",
        "service",
        "value",
        "__meridian_sort_0",
        "__meridian_sort_1",
        "__meridian_sort_2",
    )
    raw = SimpleNamespace(
        column_names=columns,
        result_rows=(
            (
                datetime(2026, 8, 25, 3, tzinfo=UTC),
                "a",
                "checkout",
                3.0,
                datetime(2026, 8, 25, 3, tzinfo=UTC),
                "a",
                "a" * 64,
            ),
            (
                datetime(2026, 8, 25, 2, tzinfo=UTC),
                "b",
                "checkout",
                2.0,
                datetime(2026, 8, 25, 2, tzinfo=UTC),
                "b",
                "b" * 64,
            ),
            (
                datetime(2026, 8, 25, 1, tzinfo=UTC),
                "c",
                "checkout",
                1.0,
                datetime(2026, 8, 25, 1, tzinfo=UTC),
                "c",
                "c" * 64,
            ),
        ),
    )
    normalized = translator.normalize_result(compiled, raw)
    assert normalized.cursor is not None
    assert len(normalized.data) == 2
    assert "__meridian_sort_0" not in normalized.data[0]

    next_query = _logical_query(layout, cursor=normalized.cursor)
    next_compiled = translator.compile_wire(
        next_query,
        _translation(layout, next_query.fingerprint),
    )
    next_sql = next_compiled.command["sql"]
    assert isinstance(next_sql, str)
    assert " OR " in next_sql


def test_mapping_first_quantile_profile_is_exact_and_validated(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    context = _translation(layout, "sha256:" + "4" * 64)
    compiled = compile_simple_query(
        "aggregate",
        {
            "where": {
                "observed_at": {
                    "gte": "2026-08-25T00:00:00Z",
                    "lt": "2026-08-26T00:00:00Z",
                }
            },
            "groupBy": ["service"],
            "metrics": [
                {
                    "name": "p99",
                    "function": "quantile",
                    "field": "value",
                    "quantile": 0.99,
                }
            ],
        },
        layout,
        context,
        settings,
        CursorSigner({"k1": b"1" * 32}, active_key_id="k1"),
    )
    sql = compiled.command["sql"]
    assert isinstance(sql, str)
    assert "quantileExact(0.99)" in sql
    assert "GROUP BY" in sql


def test_released_query_planner_contract_compiles_natively(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    translator = ClickHouseQueryTranslator(
        settings,
        CursorSigner({"k1": b"1" * 32}, active_key_id="k1"),
    )
    operation = QueryOperation(
        catalog="evidence",
        targets=(QueryTarget(layout.resource),),
        operation="scan",
        filter=TimestampRange(
            Field("observed_at"),
            Literal("2026-08-25T00:00:00Z", "utcTimestamp"),
            Literal("2026-08-26T00:00:00Z", "utcTimestamp"),
        ),
        page=PageSpec(50),
        consistency="eventual",
        budget=SafetyBudget(max_normalized_bytes=settings.max_result_bytes),
    )
    requirements = infer_requirements(operation)
    for requirement in requirements.requirements:
        supported, reason = translator.capabilities.supports(
            requirement,
            operation=operation.operation,
        )
        assert supported, reason
        assert translator.capabilities.is_native(requirement.semantic_id)
    plan = PlannedQuery(
        operation=operation,
        binding_id="clickhouse-test",
        requirements=requirements,
        assignments={
            item.semantic_id: ImplementationMode.NATIVE for item in requirements.requirements
        },
        capability_fingerprint=translator.capabilities.fingerprint,
        registry_fingerprint=REGISTRY_FINGERPRINT,
        schema_fingerprints={layout.resource.canonical: layout.schema_fingerprint},
    )
    context = plan.translation_context(scope_fingerprint="sha256:" + "3" * 64)
    compiled = assert_translation_contract(translator, plan, context)
    assert compiled.adapter_id == "meridian.storage.clickhouse"


@pytest.mark.parametrize("wire", [False, True])
def test_schema_directed_binary_and_json_result_normalization(wire):
    from dataclasses import replace

    from meridian_storage.query import Projection, ResultSpec
    from meridian_storage.semantics import (
        FieldDefinition,
        LogicalKind,
        LogicalType,
        canonical_json_bytes,
    )

    from meridian_storage.adapters.clickhouse import ClickHouseSchemaCompiler
    from tests.conftest import RESOURCE_FINGERPRINT, build_layout, build_schema

    schema = build_schema()
    schema = replace(
        schema,
        fields=(
            *schema.fields,
            FieldDefinition("binary", LogicalType(LogicalKind.BYTES)),
            FieldDefinition("attributes", LogicalType(LogicalKind.JSON)),
        ),
    )
    layout = (
        ClickHouseSchemaCompiler()
        .compile(
            database="meridian_adapter_test",
            resource=build_layout().resource,
            resource_fingerprint=RESOURCE_FINGERPRINT,
            schema=schema,
            record_profile="metric",
        )
        .layout
    )
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    signer = CursorSigner({"k1": b"1" * 32}, active_key_id="k1")
    translator = ClickHouseQueryTranslator(settings, signer)
    logical = _logical_query(layout)
    if wire:
        logical = replace(
            logical,
            result=ResultSpec(
                projection=(
                    Projection(Field("binary"), "id"),
                    Projection(Field("attributes"), "payload"),
                )
            ),
        )
        compiled = translator.compile_wire(logical, _translation(layout, logical.fingerprint))
        names = ("id", "payload")
    else:
        compiled = compile_simple_query(
            "query",
            {
                "where": {
                    "observed_at": {"gte": "2026-08-25T00:00:00Z", "lt": "2026-08-26T00:00:00Z"}
                },
                "select": ["binary", "attributes"],
            },
            layout,
            _translation(layout, logical.fingerprint),
            settings,
            signer,
        )
        names = ("binary", "attributes")
    assert compiled.command["columnFormats"] == {names[0]: "bytes", "__meridian_sort_0": "int"}
    result = translator.normalize_result(
        compiled,
        SimpleNamespace(
            column_names=names,
            result_rows=[(b"\xff\x00", '{"typed":[true,1,null]}')],
        ),
    )
    assert canonical_json_bytes(result.operation_data()["items"]) == canonical_json_bytes(
        [{names[0]: "/wA=", names[1]: {"typed": [True, 1, None]}}]
    )
