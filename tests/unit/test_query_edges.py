# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest
from meridian_storage.query import (
    Aggregate,
    BinaryExpression,
    BooleanExpression,
    CursorSigner,
    Field,
    ImplementationMode,
    Literal,
    MembershipExpression,
    NamedAggregate,
    NullTest,
    PageSpec,
    PlannedQuery,
    Projection,
    QueryOperation,
    QueryTarget,
    ResultSpec,
    Sort,
    TimestampRange,
    TranslationContext,
    UnaryExpression,
    infer_requirements,
)

from meridian_storage.adapters.clickhouse import ClickHouseQueryTranslator, ClickHouseSettings
from meridian_storage.adapters.clickhouse.query.compiler import (
    _as_mapping,
    _datetime,
    _field_names,
    _json_value,
    _parameter_type,
    _SQLCompiler,
    compile_simple_query,
    compiler_database,
)
from tests.conftest import REGISTRY_FINGERPRINT, build_binding


def _bounds() -> TimestampRange:
    return TimestampRange(
        Field("observed_at"),
        Literal("2026-08-25T00:00:00Z", "utcTimestamp"),
        Literal("2026-08-26T00:00:00Z", "utcTimestamp"),
    )


def _context(layout, fingerprint="sha256:" + "4" * 64):  # type: ignore[no-untyped-def]
    return TranslationContext(
        binding_id="clickhouse-test",
        plan_fingerprint=fingerprint,
        registry_fingerprint=REGISTRY_FINGERPRINT,
        schema_fingerprints={layout.resource.canonical: layout.schema_fingerprint},
        scope_fingerprint="sha256:" + "3" * 64,
        deadline_ms=30_000,
    )


def _translator(layout):  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    signer = CursorSigner({"key": b"k" * 32}, active_key_id="key")
    return settings, signer, ClickHouseQueryTranslator(settings, signer)


def test_planned_boolean_membership_null_and_comparison_expressions(layout) -> None:  # type: ignore[no-untyped-def]
    settings, _signer, _translator_value = _translator(layout)
    compiler = _SQLCompiler(layout, settings.database)
    expression = BooleanExpression(
        "and",
        (
            BinaryExpression("eq", Field("service"), Literal("checkout", "string")),
            BooleanExpression(
                "or",
                (
                    MembershipExpression(
                        Field("series_id"),
                        (Literal("a", "string"), Literal("b", "string")),
                        negated=True,
                    ),
                    NullTest(Field("service"), is_null=False),
                ),
            ),
            UnaryExpression(
                "not",
                BinaryExpression("lt", Field("value"), Literal(0.0, "float64")),
            ),
        ),
    )
    sql = compiler.expression(expression)
    assert "NOT IN" in sql
    assert "IS NOT NULL" in sql
    assert "NOT (" in sql
    assert "checkout" not in sql


def test_planned_expression_rejections_are_deterministic(layout) -> None:  # type: ignore[no-untyped-def]
    settings, _signer, _translator_value = _translator(layout)
    compiler = _SQLCompiler(layout, settings.database)
    with pytest.raises(ValueError, match="does not advertise"):
        compiler.expression(BinaryExpression("prefix", Field("service"), Literal("x", "string")))
    with pytest.raises(ValueError, match="arithmetic unary"):
        compiler.expression(UnaryExpression("negate", Field("value")))
    with pytest.raises(ValueError, match="membership values"):
        compiler.expression(MembershipExpression(Field("service"), (Field("series_id"),)))
    with pytest.raises(ValueError, match="boundaries must be literals"):
        compiler.expression(TimestampRange(Field("observed_at"), Field("observed_at"), None))
    with pytest.raises(ValueError, match="qualifier"):
        compiler.expression(Field("service", "evidence:other/resource"))
    with pytest.raises(ValueError, match="sort tuple"):
        compiler.keyset((("`value`", "asc", "Float64"),), ())


def test_mapping_filters_support_closed_operator_set(layout) -> None:  # type: ignore[no-untyped-def]
    settings, signer, _translator_value = _translator(layout)
    compiled = compile_simple_query(
        "query",
        {
            "where": {
                "observed_at": {
                    "gte": "2026-08-25T00:00:00Z",
                    "lt": "2026-08-26T00:00:00Z",
                },
                "series_id": {"in": ["one", "two"], "ne": "zero"},
                "service": {"notIn": ["internal"], "isNull": False},
                "value": {"gt": 0.0, "lte": 100.0},
            },
            "select": ["series_id", "value"],
            "orderBy": [{"field": "value", "direction": "asc"}],
            "limit": 5,
        },
        layout,
        _context(layout),
        settings,
        signer,
    )
    sql = compiled.command["sql"]
    assert isinstance(sql, str)
    assert " NOT IN " in sql
    assert " IS NOT NULL" in sql
    assert " ORDER BY " in sql
    assert " LIMIT 6" in sql
    assert all(value not in sql for value in ("internal", "one", "zero"))


@pytest.mark.parametrize(
    ("where", "message"),
    [
        ({"unknown": 1}, "unknown field"),
        ({"service": {"contains": "x"}}, "unsupported"),
        ({"service": {"in": []}}, "cannot be empty"),
        ({"service": {"notIn": "x"}}, "must be an array"),
        ({"service": {"isNull": "yes"}}, "must be boolean"),
    ],
)
def test_mapping_filter_rejections(layout, where: object, message: str) -> None:  # type: ignore[no-untyped-def]
    settings, signer, _translator_value = _translator(layout)
    compiler = _SQLCompiler(layout, settings.database)
    with pytest.raises((TypeError, ValueError), match=message):
        compiler.simple_where(where)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="where must be an object"):
        compile_simple_query(
            "query",
            {"where": "bad"},
            layout,
            _context(layout),
            settings,
            signer,
        )


def test_mapping_aggregate_functions_are_exact_and_named(layout) -> None:  # type: ignore[no-untyped-def]
    settings, signer, _translator_value = _translator(layout)
    compiled = compile_simple_query(
        "aggregate",
        {
            "where": {
                "observed_at": {
                    "gte": "2026-08-25T00:00:00Z",
                    "lt": "2026-08-26T00:00:00Z",
                }
            },
            "metrics": [
                {"name": "rows", "function": "count"},
                {
                    "name": "series",
                    "function": "count",
                    "field": "series_id",
                    "distinct": True,
                },
                {"name": "total", "function": "sum", "field": "value"},
                {"name": "mean", "function": "avg", "field": "value"},
                {"name": "minimum", "function": "min", "field": "value"},
                {"name": "maximum", "function": "max", "field": "value"},
                {
                    "name": "p95",
                    "function": "quantile",
                    "field": "value",
                    "quantile": 0.95,
                },
            ],
        },
        layout,
        _context(layout),
        settings,
        signer,
    )
    sql = compiled.command["sql"]
    assert isinstance(sql, str)
    for fragment in (
        "count()",
        "uniqExact",
        "sum(",
        "avg(",
        "min(",
        "max(",
        "quantileExact(0.95)",
    ):
        assert fragment in sql


@pytest.mark.parametrize(
    ("metric", "message"),
    [
        ("count", "must be an object"),
        ({"name": "n"}, "unknown or missing"),
        ({"name": 1, "function": "count"}, "name or function"),
        ({"name": "sum", "function": "sum"}, "requires a field"),
        (
            {"name": "p", "function": "quantile", "field": "value", "quantile": True},
            "numeric quantile",
        ),
        (
            {"name": "p", "function": "quantile", "field": "value", "quantile": 1.0},
            "between zero and one",
        ),
    ],
)
def test_mapping_aggregate_rejections(layout, metric: object, message: str) -> None:  # type: ignore[no-untyped-def]
    settings, signer, _translator_value = _translator(layout)
    with pytest.raises((TypeError, ValueError), match=message):
        compile_simple_query(
            "aggregate",
            {
                "where": {
                    "observed_at": {
                        "gte": "2026-08-25T00:00:00Z",
                        "lt": "2026-08-26T00:00:00Z",
                    }
                },
                "metrics": [metric],
            },
            layout,
            _context(layout),
            settings,
            signer,
        )


def test_planned_projection_total_order_and_aggregates(layout) -> None:  # type: ignore[no-untyped-def]
    _settings, _signer, translator = _translator(layout)
    record = QueryOperation(
        catalog="evidence",
        targets=(QueryTarget(layout.resource),),
        operation="scan",
        result=ResultSpec(
            projection=(Projection(Field("value"), "metric_value"),), include_total=True
        ),
        filter=_bounds(),
        order=(Sort(Field("service"), "asc"),),
        page=PageSpec(10),
        consistency="eventual",
    )
    record_sql = translator.compile_wire(record, _context(layout, record.fingerprint)).command[
        "sql"
    ]
    assert isinstance(record_sql, str)
    assert "AS `metric_value`" in record_sql
    assert "count() OVER ()" in record_sql

    aggregate = QueryOperation(
        catalog="evidence",
        targets=(QueryTarget(layout.resource),),
        operation="aggregate",
        result=ResultSpec("aggregate"),
        filter=_bounds(),
        grouping=(Field("service"),),
        aggregates=(
            NamedAggregate("rows", Aggregate("count")),
            NamedAggregate("series", Aggregate("count", Field("series_id"), distinct=True)),
            NamedAggregate("total", Aggregate("sum", Field("value"))),
            NamedAggregate("mean", Aggregate("avg", Field("value"))),
            NamedAggregate("low", Aggregate("min", Field("value"))),
            NamedAggregate("high", Aggregate("max", Field("value"))),
            NamedAggregate("median", Aggregate("percentile", Field("value"))),
        ),
        consistency="eventual",
    )
    aggregate_sql = translator.compile_wire(
        aggregate, _context(layout, aggregate.fingerprint)
    ).command["sql"]
    assert isinstance(aggregate_sql, str)
    assert "GROUP BY" in aggregate_sql
    assert "quantileExact(0.5)" in aggregate_sql
    assert "uniqExact" in aggregate_sql


def test_planned_query_identity_empty_and_consistency_guards(layout) -> None:  # type: ignore[no-untyped-def]
    _settings, _signer, translator = _translator(layout)
    operation = QueryOperation(
        catalog="evidence",
        targets=(QueryTarget(layout.resource),),
        operation="scan",
        filter=_bounds(),
        consistency="eventual",
    )
    requirements = infer_requirements(operation)
    plan = PlannedQuery(
        operation,
        "clickhouse-test",
        requirements,
        {item.semantic_id: ImplementationMode.NATIVE for item in requirements.requirements},
        translator.capabilities.fingerprint,
        REGISTRY_FINGERPRINT,
        {layout.resource.canonical: layout.schema_fingerprint},
        empty_result=True,
    )
    context = plan.translation_context(scope_fingerprint="sha256:" + "3" * 64)
    empty = translator.compile(plan, context)
    assert empty.command["empty"] is True
    assert translator.normalize_result(empty, object()).data == ()

    with pytest.raises(TypeError, match="validated PlannedQuery"):
        translator.compile(object(), context)
    with pytest.raises(ValueError, match="identities differ"):
        translator.compile(plan, replace(context, binding_id="other"))
    with pytest.raises(ValueError, match="eventual consistency"):
        translator.compile_wire(replace(operation, consistency="strong"), _context(layout))
    with pytest.raises(ValueError, match="Schema fingerprint"):
        translator.compile_wire(
            operation,
            replace(
                _context(layout, operation.fingerprint),
                schema_fingerprints={layout.resource.canonical: "sha256:" + "9" * 64},
            ),
        )


def test_mapping_live_keyset_round_trip(layout) -> None:  # type: ignore[no-untyped-def]
    settings, signer, translator = _translator(layout)
    input_value = {
        "where": {
            "observed_at": {
                "gte": "2026-08-25T00:00:00Z",
                "lt": "2026-08-26T00:00:00Z",
            }
        },
        "select": ["series_id"],
        "limit": 1,
    }
    compiled = compile_simple_query(
        "query", input_value, layout, _context(layout), settings, signer
    )
    raw = SimpleNamespace(
        column_names=(
            "series_id",
            "__meridian_sort_0",
            "__meridian_sort_1",
            "__meridian_sort_2",
        ),
        result_rows=(
            ("one", datetime(2026, 8, 25, 2, tzinfo=UTC), "one", "a" * 64),
            ("two", datetime(2026, 8, 25, 1, tzinfo=UTC), "two", "b" * 64),
        ),
    )
    normalized = translator.normalize_result(compiled, raw)
    assert normalized.cursor is not None
    next_compiled = compile_simple_query(
        "query",
        {**input_value, "cursor": normalized.cursor},
        layout,
        _context(layout),
        settings,
        signer,
    )
    assert " OR " in next_compiled.command["sql"]


def test_normalization_and_low_level_guards_cover_wire_types(layout) -> None:  # type: ignore[no-untyped-def]
    assert _json_value(None) is None
    assert _json_value(1.5) == 1.5
    assert _json_value(datetime(2026, 8, 25, tzinfo=UTC)) == "2026-08-25T00:00:00Z"
    assert _json_value(date(2026, 8, 25)) == "2026-08-25"
    assert _json_value(b"hello") == "aGVsbG8="
    assert _json_value({"nested": [1, b"a"]}) == {"nested": [1, "YQ=="]}
    with pytest.raises(ValueError, match="non-finite"):
        _json_value(float("nan"))
    with pytest.raises(TypeError, match="must be an object"):
        _as_mapping([], "value")  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="database was not bound"):
        compiler_database(object())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="field names"):
        _field_names(["ok", ""], "select")
    assert _parameter_type("Nullable(LowCardinality(String))") == "String"
    assert _datetime("2026-08-25T01:00:00+01:00") == datetime(2026, 8, 25, tzinfo=UTC)
    with pytest.raises(TypeError, match="RFC 3339"):
        _datetime(1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="include an offset"):
        _datetime("2026-08-25T00:00:00")
