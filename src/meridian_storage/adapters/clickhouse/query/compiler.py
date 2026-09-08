# SPDX-License-Identifier: Apache-2.0
"""Compile validated logical queries to parameterized ClickHouse commands."""

from __future__ import annotations

import base64
import binascii
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

from meridian_storage.query import (
    Aggregate,
    BinaryExpression,
    BooleanExpression,
    CompiledQuery,
    CursorExpectations,
    CursorSigner,
    Field,
    Literal,
    MembershipExpression,
    NormalizedQueryResult,
    NullTest,
    PlannedQuery,
    QueryOperation,
    TimestampRange,
    TranslationContext,
    UnaryExpression,
    ValueExpression,
)
from meridian_storage.semantics import JsonValue, canonical_json_bytes, sha256_fingerprint

from .._canonical import quote_identifier
from .._timestamps import timestamp_nanoseconds, timestamp_text
from ..configuration import ADAPTER_ID, ClickHouseSettings
from ..descriptor import query_capabilities
from ..schema import HIDDEN_ROW, HIDDEN_SCOPE, ResourceLayout

_SORT_ALIAS_PREFIX = "__meridian_sort_"
_TOTAL_ALIAS = "__meridian_total"


class ClickHouseQueryTranslator:
    """Translate released Query 1.0.0 plans without exposing ClickHouse to consumers."""

    def __init__(self, settings: ClickHouseSettings, cursor_signer: CursorSigner) -> None:
        self._settings = settings
        self._cursor_signer = cursor_signer
        self._capabilities = query_capabilities(settings)

    @property
    def capabilities(self):  # type: ignore[no-untyped-def]
        return self._capabilities

    def compile(self, plan: object, context: TranslationContext) -> CompiledQuery:
        if not isinstance(plan, PlannedQuery):
            raise TypeError("ClickHouse query translation requires a validated PlannedQuery")
        if plan.binding_id != context.binding_id or plan.fingerprint != context.plan_fingerprint:
            raise ValueError("query plan and TranslationContext identities differ")
        return self._compile_operation(plan.operation, context, empty=plan.empty_result)

    def compile_wire(
        self,
        operation: QueryOperation,
        context: TranslationContext,
    ) -> CompiledQuery:
        """Compile a Query 1.0.0 wire plan already validated by the runtime."""

        return self._compile_operation(operation, context, empty=False)

    def _compile_operation(
        self,
        operation: QueryOperation,
        context: TranslationContext,
        *,
        empty: bool,
    ) -> CompiledQuery:
        if operation.consistency != "eventual":
            raise ValueError("ClickHouse V1 query plans require eventual consistency")
        if operation.joins or operation.traversal is not None or len(operation.targets) != 1:
            raise ValueError(
                "ClickHouse V1 accepts one Resource and does not advertise joins/traversal"
            )
        layout = self._settings.layout_for(operation.targets[0].resource)
        _verify_schema_pin(layout, context)
        if empty:
            return CompiledQuery(
                ADAPTER_ID,
                context.plan_fingerprint,
                _command(
                    "SELECT 1 WHERE 0",
                    (),
                    operation,
                    context,
                    cursor_plan_fingerprint=_cursor_plan_fingerprint(operation),
                    empty=True,
                    sort_count=0,
                ),
                {},
                operation.result.shape,
            )
        compiler = _SQLCompiler(layout, self._settings.database)
        predicate = compiler.expression(operation.filter) if operation.filter is not None else "1"
        _require_bounded_time_range(operation.operation, operation.filter, layout, self._settings)
        scope_parameter = compiler.parameter(
            context.scope_fingerprint.removeprefix("sha256:"), "String"
        )
        where = f"{quote_identifier(HIDDEN_SCOPE)} = {scope_parameter} AND ({predicate})"
        cursor_plan = _cursor_plan_fingerprint(operation)
        sorts = _sorts(operation, layout)
        if operation.page.cursor is not None:
            payload = self._cursor_signer.verify(
                operation.page.cursor,
                expected=CursorExpectations(
                    plan_fingerprint=cursor_plan,
                    schema_fingerprints=context.schema_fingerprints,
                    registry_fingerprint=context.registry_fingerprint,
                    scope_fingerprint=context.scope_fingerprint,
                    page_size=operation.page.size,
                ),
            )
            where += " AND (" + compiler.keyset(sorts, payload.sort_tuple) + ")"
        if operation.operation == "aggregate":
            sql, columns = _aggregate_sql(operation, layout, compiler, where)
            command = _command(
                sql,
                columns,
                operation,
                context,
                cursor_plan_fingerprint=cursor_plan,
                aggregate=True,
                sort_count=0,
            )
        else:
            sql, columns = _record_sql(operation, layout, compiler, where, sorts)
            command = _command(
                sql,
                columns,
                operation,
                context,
                cursor_plan_fingerprint=cursor_plan,
                sort_count=len(sorts),
            )
        result_fields = (
            {item.name: item.name for item in operation.grouping if isinstance(item, Field)}
            if operation.operation == "aggregate"
            else {
                item.alias or item.expression.name: item.expression.name
                for item in operation.result.projection
                if isinstance(item.expression, Field)
            }
            if operation.result.projection
            else {name: name for name in layout.column_map}
        )
        if operation.operation == "aggregate":
            result_fields.update(
                {
                    item.name: item.aggregate.operand.name
                    for item in operation.aggregates
                    if item.aggregate.function in {"min", "max"}
                    and isinstance(item.aggregate.operand, Field)
                }
            )
        command.update(_result_metadata(layout, result_fields, sorts=sorts))
        return CompiledQuery(
            adapter_id=ADAPTER_ID,
            plan_fingerprint=context.plan_fingerprint,
            command=cast(JsonValue, command),
            parameters=compiler.parameters,
            expected_result_shape=operation.result.shape,
        )

    def normalize_result(
        self,
        compiled: CompiledQuery,
        raw_result: object,
    ) -> NormalizedQueryResult:
        command = _as_mapping(compiled.command, "compiled command")
        if command.get("empty") is True:
            return NormalizedQueryResult((), provenance={"pushdown": "complete"})
        columns = getattr(raw_result, "column_names", None)
        rows = getattr(raw_result, "result_rows", None)
        if not isinstance(columns, Sequence) or not isinstance(rows, Sequence):
            raise TypeError("ClickHouse query result does not expose columns and rows")
        column_names = tuple(str(item) for item in columns)
        page_size = cast(int, command["pageSize"])
        has_more = not command.get("aggregate", False) and len(rows) > page_size
        visible_rows = rows[:page_size]
        items: list[JsonValue] = []
        for row in visible_rows:
            mapping = dict(zip(column_names, cast(Sequence[Any], row), strict=True))
            items.append(
                {
                    key: _logical_json_value(value, key, command)
                    for key, value in mapping.items()
                    if not key.startswith(_SORT_ALIAS_PREFIX) and key != _TOTAL_ALIAS
                }
            )
        cursor: str | None = None
        if has_more and visible_rows:
            last = dict(zip(column_names, cast(Sequence[Any], visible_rows[-1]), strict=True))
            sort_count = cast(int, command["sortCount"])
            sort_tuple = tuple(
                _logical_json_value(
                    last[f"{_SORT_ALIAS_PREFIX}{index}"], f"{_SORT_ALIAS_PREFIX}{index}", command
                )
                for index in range(sort_count)
            )
            cursor = self._cursor_signer.issue(
                plan_fingerprint=cast(str, command["cursorPlanFingerprint"]),
                schema_fingerprints=cast(Mapping[str, str], command["schemaFingerprints"]),
                registry_fingerprint=cast(str, command["registryFingerprint"]),
                scope_fingerprint=cast(str, command["scopeFingerprint"]),
                sort_tuple=sort_tuple,
                page_size=page_size,
            )
        return NormalizedQueryResult(
            tuple(items),
            cursor=cursor,
            provenance={
                "cursor": "signed-live-keyset" if cursor is not None else "none",
                "pushdown": "complete",
                "scopePredicate": "first",
            },
        )


def compile_simple_query(
    operation: str,
    input_value: Mapping[str, JsonValue],
    layout: ResourceLayout,
    context: TranslationContext,
    settings: ClickHouseSettings,
    cursor_signer: CursorSigner,
) -> CompiledQuery:
    """Compile the mapping-first Catalog 1.0.0 surface emitted by Semantics."""

    compiler = _SQLCompiler(layout, settings.database)
    where_value = input_value.get("where", {})
    if not isinstance(where_value, Mapping):
        raise TypeError("mapping-first query where must be an object")
    predicate, bounds = compiler.simple_where(where_value)
    scope = compiler.parameter(context.scope_fingerprint.removeprefix("sha256:"), "String")
    where = f"{quote_identifier(HIDDEN_SCOPE)} = {scope} AND ({predicate})"
    if operation in {"query", "aggregate"}:
        _validate_simple_bounds(bounds, layout, settings)
    table = layout.qualified_table(settings.database)
    final = " FINAL" if layout.query_final else ""
    if operation == "aggregate":
        metrics = input_value.get("metrics")
        groups = input_value.get("groupBy", ())
        if not isinstance(metrics, Sequence) or isinstance(metrics, (str, bytes)) or not metrics:
            raise ValueError("mapping-first aggregate requires non-empty metrics")
        group_fields = _field_names(groups, "groupBy")
        selections = [
            f"{quote_identifier(layout.physical_column(name))} AS {quote_identifier(name)}"
            for name in group_fields
        ]
        selections.extend(_simple_metric(metric, layout) for metric in metrics)
        group_sql = (
            " GROUP BY "
            + ", ".join(quote_identifier(layout.physical_column(name)) for name in group_fields)
            if group_fields
            else ""
        )
        sql = f"SELECT {', '.join(selections)} FROM {table}{final} WHERE {where}{group_sql}"
        output_columns = (*group_fields, *(_metric_name(item) for item in metrics))
        shape = "aggregate"
        page_size = 500
    else:
        select_value = input_value.get("select", ())
        selected = (
            _field_names(select_value, "select") if select_value else tuple(layout.column_map)
        )
        for name in selected:
            layout.physical_column(name)
        selections = [
            f"{quote_identifier(layout.physical_column(name))} AS {quote_identifier(name)}"
            for name in selected
        ]
        raw_limit = input_value.get("limit", 1 if operation == "get" else 50)
        if (
            isinstance(raw_limit, bool)
            or not isinstance(raw_limit, int)
            or not 1 <= raw_limit <= 500
        ):
            raise ValueError("mapping-first query limit must be between 1 and 500")
        page_size = raw_limit
        sorts = _simple_sorts(input_value.get("orderBy", ()), layout)
        if not sorts:
            sorts = _default_sorts(layout)
        cursor = input_value.get("cursor")
        stable = sha256_fingerprint(
            cast(
                JsonValue,
                {**dict(input_value), "cursor": None, "resource": layout.resource.to_dict()},
            )
        )
        if cursor is not None:
            if not isinstance(cursor, str):
                raise TypeError("mapping-first cursor must be an opaque string")
            payload = cursor_signer.verify(
                cursor,
                expected=CursorExpectations(
                    stable,
                    context.schema_fingerprints,
                    context.scope_fingerprint,
                    page_size,
                    context.registry_fingerprint,
                ),
            )
            where += " AND (" + compiler.keyset(sorts, payload.sort_tuple) + ")"
        selections.extend(
            f"{expression} AS {quote_identifier(f'{_SORT_ALIAS_PREFIX}{index}')}"
            for index, (expression, _, _) in enumerate(sorts)
        )
        order_sql = _order_sql(sorts)
        sql = (
            f"SELECT {', '.join(selections)} FROM {table}{final} WHERE {where} "
            f"ORDER BY {order_sql} LIMIT {page_size + 1}"
        )
        output_columns = selected
        shape = "records"
    stable_fingerprint = locals().get("stable", context.plan_fingerprint)
    command: dict[str, JsonValue] = {
        "aggregate": operation == "aggregate",
        "columns": list(output_columns),
        "cursorPlanFingerprint": cast(str, stable_fingerprint),
        "empty": False,
        "pageSize": page_size,
        "registryFingerprint": context.registry_fingerprint,
        "schemaFingerprints": dict(context.schema_fingerprints),
        "scopeFingerprint": context.scope_fingerprint,
        "sortCount": 0 if operation == "aggregate" else len(sorts),
        "sql": sql,
    }
    result_fields = {
        name: name for name in (group_fields if operation == "aggregate" else selected)
    }
    if operation == "aggregate":
        for metric in cast(Sequence[JsonValue], metrics):
            if isinstance(metric, Mapping) and metric.get("function") in {"min", "max"}:
                result_fields[cast(str, metric["name"])] = cast(str, metric["field"])
    command.update(
        _result_metadata(layout, result_fields, sorts=() if operation == "aggregate" else sorts)
    )
    return CompiledQuery(ADAPTER_ID, context.plan_fingerprint, command, compiler.parameters, shape)


class _SQLCompiler:
    def __init__(self, layout: ResourceLayout, database: str) -> None:
        self.layout = layout
        self.database = database
        self.parameters: dict[str, JsonValue] = {}

    def parameter(self, value: JsonValue, clickhouse_type: str) -> str:
        name = f"p{len(self.parameters)}"
        self.parameters[name] = _parameter_value(value, clickhouse_type)
        return "{" + name + ":" + _parameter_type(clickhouse_type) + "}"

    def expression(self, expression: ValueExpression | None) -> str:
        if expression is None:
            return "1"
        if isinstance(expression, Field):
            return self.field(expression)
        if isinstance(expression, Literal):
            return self.parameter(expression.value, _literal_type(expression))
        if isinstance(expression, BinaryExpression):
            left = self.expression(expression.left)
            right_type = self._operand_type(expression.left)
            if isinstance(expression.right, Literal):
                right = self.parameter(expression.right.value, right_type)
            else:
                right = self.expression(expression.right)
            operator = {
                "eq": "=",
                "ne": "!=",
                "lt": "<",
                "lte": "<=",
                "gt": ">",
                "gte": ">=",
            }.get(expression.operator)
            if operator is None:
                raise ValueError(f"ClickHouse does not advertise {expression.operator!r}")
            return f"({left} {operator} {right})"
        if isinstance(expression, BooleanExpression):
            operator = " AND " if expression.operator == "and" else " OR "
            return "(" + operator.join(self.expression(item) for item in expression.operands) + ")"
        if isinstance(expression, UnaryExpression):
            if expression.operator != "not":
                raise ValueError("ClickHouse does not advertise arithmetic unary expressions")
            return f"NOT ({self.expression(expression.operand)})"
        if isinstance(expression, NullTest):
            operator = "IS NULL" if expression.is_null else "IS NOT NULL"
            return f"({self.expression(expression.operand)} {operator})"
        if isinstance(expression, MembershipExpression):
            operand_type = self._operand_type(expression.operand)
            values = []
            for item in expression.values:
                if not isinstance(item, Literal):
                    raise ValueError("ClickHouse membership values must be literals")
                values.append(self.parameter(item.value, operand_type))
            operator = "NOT IN" if expression.negated else "IN"
            return f"({self.expression(expression.operand)} {operator} ({', '.join(values)}))"
        if isinstance(expression, TimestampRange):
            field = self.expression(expression.operand)
            field_type = self._operand_type(expression.operand)
            parts: list[str] = []
            if expression.start is not None:
                if not isinstance(expression.start, Literal):
                    raise ValueError("ClickHouse timestamp range boundaries must be literals")
                start = self.parameter(expression.start.value, field_type)
                parts.append(f"{field} {'>=' if expression.include_start else '>'} {start}")
            if expression.end is not None:
                if not isinstance(expression.end, Literal):
                    raise ValueError("ClickHouse timestamp range boundaries must be literals")
                end = self.parameter(expression.end.value, field_type)
                parts.append(f"{field} {'<=' if expression.include_end else '<'} {end}")
            return "(" + " AND ".join(parts) + ")"
        raise ValueError(f"ClickHouse does not advertise expression {type(expression).__name__}")

    def field(self, value: Field) -> str:
        if value.resource is not None and value.resource != self.layout.resource.canonical:
            raise ValueError("ClickHouse field qualifier does not match the query Resource")
        return quote_identifier(self.layout.physical_column(value.name))

    def keyset(
        self,
        sorts: Sequence[tuple[str, str, str]],
        values: Sequence[JsonValue],
    ) -> str:
        if len(sorts) != len(values):
            raise ValueError("cursor sort tuple does not match deterministic query order")
        branches: list[str] = []
        equal: list[str] = []
        for (expression, direction, clickhouse_type), value in zip(sorts, values, strict=True):
            if expression == quote_identifier(HIDDEN_ROW):
                value = _physical_row_fingerprint(value)
            parameter = self.parameter(value, clickhouse_type)
            comparison = ">" if direction == "asc" else "<"
            branches.append(
                "(" + " AND ".join((*equal, f"{expression} {comparison} {parameter}")) + ")"
            )
            equal.append(f"{expression} = {parameter}")
        return " OR ".join(branches)

    def simple_where(
        self,
        where: Mapping[str, JsonValue],
    ) -> tuple[str, Mapping[str, tuple[JsonValue | None, JsonValue | None]]]:
        predicates: list[str] = []
        bounds: dict[str, tuple[JsonValue | None, JsonValue | None]] = {}
        for field_name, value in sorted(where.items()):
            column = self.layout.column_map.get(field_name)
            if column is None:
                raise ValueError(f"mapping-first filter names unknown field {field_name!r}")
            field = quote_identifier(column.physical_name)
            if not isinstance(value, Mapping):
                predicates.append(f"{field} = {self.parameter(value, column.clickhouse_type)}")
                continue
            lower: JsonValue | None = None
            upper: JsonValue | None = None
            for operator, candidate in sorted(value.items()):
                if operator not in {
                    "eq",
                    "ne",
                    "lt",
                    "lte",
                    "gt",
                    "gte",
                    "in",
                    "notIn",
                    "isNull",
                }:
                    raise ValueError(f"mapping-first filter operator {operator!r} is unsupported")
                if operator in {"in", "notIn"}:
                    if not isinstance(candidate, Sequence) or isinstance(candidate, (str, bytes)):
                        raise TypeError(f"mapping-first {operator} filter must be an array")
                    params = [self.parameter(item, column.clickhouse_type) for item in candidate]
                    if not params:
                        raise ValueError(f"mapping-first {operator} filter cannot be empty")
                    keyword = "NOT IN" if operator == "notIn" else "IN"
                    predicates.append(f"{field} {keyword} ({', '.join(params)})")
                elif operator == "isNull":
                    if not isinstance(candidate, bool):
                        raise TypeError("mapping-first isNull filter must be boolean")
                    predicates.append(f"{field} IS {'NULL' if candidate else 'NOT NULL'}")
                else:
                    symbol = {
                        "eq": "=",
                        "ne": "!=",
                        "lt": "<",
                        "lte": "<=",
                        "gt": ">",
                        "gte": ">=",
                    }[operator]
                    parameter = self.parameter(candidate, column.clickhouse_type)
                    predicates.append(f"{field} {symbol} {parameter}")
                    if operator in {"gt", "gte"}:
                        lower = candidate
                    if operator in {"lt", "lte"}:
                        upper = candidate
            bounds[field_name] = (lower, upper)
        return (" AND ".join(predicates) if predicates else "1"), bounds

    def _operand_type(self, value: ValueExpression) -> str:
        if isinstance(value, Field):
            return self.layout.column_map[value.name].clickhouse_type
        if isinstance(value, Literal):
            return _literal_type(value)
        raise ValueError("ClickHouse comparison operands must have a concrete logical type")


def _record_sql(
    operation: QueryOperation,
    layout: ResourceLayout,
    compiler: _SQLCompiler,
    where: str,
    sorts: Sequence[tuple[str, str, str]],
) -> tuple[str, tuple[str, ...]]:
    if operation.result.projection:
        selections: list[str] = []
        columns: list[str] = []
        for projection in operation.result.projection:
            if not isinstance(projection.expression, Field):
                raise ValueError("ClickHouse V1 projection advertises Schema fields only")
            alias = projection.alias or projection.expression.name
            selections.append(
                f"{compiler.field(projection.expression)} AS {quote_identifier(alias)}"
            )
            columns.append(alias)
    else:
        columns = list(layout.column_map)
        selections = [
            f"{quote_identifier(layout.physical_column(name))} AS {quote_identifier(name)}"
            for name in columns
        ]
    selections.extend(
        f"{expression} AS {quote_identifier(f'{_SORT_ALIAS_PREFIX}{index}')}"
        for index, (expression, _, _) in enumerate(sorts)
    )
    if operation.result.include_total:
        selections.append(f"count() OVER () AS {quote_identifier(_TOTAL_ALIAS)}")
    final = " FINAL" if layout.query_final else ""
    table = layout.qualified_table(compiler_database(compiler))
    sql = (
        f"SELECT {', '.join(selections)} FROM {table}{final} "
        f"WHERE {where} ORDER BY {_order_sql(sorts)} LIMIT {operation.page.size + 1}"
    )
    return sql, tuple(columns)


def _aggregate_sql(
    operation: QueryOperation,
    layout: ResourceLayout,
    compiler: _SQLCompiler,
    where: str,
) -> tuple[str, tuple[str, ...]]:
    selections: list[str] = []
    columns: list[str] = []
    group_expressions: list[str] = []
    for _index, expression in enumerate(operation.grouping):
        if not isinstance(expression, Field):
            raise ValueError("ClickHouse V1 grouping advertises Schema fields only")
        selected = compiler.field(expression)
        alias = expression.name
        selections.append(f"{selected} AS {quote_identifier(alias)}")
        group_expressions.append(selected)
        columns.append(alias)
    for named in operation.aggregates:
        selections.append(
            f"{_aggregate_expression(named.aggregate, compiler)} AS {quote_identifier(named.name)}"
        )
        columns.append(named.name)
    final = " FINAL" if layout.query_final else ""
    group_sql = f" GROUP BY {', '.join(group_expressions)}" if group_expressions else ""
    table = layout.qualified_table(compiler_database(compiler))
    sql = f"SELECT {', '.join(selections)} FROM {table}{final} WHERE {where}{group_sql}"
    return sql, tuple(columns)


def compiler_database(compiler: _SQLCompiler) -> str:
    database = getattr(compiler, "database", None)
    if not isinstance(database, str):
        raise RuntimeError("query compiler database was not bound")
    return database


def _aggregate_expression(value: Aggregate, compiler: _SQLCompiler) -> str:
    operand = "*" if value.operand is None else compiler.expression(value.operand)
    if value.function == "count":
        if value.operand is None:
            return "count()"
        return f"uniqExact({operand})" if value.distinct else f"count({operand})"
    if value.distinct:
        raise ValueError("ClickHouse only advertises distinct count")
    if value.function == "percentile":
        return f"quantileExact(0.5)({operand})"
    function = {"avg": "avg", "max": "max", "min": "min", "sum": "sum"}[value.function]
    return f"{function}({operand})"


def _sorts(operation: QueryOperation, layout: ResourceLayout) -> tuple[tuple[str, str, str], ...]:
    result: list[tuple[str, str, str]] = []
    names: set[str] = set()
    for sort in operation.order:
        if not isinstance(sort.expression, Field):
            raise ValueError("ClickHouse deterministic order requires Schema fields")
        column = layout.column_map[sort.expression.name]
        if column.nullable:
            raise ValueError("ClickHouse live-keyset ordering requires non-nullable fields")
        result.append(
            (quote_identifier(column.physical_name), sort.direction, column.clickhouse_type)
        )
        names.add(sort.expression.name)
    for expression, direction, clickhouse_type, logical_name in _default_sort_fields(layout):
        if logical_name not in names:
            result.append((expression, direction, clickhouse_type))
    return tuple(result)


def _default_sorts(layout: ResourceLayout) -> tuple[tuple[str, str, str], ...]:
    return tuple((item[0], item[1], item[2]) for item in _default_sort_fields(layout))


def _default_sort_fields(
    layout: ResourceLayout,
) -> tuple[tuple[str, str, str, str], ...]:
    fields = [
        (
            quote_identifier(layout.physical_column(layout.timestamp_field)),
            "desc",
            layout.column_map[layout.timestamp_field].clickhouse_type,
            layout.timestamp_field,
        )
    ]
    fields.extend(
        (
            quote_identifier(layout.physical_column(name)),
            "asc",
            layout.column_map[name].clickhouse_type,
            name,
        )
        for name in layout.identity_fields
        if name != layout.timestamp_field
    )
    fields.append((quote_identifier(HIDDEN_ROW), "asc", "String", HIDDEN_ROW))
    return tuple(fields)


def _simple_sorts(value: JsonValue, layout: ResourceLayout) -> tuple[tuple[str, str, str], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError("mapping-first orderBy must be an array")
    result: list[tuple[str, str, str]] = []
    for item in value:
        if not isinstance(item, Mapping) or set(item) != {"field", "direction"}:
            raise ValueError("mapping-first orderBy entries require field and direction")
        name = item["field"]
        direction = item["direction"]
        if not isinstance(name, str) or direction not in {"asc", "desc"}:
            raise ValueError("mapping-first orderBy entry is invalid")
        column = layout.column_map.get(name)
        if column is None or column.nullable:
            raise ValueError("mapping-first orderBy requires a known non-nullable field")
        result.append(
            (quote_identifier(column.physical_name), cast(str, direction), column.clickhouse_type)
        )
    existing = {
        item["field"]
        for item in cast(Sequence[Mapping[str, JsonValue]], value)
        if isinstance(item.get("field"), str)
    }
    result.extend(
        (expression, direction, clickhouse_type)
        for expression, direction, clickhouse_type, logical_name in _default_sort_fields(layout)
        if logical_name not in existing
    )
    return tuple(result)


def _order_sql(sorts: Sequence[tuple[str, str, str]]) -> str:
    return ", ".join(f"{expression} {direction.upper()}" for expression, direction, _ in sorts)


def _command(
    sql: str,
    columns: Sequence[str],
    operation: QueryOperation,
    context: TranslationContext,
    *,
    cursor_plan_fingerprint: str,
    aggregate: bool = False,
    empty: bool = False,
    sort_count: int,
) -> dict[str, JsonValue]:
    return {
        "aggregate": aggregate,
        "columns": list(columns),
        "cursorPlanFingerprint": cursor_plan_fingerprint,
        "empty": empty,
        "pageSize": operation.page.size,
        "registryFingerprint": context.registry_fingerprint,
        "schemaFingerprints": dict(context.schema_fingerprints),
        "scopeFingerprint": context.scope_fingerprint,
        "sortCount": sort_count,
        "sql": sql,
    }


def _cursor_plan_fingerprint(operation: QueryOperation) -> str:
    stable = replace(operation, page=replace(operation.page, cursor=None))
    return stable.fingerprint


def _require_bounded_time_range(
    operation: str,
    expression: ValueExpression | None,
    layout: ResourceLayout,
    settings: ClickHouseSettings,
) -> None:
    if operation == "get":
        return
    ranges = _timestamp_ranges(expression, layout.timestamp_field)
    if len(ranges) != 1:
        raise ValueError("ClickHouse scans require exactly one bounded timestamp range")
    start, end = ranges[0]
    _validate_range(start, end, settings.max_time_range_seconds)


def _timestamp_ranges(
    expression: ValueExpression | None,
    field_name: str,
) -> list[tuple[JsonValue, JsonValue]]:
    if expression is None:
        return []
    if isinstance(expression, TimestampRange):
        if (
            isinstance(expression.operand, Field)
            and expression.operand.name == field_name
            and isinstance(expression.start, Literal)
            and isinstance(expression.end, Literal)
        ):
            return [(expression.start.value, expression.end.value)]
        return []
    if isinstance(expression, BooleanExpression):
        return [
            item
            for operand in expression.operands
            for item in _timestamp_ranges(operand, field_name)
        ]
    return []


def _validate_simple_bounds(
    bounds: Mapping[str, tuple[JsonValue | None, JsonValue | None]],
    layout: ResourceLayout,
    settings: ClickHouseSettings,
) -> None:
    start, end = bounds.get(layout.timestamp_field, (None, None))
    if start is None or end is None:
        raise ValueError("ClickHouse scans require bounded timestamp gte/gt and lt/lte filters")
    _validate_range(start, end, settings.max_time_range_seconds)


def _validate_range(start: JsonValue, end: JsonValue, maximum_seconds: int) -> None:
    nanoseconds = timestamp_nanoseconds(end) - timestamp_nanoseconds(start)
    if nanoseconds <= 0 or nanoseconds > maximum_seconds * 1_000_000_000:
        raise ValueError("ClickHouse timestamp range is empty, reversed, or exceeds its limit")


def _verify_schema_pin(layout: ResourceLayout, context: TranslationContext) -> None:
    pinned = context.schema_fingerprints.get(layout.resource.canonical)
    if pinned != layout.schema_fingerprint:
        raise ValueError("query TranslationContext Schema fingerprint differs from the layout")


def _literal_type(value: Literal) -> str:
    logical = value.logical_type
    if isinstance(logical, str):
        kind: object = logical
    elif isinstance(logical, Mapping):
        kind = logical.get("kind")
    else:
        raise TypeError("query literal logical type must be a string or object")
    return {
        "boolean": "Bool",
        "date": "Date32",
        "decimal": "Decimal256(18)",
        "float64": "Float64",
        "int16": "Int16",
        "int32": "Int32",
        "int64": "Int64",
        "int8": "Int8",
        "string": "String",
        "utcTimestamp": "DateTime64(9, 'UTC')",
        "uuid": "UUID",
    }.get(cast(str, kind), "String")


def _physical_row_fingerprint(value: JsonValue) -> str:
    """Recover the stored hex digest from either existing signed cursor encoding.

    FixedString results are bytes with the native driver, so V1 cursors contain
    Base64; string-returning clients issued the raw hex digest. Their lengths
    are disjoint. Decode only this physical column, after cursor verification,
    without changing the wire format or logical binary serialization.
    """
    if isinstance(value, str):
        if re.fullmatch(r"[0-9a-f]{64}", value):
            return value
        if len(value) == 88 and value.isascii():
            try:
                decoded = base64.b64decode(value, validate=True)
                fingerprint = decoded.decode("ascii")
            except (binascii.Error, UnicodeDecodeError):
                pass
            else:
                if (
                    re.fullmatch(r"[0-9a-f]{64}", fingerprint)
                    and base64.b64encode(decoded).decode("ascii") == value
                ):
                    return fingerprint
    raise ValueError("cursor row fingerprint is not a canonical stored SHA-256 digest")


def _parameter_type(value: str) -> str:
    result = value
    while result.startswith("Nullable(") and result.endswith(")"):
        result = result[9:-1]
    if result.startswith("LowCardinality(") and result.endswith(")"):
        result = result[15:-1]
    return result


def _parameter_value(value: JsonValue, clickhouse_type: str) -> JsonValue:
    selected_type = _parameter_type(clickhouse_type)
    if selected_type.startswith("DateTime") and isinstance(value, str):
        return timestamp_text(timestamp_nanoseconds(value), parameter=True)
    return value


def _simple_metric(value: object, layout: ResourceLayout) -> str:
    if not isinstance(value, Mapping):
        raise TypeError("mapping-first metric must be an object")
    allowed = {"name", "function", "field", "distinct", "quantile"}
    if set(value) - allowed or not {"name", "function"} <= set(value):
        raise ValueError("mapping-first metric contains unknown or missing fields")
    name = value["name"]
    function = value["function"]
    field_name = value.get("field")
    if not isinstance(name, str) or function not in {
        "count",
        "sum",
        "avg",
        "min",
        "max",
        "quantile",
    }:
        raise ValueError("mapping-first metric name or function is invalid")
    if function == "count" and field_name is None:
        expression = "count()"
    else:
        if not isinstance(field_name, str):
            raise ValueError(f"mapping-first {function} metric requires a field")
        field = quote_identifier(layout.physical_column(field_name))
        if function == "count" and value.get("distinct") is True:
            expression = f"uniqExact({field})"
        elif function == "quantile":
            quantile = value.get("quantile")
            if isinstance(quantile, bool) or not isinstance(quantile, (int, float)):
                raise ValueError("mapping-first quantile metric requires a numeric quantile")
            selected = float(quantile)
            if not math.isfinite(selected) or not 0 < selected < 1:
                raise ValueError("mapping-first quantile must be between zero and one")
            expression = f"quantileExact({format(selected, '.15g')})({field})"
        else:
            expression = f"{function}({field})"
    return f"{expression} AS {quote_identifier(name)}"


def _metric_name(value: object) -> str:
    if not isinstance(value, Mapping) or not isinstance(value.get("name"), str):
        raise ValueError("mapping-first metric requires a name")
    return cast(str, value["name"])


def _field_names(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"mapping-first {name} must be an array")
    result = tuple(value)
    if any(not isinstance(item, str) or not item for item in result):
        raise TypeError(f"mapping-first {name} entries must be field names")
    return cast(tuple[str, ...], result)


def _result_metadata(
    layout: ResourceLayout,
    fields: Mapping[str, str],
    *,
    sorts: Sequence[tuple[str, str, str]] = (),
) -> dict[str, JsonValue]:
    formats: dict[str, JsonValue] = {}
    json_fields: list[JsonValue] = []
    timestamps: list[JsonValue] = []
    for alias, name in fields.items():
        logical = layout.column_map[name].logical_type
        kind = logical.get("kind") if isinstance(logical, Mapping) else logical
        if kind == "utcTimestamp":
            formats[alias] = "int"
            timestamps.append(alias)
        elif kind == "bytes":
            formats[alias] = "bytes"
        elif kind in {"json", "objectRef", "recordRef"}:
            json_fields.append(alias)
    for index, (_, _, column_type) in enumerate(sorts):
        if _parameter_type(column_type).startswith("DateTime64(9"):
            alias = f"{_SORT_ALIAS_PREFIX}{index}"
            formats[alias] = "int"
            timestamps.append(alias)
    return {"columnFormats": formats, "jsonColumns": json_fields, "timestampColumns": timestamps}


def _logical_json_value(value: Any, name: str, command: Mapping[str, JsonValue]) -> JsonValue:
    if name in cast(Sequence[str], command.get("timestampColumns", ())):
        if isinstance(value, int) and not isinstance(value, bool):
            return timestamp_text(value)
        if isinstance(value, (list, tuple)):
            return [_logical_json_value(item, name, command) for item in value]
    if name in cast(Sequence[str], command.get("jsonColumns", ())):
        if isinstance(value, str):
            return cast(JsonValue, json.loads(value))
        if isinstance(value, (list, tuple)):
            return [_logical_json_value(item, name, command) for item in value]
    return _json_value(value)


def _json_value(value: Any) -> JsonValue:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("ClickHouse returned a non-finite value")
        return value
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if hasattr(value, "isoformat"):
        return cast(str, value.isoformat())
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, Sequence):
        return [_json_value(item) for item in value]
    return str(value)


def _as_mapping(value: JsonValue, name: str) -> Mapping[str, JsonValue]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    canonical_json_bytes(value)
    return value


__all__ = ["ClickHouseQueryTranslator", "compile_simple_query"]
