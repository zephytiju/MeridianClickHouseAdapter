# SPDX-License-Identifier: Apache-2.0
"""Deterministic Meridian Schema to ClickHouse DDL compilation."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from typing import cast

from meridian_storage.semantics import (
    Cardinality,
    FieldDefinition,
    LogicalKind,
    LogicalType,
    SchemaDocument,
    TimeSeriesProfile,
)

from meridian_storage import ResourceRef

from .._canonical import physical_identifier, quote_identifier, require_fingerprint
from .layout import (
    HIDDEN_BATCH,
    HIDDEN_INGESTED,
    HIDDEN_RESOURCE,
    HIDDEN_ROW,
    HIDDEN_SCHEMA,
    HIDDEN_SCOPE,
    HIDDEN_TENANT,
    ColumnLayout,
    RecordProfile,
    ResourceLayout,
    Topology,
)

METADATA_TABLE = "_meridian_resources"
MAX_DECIMAL_PRECISION = 76

_SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9_]+")


@dataclass(frozen=True, slots=True)
class SchemaCompilation:
    layout: ResourceLayout
    metadata_table_sql: str
    create_table_sql: str

    @property
    def statements(self) -> tuple[str, ...]:
        return (self.metadata_table_sql, self.create_table_sql)


class ClickHouseSchemaCompiler:
    """Compile reviewed logical Schemas without executing deployment lifecycle actions."""

    def compile(
        self,
        *,
        database: str,
        resource: ResourceRef,
        resource_fingerprint: str,
        schema: SchemaDocument,
        record_profile: RecordProfile | str,
        topology: Topology | str = Topology.STANDALONE,
        timestamp_field: str | None = None,
        identity_fields: tuple[str, ...] | None = None,
        dimension_fields: tuple[str, ...] | None = None,
        measurement_fields: tuple[str, ...] | None = None,
        retention_seconds: int | None = None,
        partition_interval: str = "month",
        administrative_profiles: tuple[str, ...] = (),
        query_final: bool = True,
    ) -> SchemaCompilation:
        database = physical_identifier(database, "database")
        resource = ResourceRef.parse(resource)
        if resource.catalog not in {"structured", "evidence"}:
            raise ValueError("ClickHouse layouts belong to structured or evidence Resources")
        schema_ref = schema.ref.to_core()
        if (schema_ref.catalog, schema_ref.namespace, schema_ref.logical_name) != (
            resource.catalog,
            resource.namespace,
            resource.logical_name,
        ):
            raise ValueError("Schema and Resource must share Catalog, Namespace, and logical name")
        if schema.consistency != "eventual":
            raise ValueError("ClickHouse Resources must declare eventual consistency")
        resource_fp = cast(
            str,
            require_fingerprint(resource_fingerprint, "Resource fingerprint"),
        )
        profile = RecordProfile(record_profile)
        selected_topology = Topology(topology)
        table = physical_name(resource.logical_name, prefix="m")
        columns = tuple(self.compile_column(item) for item in schema.fields)
        logical_names = {item.logical_name for item in columns}
        time_profile = schema.profile
        if isinstance(time_profile, TimeSeriesProfile):
            selected_timestamp = (
                timestamp_field if timestamp_field is not None else time_profile.timestamp_field
            )
            selected_identities = (
                identity_fields if identity_fields is not None else time_profile.series_identity
            )
            selected_dimensions = (
                dimension_fields if dimension_fields is not None else time_profile.dimensions
            )
            selected_measurements = (
                measurement_fields if measurement_fields is not None else time_profile.measurements
            )
        else:
            selected_timestamp = (
                timestamp_field if timestamp_field is not None else _timestamp_field(schema)
            )
            selected_identities = (
                identity_fields if identity_fields is not None else schema.identity
            )
            selected_dimensions = dimension_fields if dimension_fields is not None else ()
            selected_measurements = measurement_fields if measurement_fields is not None else ()
        for name in (
            selected_timestamp,
            *selected_identities,
            *selected_dimensions,
            *selected_measurements,
        ):
            if name not in logical_names:
                raise ValueError(f"layout role references unknown Schema field {name!r}")
        if not selected_identities:
            raise ValueError("ClickHouse append layouts require stable Schema identity fields")
        indexes = {
            physical_name(index.name, prefix="idx"): index.fields
            for index in schema.indexes
            if index.kind in {"btree", "hash", "time-series"}
        }
        layout = ResourceLayout(
            resource=resource,
            table=table,
            record_profile=profile,
            schema_version=cast(str, schema.ref.version),
            resource_fingerprint=resource_fp,
            schema_fingerprint=schema.fingerprint,
            columns=columns,
            timestamp_field=selected_timestamp,
            identity_fields=tuple(selected_identities),
            dimension_fields=tuple(selected_dimensions),
            measurement_fields=tuple(selected_measurements),
            retention_seconds=retention_seconds,
            partition_interval=partition_interval,
            topology=selected_topology,
            query_final=query_final,
            append_only=resource.catalog == "evidence",
            administrative_profiles=administrative_profiles,
            indexes=indexes,
        )
        return SchemaCompilation(
            layout=layout,
            metadata_table_sql=metadata_table_ddl(database, selected_topology),
            create_table_sql=create_table_ddl(database, layout),
        )

    def compile_column(self, field: FieldDefinition) -> ColumnLayout:
        many = field.cardinality is Cardinality.MANY
        clickhouse_type = logical_type_to_clickhouse(field.logical_type)
        if many:
            element = f"Nullable({clickhouse_type})" if field.nullable else clickhouse_type
            clickhouse_type = f"Array({element})"
        elif field.nullable:
            clickhouse_type = f"Nullable({clickhouse_type})"
        return ColumnLayout(
            logical_name=field.name,
            physical_name=physical_name(field.name, prefix="c"),
            logical_type=field.logical_type.to_wire(),
            clickhouse_type=clickhouse_type,
            nullable=field.nullable,
            many=many,
        )


def logical_type_to_clickhouse(logical_type: LogicalType) -> str:
    logical_type = LogicalType.parse(logical_type)
    kind = logical_type.kind
    primitive = {
        LogicalKind.BOOLEAN: "Bool",
        LogicalKind.INT8: "Int8",
        LogicalKind.INT16: "Int16",
        LogicalKind.INT32: "Int32",
        LogicalKind.INT64: "Int64",
        LogicalKind.FLOAT64: "Float64",
        LogicalKind.STRING: "String",
        LogicalKind.BYTES: "String",
        LogicalKind.UUID: "UUID",
        LogicalKind.UTC_TIMESTAMP: "DateTime64(9, 'UTC')",
        LogicalKind.DATE: "Date32",
        LogicalKind.DURATION: "Int64",
        LogicalKind.ENUM: "LowCardinality(String)",
        LogicalKind.JSON: "String",
        LogicalKind.RECORD_REF: "String",
        LogicalKind.OBJECT_REF: "String",
        LogicalKind.WGS84_POINT: "Tuple(Float64, Float64)",
    }
    if kind is LogicalKind.DECIMAL:
        precision = cast(int, logical_type.precision)
        scale = cast(int, logical_type.scale)
        if precision > MAX_DECIMAL_PRECISION:
            raise ValueError(
                f"ClickHouse Decimal supports precision at most {MAX_DECIMAL_PRECISION}"
            )
        return f"Decimal({precision}, {scale})"
    return primitive[kind]


def physical_name(value: str, *, prefix: str) -> str:
    """Create a stable bounded physical name without exposing it as logical identity."""

    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode()
    stem = _SAFE_CHARS_RE.sub("_", normalized).strip("_").lower() or prefix
    if stem[0].isdigit():
        stem = f"{prefix}_{stem}"
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]
    return physical_identifier(f"{stem[:100]}_{digest}", "generated physical name")


def metadata_table_ddl(database: str, topology: Topology) -> str:
    database = physical_identifier(database, "database")
    table = f"{quote_identifier(database)}.{quote_identifier(METADATA_TABLE)}"
    if topology is Topology.REPLICATED:
        engine = (
            "ReplicatedReplacingMergeTree("
            f"'/clickhouse/tables/{{shard}}/{database}/{METADATA_TABLE}', "
            "'{replica}', updated_at)"
        )
    else:
        engine = "ReplacingMergeTree(updated_at)"
    return "\n".join(
        (
            f"CREATE TABLE IF NOT EXISTS {table} (",
            "    resource_ref String,",
            "    resource_fingerprint FixedString(71),",
            "    schema_fingerprint FixedString(71),",
            "    profile LowCardinality(String),",
            "    table_name String,",
            "    layout_fingerprint FixedString(71),",
            "    schema_version String,",
            "    updated_at DateTime64(6, 'UTC')",
            ")",
            f"ENGINE = {engine}",
            "ORDER BY resource_ref",
        )
    )


def create_table_ddl(database: str, layout: ResourceLayout) -> str:
    table = layout.qualified_table(database)
    timestamp = quote_identifier(layout.physical_column(layout.timestamp_field))
    user_lines = [
        f"    {quote_identifier(column.physical_name)} {column.clickhouse_type}"
        for column in layout.columns
    ]
    hidden_lines = [
        f"    {quote_identifier(HIDDEN_SCOPE)} FixedString(64)",
        f"    {quote_identifier(HIDDEN_TENANT)} String",
        f"    {quote_identifier(HIDDEN_BATCH)} String",
        f"    {quote_identifier(HIDDEN_ROW)} FixedString(64)",
        f"    {quote_identifier(HIDDEN_RESOURCE)} LowCardinality(String)",
        f"    {quote_identifier(HIDDEN_SCHEMA)} String",
        f"    {quote_identifier(HIDDEN_INGESTED)} DateTime64(9, 'UTC')",
    ]
    index_lines = [
        "    INDEX "
        + quote_identifier(name)
        + " ("
        + ", ".join(quote_identifier(layout.physical_column(field)) for field in fields)
        + ") TYPE minmax GRANULARITY 1"
        for name, fields in layout.indexes.items()
    ]
    definitions = [*hidden_lines, *user_lines, *index_lines]
    definitions_with_commas = [
        line + ("," if index < len(definitions) - 1 else "")
        for index, line in enumerate(definitions)
    ]
    if layout.topology is Topology.REPLICATED:
        path = (
            f"/clickhouse/tables/{{shard}}/"
            f"{physical_identifier(database, 'database')}/{layout.table}"
        )
        engine = (
            "ReplicatedReplacingMergeTree("
            f"'{path}', "
            f"'{{replica}}', {quote_identifier(HIDDEN_INGESTED)})"
        )
    else:
        engine = f"ReplacingMergeTree({quote_identifier(HIDDEN_INGESTED)})"
    partition = {
        "hour": f"toStartOfHour({timestamp})",
        "day": f"toDate({timestamp})",
        "month": f"toYYYYMM({timestamp})",
    }[layout.partition_interval]
    order = [
        quote_identifier(HIDDEN_SCOPE),
        timestamp,
        *(quote_identifier(layout.physical_column(item)) for item in layout.identity_fields),
    ]
    if layout.append_only:
        order.append(quote_identifier(HIDDEN_ROW))
    statements = [
        f"CREATE TABLE IF NOT EXISTS {table} (",
        *definitions_with_commas,
        ")",
        f"ENGINE = {engine}",
        f"PARTITION BY {partition}",
        f"ORDER BY ({', '.join(order)})",
    ]
    if layout.retention_seconds is not None:
        statements.append(
            f"TTL toDateTime({timestamp}) + toIntervalSecond({layout.retention_seconds}) DELETE"
        )
    statements.append("SETTINGS index_granularity = 8192")
    return "\n".join(statements)


def _timestamp_field(schema: SchemaDocument) -> str:
    candidates = tuple(
        field.name
        for field in schema.fields
        if field.logical_type.kind is LogicalKind.UTC_TIMESTAMP
    )
    if len(candidates) != 1:
        raise ValueError("non-time-series Schema compilation requires one explicit timestampField")
    return candidates[0]


__all__ = [
    "MAX_DECIMAL_PRECISION",
    "METADATA_TABLE",
    "ClickHouseSchemaCompiler",
    "SchemaCompilation",
    "create_table_ddl",
    "logical_type_to_clickhouse",
    "metadata_table_ddl",
    "physical_name",
]
