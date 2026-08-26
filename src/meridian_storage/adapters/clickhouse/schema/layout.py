# SPDX-License-Identifier: Apache-2.0
"""Immutable logical-to-physical layouts rendered by deployment migration jobs."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType
from typing import cast

from meridian_storage.semantics import JsonValue, canonical_json_bytes

from meridian_storage import ResourceRef

from .._canonical import fingerprint, physical_identifier, quote_identifier, require_fingerprint

HIDDEN_SCOPE = "_meridian_scope_fingerprint"
HIDDEN_TENANT = "_meridian_tenant"
HIDDEN_BATCH = "_meridian_batch_id"
HIDDEN_ROW = "_meridian_row_fingerprint"
HIDDEN_RESOURCE = "_meridian_resource"
HIDDEN_SCHEMA = "_meridian_schema_version"
HIDDEN_INGESTED = "_meridian_ingested_at"
HIDDEN_COLUMNS = (
    HIDDEN_SCOPE,
    HIDDEN_TENANT,
    HIDDEN_BATCH,
    HIDDEN_ROW,
    HIDDEN_RESOURCE,
    HIDDEN_SCHEMA,
    HIDDEN_INGESTED,
)

_COLUMN_PRIMITIVES = frozenset(
    {
        "Bool",
        "Date32",
        "DateTime64(9, 'UTC')",
        "Float64",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "String",
        "Tuple(Float64, Float64)",
        "UUID",
    }
)
_DECIMAL_RE = re.compile(r"^Decimal\(([1-9][0-9]?), ([0-9]{1,2})\)$")


class RecordProfile(StrEnum):
    LOG = "log"
    SPAN = "span"
    METRIC = "metric"
    USAGE = "usage"
    COST = "cost"
    TIME_SERIES = "time-series"
    ANALYTICAL = "analytical"


class Topology(StrEnum):
    STANDALONE = "clickhouse-standalone"
    REPLICATED = "clickhouse-replicated"


@dataclass(frozen=True, slots=True)
class ColumnLayout:
    logical_name: str
    physical_name: str
    logical_type: JsonValue
    clickhouse_type: str
    nullable: bool = False
    many: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.logical_name, str) or not self.logical_name:
            raise ValueError("logical column name must be non-empty")
        if self.logical_name.startswith("_meridian_"):
            raise ValueError("logical column names cannot use the reserved _meridian_ prefix")
        object.__setattr__(
            self,
            "physical_name",
            physical_identifier(self.physical_name, "physical column name"),
        )
        if not isinstance(self.clickhouse_type, str) or not _valid_column_type(
            self.clickhouse_type
        ):
            raise ValueError("ClickHouse column type is outside the closed V1 type grammar")
        if not isinstance(self.nullable, bool) or not isinstance(self.many, bool):
            raise TypeError("column nullable and many flags must be booleans")
        canonical_json_bytes(self.logical_type)

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "logicalName": self.logical_name,
            "physicalName": self.physical_name,
            "logicalType": self.logical_type,
            "clickhouseType": self.clickhouse_type,
            "nullable": self.nullable,
            "many": self.many,
        }

    @classmethod
    def from_mapping(cls, value: object) -> ColumnLayout:
        if not isinstance(value, Mapping):
            raise TypeError("column layout must be an object")
        required = {
            "logicalName",
            "physicalName",
            "logicalType",
            "clickhouseType",
            "nullable",
            "many",
        }
        if set(value) != required:
            raise ValueError("column layout contains unknown or missing fields")
        return cls(
            logical_name=cast(str, value["logicalName"]),
            physical_name=cast(str, value["physicalName"]),
            logical_type=cast(JsonValue, value["logicalType"]),
            clickhouse_type=cast(str, value["clickhouseType"]),
            nullable=cast(bool, value["nullable"]),
            many=cast(bool, value["many"]),
        )


@dataclass(frozen=True, slots=True)
class ResourceLayout:
    resource: ResourceRef
    table: str
    record_profile: RecordProfile
    schema_version: str
    resource_fingerprint: str
    schema_fingerprint: str
    columns: tuple[ColumnLayout, ...]
    timestamp_field: str
    identity_fields: tuple[str, ...]
    dimension_fields: tuple[str, ...] = ()
    measurement_fields: tuple[str, ...] = ()
    retention_seconds: int | None = None
    partition_interval: str = "month"
    topology: Topology = Topology.STANDALONE
    query_final: bool = True
    administrative_profiles: tuple[str, ...] = ()
    indexes: Mapping[str, tuple[str, ...]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        resource = ResourceRef.parse(self.resource)
        table = physical_identifier(self.table, "physical table name")
        profile = RecordProfile(self.record_profile)
        topology = Topology(self.topology)
        resource_fp = cast(
            str, require_fingerprint(self.resource_fingerprint, "Resource fingerprint")
        )
        schema_fp = cast(str, require_fingerprint(self.schema_fingerprint, "Schema fingerprint"))
        columns = tuple(sorted(self.columns, key=lambda item: item.logical_name))
        if not columns or len({item.logical_name for item in columns}) != len(columns):
            raise ValueError("Resource layout columns must be non-empty and unique")
        if len({item.physical_name for item in columns}) != len(columns):
            raise ValueError("Resource layout physical column names must be unique")
        column_names = {item.logical_name for item in columns}
        timestamp = _member(self.timestamp_field, column_names, "timestamp field")
        identities = _members(
            self.identity_fields, column_names, "identity fields", allow_empty=False
        )
        dimensions = _members(self.dimension_fields, column_names, "dimension fields")
        measurements = _members(self.measurement_fields, column_names, "measurement fields")
        if len({timestamp, *identities, *dimensions, *measurements}) != (
            1 + len(identities) + len(dimensions) + len(measurements)
        ):
            raise ValueError(
                "timestamp, identity, dimension, and measurement roles must be disjoint"
            )
        if self.retention_seconds is not None and (
            isinstance(self.retention_seconds, bool) or self.retention_seconds <= 0
        ):
            raise ValueError("retention seconds must be a positive integer")
        if self.partition_interval not in {"hour", "day", "month"}:
            raise ValueError("partition interval must be hour, day, or month")
        if not isinstance(self.schema_version, str) or not self.schema_version:
            raise ValueError("schema version must be non-empty")
        if not isinstance(self.query_final, bool):
            raise TypeError("queryFinal must be boolean")
        admin = tuple(sorted(set(self.administrative_profiles)))
        if len(admin) != len(self.administrative_profiles) or any(not item for item in admin):
            raise ValueError("administrative profiles must be non-empty and unique")
        indexes: dict[str, tuple[str, ...]] = {}
        for name, fields in sorted(self.indexes.items()):
            physical_identifier(name, "physical index name")
            indexes[name] = _members(
                fields, column_names, f"index {name!r} fields", allow_empty=False
            )
        object.__setattr__(self, "resource", resource)
        object.__setattr__(self, "table", table)
        object.__setattr__(self, "record_profile", profile)
        object.__setattr__(self, "topology", topology)
        object.__setattr__(self, "resource_fingerprint", resource_fp)
        object.__setattr__(self, "schema_fingerprint", schema_fp)
        object.__setattr__(self, "columns", columns)
        object.__setattr__(self, "timestamp_field", timestamp)
        object.__setattr__(self, "identity_fields", identities)
        object.__setattr__(self, "dimension_fields", dimensions)
        object.__setattr__(self, "measurement_fields", measurements)
        object.__setattr__(self, "administrative_profiles", admin)
        object.__setattr__(self, "indexes", MappingProxyType(indexes))

    @property
    def column_map(self) -> Mapping[str, ColumnLayout]:
        return MappingProxyType({item.logical_name: item for item in self.columns})

    @property
    def physical_column_map(self) -> Mapping[str, str]:
        return MappingProxyType({item.logical_name: item.physical_name for item in self.columns})

    @property
    def order_fields(self) -> tuple[str, ...]:
        return (HIDDEN_SCOPE, self.timestamp_field, *self.identity_fields)

    @property
    def layout_fingerprint(self) -> str:
        return fingerprint(cast(JsonValue, self.to_dict(include_fingerprint=False)))

    @property
    def insert_columns(self) -> tuple[str, ...]:
        return (*HIDDEN_COLUMNS, *(item.physical_name for item in self.columns))

    def qualified_table(self, database: str) -> str:
        database_name = quote_identifier(physical_identifier(database, "database"))
        return f"{database_name}.{quote_identifier(self.table)}"

    def physical_column(self, logical_name: str) -> str:
        try:
            return self.column_map[logical_name].physical_name
        except KeyError as exc:
            raise ValueError(f"unknown logical field {logical_name!r}") from exc

    def to_dict(self, *, include_fingerprint: bool = True) -> dict[str, JsonValue]:
        result: dict[str, JsonValue] = {
            "resource": self.resource.to_dict(),
            "table": self.table,
            "recordProfile": self.record_profile.value,
            "schemaVersion": self.schema_version,
            "resourceFingerprint": self.resource_fingerprint,
            "schemaFingerprint": self.schema_fingerprint,
            "columns": [item.to_dict() for item in self.columns],
            "timestampField": self.timestamp_field,
            "identityFields": list(self.identity_fields),
            "dimensionFields": list(self.dimension_fields),
            "measurementFields": list(self.measurement_fields),
            "retentionSeconds": self.retention_seconds,
            "partitionInterval": self.partition_interval,
            "topology": self.topology.value,
            "queryFinal": self.query_final,
            "administrativeProfiles": list(self.administrative_profiles),
            "indexes": {name: list(fields) for name, fields in self.indexes.items()},
        }
        if include_fingerprint:
            result["layoutFingerprint"] = self.layout_fingerprint
        return result

    @classmethod
    def from_mapping(cls, value: object) -> ResourceLayout:
        if not isinstance(value, Mapping):
            raise TypeError("Resource layout must be an object")
        required = {
            "resource",
            "table",
            "recordProfile",
            "schemaVersion",
            "resourceFingerprint",
            "schemaFingerprint",
            "columns",
            "timestampField",
            "identityFields",
            "dimensionFields",
            "measurementFields",
            "retentionSeconds",
            "partitionInterval",
            "topology",
            "queryFinal",
            "administrativeProfiles",
            "indexes",
            "layoutFingerprint",
        }
        if set(value) != required:
            raise ValueError("Resource layout contains unknown or missing fields")
        resource = value["resource"]
        columns = value["columns"]
        indexes = value["indexes"]
        if not isinstance(resource, Mapping):
            raise TypeError("Resource layout reference must be an object")
        if not isinstance(columns, Sequence) or isinstance(columns, (str, bytes)):
            raise TypeError("Resource layout columns must be an array")
        if not isinstance(indexes, Mapping):
            raise TypeError("Resource layout indexes must be an object")
        result = cls(
            resource=ResourceRef.parse(resource),
            table=cast(str, value["table"]),
            record_profile=RecordProfile(cast(str, value["recordProfile"])),
            schema_version=cast(str, value["schemaVersion"]),
            resource_fingerprint=cast(str, value["resourceFingerprint"]),
            schema_fingerprint=cast(str, value["schemaFingerprint"]),
            columns=tuple(ColumnLayout.from_mapping(item) for item in columns),
            timestamp_field=cast(str, value["timestampField"]),
            identity_fields=_strings(value["identityFields"], "identityFields"),
            dimension_fields=_strings(value["dimensionFields"], "dimensionFields"),
            measurement_fields=_strings(value["measurementFields"], "measurementFields"),
            retention_seconds=cast(int | None, value["retentionSeconds"]),
            partition_interval=cast(str, value["partitionInterval"]),
            topology=Topology(cast(str, value["topology"])),
            query_final=cast(bool, value["queryFinal"]),
            administrative_profiles=_strings(
                value["administrativeProfiles"], "administrativeProfiles"
            ),
            indexes={
                cast(str, name): _strings(fields, f"indexes.{name}")
                for name, fields in indexes.items()
            },
        )
        declared = require_fingerprint(value["layoutFingerprint"], "layout fingerprint")
        if declared != result.layout_fingerprint:
            raise ValueError("declared layout fingerprint does not match canonical content")
        return result


def _member(value: object, allowed: set[str], name: str) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{name} must name one layout column")
    return value


def _members(
    values: Sequence[str],
    allowed: set[str],
    name: str,
    *,
    allow_empty: bool = True,
) -> tuple[str, ...]:
    result = tuple(values)
    if (not allow_empty and not result) or len(set(result)) != len(result):
        raise ValueError(f"{name} must be {'non-empty and ' if not allow_empty else ''}unique")
    for item in result:
        _member(item, allowed, name)
    return result


def _strings(value: object, name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise TypeError(f"{name} must be an array")
    if any(not isinstance(item, str) for item in value):
        raise TypeError(f"{name} entries must be strings")
    return tuple(cast(Sequence[str], value))


def _valid_column_type(value: str) -> bool:
    if value in _COLUMN_PRIMITIVES or value == "LowCardinality(String)":
        return True
    decimal = _DECIMAL_RE.fullmatch(value)
    if decimal is not None:
        precision, scale = (int(item) for item in decimal.groups())
        return precision <= 76 and scale <= precision
    for wrapper in ("Array", "Nullable"):
        prefix = wrapper + "("
        if value.startswith(prefix) and value.endswith(")"):
            return _valid_column_type(value[len(prefix) : -1])
    return False


__all__ = [
    "HIDDEN_BATCH",
    "HIDDEN_COLUMNS",
    "HIDDEN_INGESTED",
    "HIDDEN_RESOURCE",
    "HIDDEN_ROW",
    "HIDDEN_SCHEMA",
    "HIDDEN_SCOPE",
    "HIDDEN_TENANT",
    "ColumnLayout",
    "RecordProfile",
    "ResourceLayout",
    "Topology",
]
