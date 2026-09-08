# SPDX-License-Identifier: Apache-2.0
"""Deterministic bounded writes with retry-window deduplication."""

from __future__ import annotations

import base64
import hashlib
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, cast
from uuid import UUID

from meridian_storage.errors import ErrorCode, ValidationError
from meridian_storage.semantics import JsonValue, canonical_json_bytes
from meridian_storage.spi import ExecutionRequest

from .._canonical import scope_digest
from .._timestamps import timestamp_nanoseconds
from ..client import ClickHouseClient
from ..configuration import ClickHouseSettings
from ..schema import ResourceLayout


@dataclass(frozen=True, slots=True)
class PreparedBatch:
    layout: ResourceLayout
    batch_id: str
    rows: tuple[tuple[Any, ...], ...]
    encoded_bytes: int


@dataclass(frozen=True, slots=True)
class BatchReceipt:
    batch_id: str
    accepted_rows: int
    visibility: str = "eventual"

    def to_dict(self) -> dict[str, JsonValue]:
        return {
            "acceptedRows": self.accepted_rows,
            "batchId": self.batch_id,
            "visibility": self.visibility,
        }


class BatchExecutor:
    def __init__(self, client: ClickHouseClient, settings: ClickHouseSettings) -> None:
        self._client = client
        self._settings = settings

    def execute(self, batch: PreparedBatch) -> BatchReceipt:
        insert_settings: dict[str, Any] = {
            "insert_deduplication_token": batch.batch_id,
        }
        if self._settings.topology.value == "clickhouse-replicated":
            insert_settings["insert_quorum"] = self._settings.insert_quorum
        self._client.insert(
            table=batch.layout.table,
            data=batch.rows,
            column_names=batch.layout.insert_columns,
            database=self._settings.database,
            settings=insert_settings,
        )
        return BatchReceipt(batch.batch_id, len(batch.rows))


def prepare_batch(
    request: ExecutionRequest,
    layout: ResourceLayout,
    records: object,
    settings: ClickHouseSettings,
    *,
    now: datetime | None = None,
) -> PreparedBatch:
    values = _records(records)
    if not values:
        _invalid("ClickHouse append batch cannot be empty", request)
    if len(values) > settings.max_batch_rows:
        _invalid("ClickHouse append batch exceeds the advertised row limit", request)
    encoded_bytes = len(canonical_json_bytes(cast(JsonValue, values)))
    if encoded_bytes > settings.max_batch_bytes:
        _invalid("ClickHouse append batch exceeds the advertised byte limit", request)
    batch_id = _batch_id(request, layout)
    ingested_at = (now or datetime.now(UTC)).astimezone(UTC)
    prepared = tuple(
        _prepare_row(request, layout, record, batch_id, ingested_at) for record in values
    )
    return PreparedBatch(layout, batch_id, prepared, encoded_bytes)


def _records(value: object) -> tuple[Mapping[str, JsonValue], ...]:
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("ClickHouse write record keys must be strings")
        return (cast(Mapping[str, JsonValue], value),)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise TypeError("ClickHouse write data must be a record or an array of records")
    result: list[Mapping[str, JsonValue]] = []
    for item in value:
        if not isinstance(item, Mapping) or any(not isinstance(key, str) for key in item):
            raise TypeError("ClickHouse write batch entries must be record objects")
        result.append(cast(Mapping[str, JsonValue], item))
    return tuple(result)


def _prepare_row(
    request: ExecutionRequest,
    layout: ResourceLayout,
    record: Mapping[str, JsonValue],
    batch_id: str,
    ingested_at: datetime,
) -> tuple[Any, ...]:
    expected = set(layout.column_map)
    unknown = set(record) - expected
    if unknown:
        _invalid(f"record contains unknown Schema fields: {sorted(unknown)!r}", request)
    logical_values: list[Any] = []
    for column in layout.columns:
        if column.logical_name not in record:
            if not column.nullable:
                _invalid(f"record is missing required field {column.logical_name!r}", request)
            raw: JsonValue = None
        else:
            raw = record[column.logical_name]
        if raw is None and not column.nullable:
            _invalid(f"record field {column.logical_name!r} cannot be null", request)
        logical_values.append(_coerce(raw, column.logical_type, many=column.many))
    canonical_record = {name: record.get(name) for name in sorted(expected)}
    row_fingerprint = hashlib.sha256(canonical_json_bytes(canonical_record)).hexdigest()
    hidden: tuple[Any, ...] = (
        scope_digest(request.context),
        request.context.tenant or "",
        batch_id,
        row_fingerprint,
        layout.resource.canonical,
        layout.schema_version,
        ingested_at,
    )
    return (*hidden, *logical_values)


def _batch_id(request: ExecutionRequest, layout: ResourceLayout) -> str:
    stable_key = request.context.idempotency_key or request.request_id
    material = canonical_json_bytes(
        {
            "bindingId": request.binding_id,
            "operationFingerprint": request.operation.request_fingerprint,
            "resource": layout.resource.canonical,
            "schemaFingerprint": layout.schema_fingerprint,
            "scopeFingerprint": scope_digest(request.context),
            "stableKey": stable_key,
        }
    )
    return "mb1_" + hashlib.sha256(material).hexdigest()


def _coerce(value: JsonValue, logical_type: JsonValue, *, many: bool) -> Any:
    if many:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
            raise TypeError("many-valued Schema field requires an array")
        return [_coerce(item, logical_type, many=False) for item in value]
    if value is None:
        return None
    if isinstance(logical_type, str):
        kind: object = logical_type
    elif isinstance(logical_type, Mapping):
        kind = logical_type.get("kind")
    else:
        raise TypeError("logical type layout must be a string or object")
    if kind == "boolean":
        if not isinstance(value, bool):
            raise TypeError("boolean field requires a boolean")
        return value
    integer_ranges = {
        "int8": (-(2**7), 2**7 - 1),
        "int16": (-(2**15), 2**15 - 1),
        "int32": (-(2**31), 2**31 - 1),
        "int64": (-(2**63), 2**63 - 1),
        "duration": (-(2**63), 2**63 - 1),
    }
    if kind in integer_ranges:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{kind} field requires an integer")
        minimum, maximum = integer_ranges[kind]
        if not minimum <= value <= maximum:
            raise ValueError(f"{kind} field is outside its signed range")
        return value
    if kind == "float64":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError("float64 field requires a number")
        selected = float(value)
        if not math.isfinite(selected):
            raise ValueError("float64 field requires a finite number")
        return selected
    if kind in {"string", "enum"}:
        if not isinstance(value, str):
            raise TypeError(f"{kind} field requires text")
        return value
    if kind == "utcTimestamp":
        if not isinstance(value, str):
            raise TypeError("utc-timestamp field requires an RFC 3339 string")
        return timestamp_nanoseconds(value)
    if kind == "date":
        if not isinstance(value, str):
            raise TypeError("date field requires an ISO date string")
        return date.fromisoformat(value)
    if kind == "uuid":
        if not isinstance(value, str):
            raise TypeError("uuid field requires canonical text")
        return UUID(value)
    if kind == "decimal":
        if isinstance(value, bool) or not isinstance(value, (str, int, float)):
            raise TypeError("decimal field requires canonical text or a finite number")
        selected_decimal = Decimal(value)
        if not selected_decimal.is_finite():
            raise ValueError("decimal field requires a finite value")
        return selected_decimal
    if kind == "bytes":
        if not isinstance(value, str):
            raise TypeError("bytes field requires canonical base64 text")
        return base64.b64decode(value, validate=True)
    if kind in {"json", "recordRef", "objectRef"}:
        return canonical_json_bytes(value).decode("utf-8")
    if kind == "wgs84Point":
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != 2:
            raise TypeError("wgs84-point field requires [longitude, latitude]")
        point = cast(Sequence[object], value)
        if any(isinstance(item, bool) or not isinstance(item, (int, float)) for item in point):
            raise TypeError("wgs84-point coordinates must be numbers")
        longitude, latitude = (float(cast(int | float, item)) for item in point)
        if (
            not math.isfinite(longitude)
            or not math.isfinite(latitude)
            or not -180 <= longitude <= 180
            or not -90 <= latitude <= 90
        ):
            raise ValueError("wgs84-point coordinates are outside their valid range")
        return (longitude, latitude)
    raise ValueError(f"logical type kind {kind!r} is unsupported")


def _invalid(message: str, request: ExecutionRequest) -> None:
    raise ValidationError(
        ErrorCode.OPERATION_INVALID,
        message,
        operation_contract=request.operation.operation_contract,
        resource_ref=str(request.operation.resources[0]),
        request_id=request.request_id,
        execution_id=request.execution_id,
    )


__all__ = ["BatchExecutor", "BatchReceipt", "PreparedBatch", "prepare_batch"]
