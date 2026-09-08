# SPDX-License-Identifier: Apache-2.0
"""Closed Adapter settings parsed from a Meridian Core ``BindingConfig``."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import cast
from urllib.parse import urlsplit

from meridian_storage.errors import ConfigurationError, ErrorCode
from meridian_storage.runtime import BindingConfig
from meridian_storage.semantics import JsonValue

from meridian_storage import ResourceRef

from ._canonical import physical_identifier
from .schema import ResourceLayout, Topology

ADAPTER_ID = "meridian.storage.clickhouse"
ADAPTER_CONTRACT_VERSION = "1.0.0"
TESTED_ENGINE_VERSIONS = ("25.3",)
# Legacy public name: historical conformance metadata, never an acceptance predicate.
SUPPORTED_ENGINE_VERSIONS = TESTED_ENGINE_VERSIONS
DEFAULT_MAX_BATCH_ROWS = 10_000
DEFAULT_MAX_BATCH_BYTES = 16 * 1024 * 1024
DEFAULT_MAX_TIME_RANGE_SECONDS = 31 * 24 * 60 * 60
DEFAULT_RETRY_WINDOW_SECONDS = 24 * 60 * 60
DEFAULT_CURSOR_TTL_SECONDS = 15 * 60

_SETTING_KEYS = frozenset(
    {
        "layouts",
        "maxBatchRows",
        "maxBatchBytes",
        "maxTimeRangeSeconds",
        "retryWindowSeconds",
        "cursorTtlSeconds",
        "insertQuorum",
        "requiredFunctions",
    }
)


@dataclass(frozen=True, slots=True)
class Endpoint:
    host: str
    port: int
    secure: bool


@dataclass(frozen=True, slots=True)
class ClickHouseSettings:
    database: str
    topology: Topology
    endpoint: Endpoint
    layouts: Mapping[str, ResourceLayout]
    max_batch_rows: int
    max_batch_bytes: int
    max_time_range_seconds: int
    retry_window_seconds: int
    cursor_ttl_seconds: int
    insert_quorum: int
    required_functions: tuple[str, ...]
    operation_timeout_ms: int
    max_result_bytes: int

    @classmethod
    def from_binding(cls, binding: BindingConfig) -> ClickHouseSettings:
        if binding.adapter_id != ADAPTER_ID:
            _fail("Binding Adapter identity does not select ClickHouse")
        if binding.adapter_contract != ADAPTER_CONTRACT_VERSION:
            _fail("Binding Adapter contract must be exactly 1.0.0")
        try:
            topology = Topology(binding.engine_profile)
        except ValueError as exc:
            raise ConfigurationError(
                ErrorCode.CONFIG_INVALID,
                "ClickHouse Binding Engine profile must be standalone or replicated",
            ) from exc
        if binding.endpoint is None:
            _fail("Platform must resolve serviceRef to a private endpoint before Adapter creation")
        endpoint = parse_endpoint(binding.endpoint, tls_mode=binding.tls.mode)
        database = physical_identifier(binding.physical_namespace, "physical namespace")
        settings = binding.settings
        unknown = set(settings) - _SETTING_KEYS
        if unknown:
            _fail(f"ClickHouse settings contain unknown fields: {sorted(unknown)!r}")
        raw_layouts = settings.get("layouts")
        if not isinstance(raw_layouts, Sequence) or isinstance(raw_layouts, (str, bytes)):
            _fail("ClickHouse settings.layouts must be an array")
        layouts: dict[str, ResourceLayout] = {}
        for index, value in enumerate(raw_layouts):
            try:
                layout = ResourceLayout.from_mapping(value)
            except (TypeError, ValueError) as exc:
                raise ConfigurationError(
                    ErrorCode.CONFIG_INVALID,
                    f"ClickHouse settings.layouts[{index}] is invalid",
                ) from exc
            if layout.topology is not topology:
                _fail("every ClickHouse layout topology must match the Binding Engine profile")
            key = layout.resource.canonical
            if key in layouts:
                _fail(f"ClickHouse settings.layouts contains duplicate Resource {key!r}")
            layouts[key] = layout
        if not layouts:
            _fail("ClickHouse settings.layouts cannot be empty")
        required_functions = _strings(
            settings.get("requiredFunctions", ("count", "quantile", "sum")),
            "requiredFunctions",
        )
        return cls(
            database=database,
            topology=topology,
            endpoint=endpoint,
            layouts=MappingProxyType(dict(sorted(layouts.items()))),
            max_batch_rows=_integer(
                settings.get("maxBatchRows", DEFAULT_MAX_BATCH_ROWS),
                "maxBatchRows",
                1,
                1_000_000,
            ),
            max_batch_bytes=_integer(
                settings.get("maxBatchBytes", DEFAULT_MAX_BATCH_BYTES),
                "maxBatchBytes",
                1,
                2_147_483_647,
            ),
            max_time_range_seconds=_integer(
                settings.get("maxTimeRangeSeconds", DEFAULT_MAX_TIME_RANGE_SECONDS),
                "maxTimeRangeSeconds",
                1,
                366 * 24 * 60 * 60,
            ),
            retry_window_seconds=_integer(
                settings.get("retryWindowSeconds", DEFAULT_RETRY_WINDOW_SECONDS),
                "retryWindowSeconds",
                1,
                7 * 24 * 60 * 60,
            ),
            cursor_ttl_seconds=_integer(
                settings.get("cursorTtlSeconds", DEFAULT_CURSOR_TTL_SECONDS),
                "cursorTtlSeconds",
                1,
                86_400,
            ),
            insert_quorum=_integer(settings.get("insertQuorum", 1), "insertQuorum", 1, 64),
            required_functions=required_functions,
            operation_timeout_ms=binding.client.operation_timeout_ms,
            max_result_bytes=binding.client.max_result_bytes,
        )

    def layout_for(self, resource: object) -> ResourceLayout:
        try:
            canonical = ResourceRef.parse(resource).canonical
            return self.layouts[canonical]
        except (AttributeError, KeyError) as exc:
            raise ConfigurationError(
                ErrorCode.RESOURCE_NOT_FOUND,
                "Operation Resource has no pinned ClickHouse physical layout",
            ) from exc


def parse_endpoint(value: str, *, tls_mode: str) -> Endpoint:
    parsed = urlsplit(value)
    expected_scheme = "https" if tls_mode != "disabled" else "http"
    if (
        parsed.scheme != expected_scheme
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        _fail(
            "ClickHouse endpoint must be an http(s) origin without credentials, path, query, "
            "or fragment and must match the Binding TLS policy"
        )
    try:
        port = parsed.port or (8443 if parsed.scheme == "https" else 8123)
    except ValueError as exc:
        raise ConfigurationError(
            ErrorCode.CONFIG_INVALID, "ClickHouse endpoint port is invalid"
        ) from exc
    assert parsed.hostname is not None
    return Endpoint(parsed.hostname, port, parsed.scheme == "https")


def _integer(value: JsonValue, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        _fail(f"ClickHouse settings.{name} must be between {minimum} and {maximum}")
    return cast(int, value)


def _strings(value: JsonValue, name: str) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        _fail(f"ClickHouse settings.{name} must be an array")
    values = cast(Sequence[object], value)
    if not values or any(not isinstance(item, str) or not item for item in values):
        _fail(f"ClickHouse settings.{name} must contain one or more non-empty strings")
    result = tuple(sorted(cast(Sequence[str], values)))
    if len(result) != len(set(result)):
        _fail(f"ClickHouse settings.{name} must be unique")
    return result


def _fail(message: str) -> None:
    raise ConfigurationError(ErrorCode.CONFIG_INVALID, message)


__all__ = [
    "ADAPTER_CONTRACT_VERSION",
    "ADAPTER_ID",
    "SUPPORTED_ENGINE_VERSIONS",
    "TESTED_ENGINE_VERSIONS",
    "ClickHouseSettings",
    "Endpoint",
    "parse_endpoint",
]
