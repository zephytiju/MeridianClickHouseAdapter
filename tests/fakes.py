# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from meridian_storage.adapters.clickhouse import ResourceLayout
from meridian_storage.adapters.clickhouse.schema import HIDDEN_COLUMNS


@dataclass
class FakeResult:
    column_names: tuple[str, ...]
    result_rows: Sequence[Sequence[Any]]
    summary: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if self.summary is None:
            self.summary = {}


class FakeClient:
    def __init__(self, layout: ResourceLayout, *, version: str = "25.3.8.23") -> None:
        self.layout = layout
        self.version = version
        self.closed = False
        self.commands: list[str] = []
        self.inserts: list[tuple[object, ...]] = []
        self.ordinary_result = FakeResult((), ())
        self.function_names = ("count", "quantile", "sum")
        self.metadata_override: tuple[str, ...] | None = None
        self.timezone = "UTC"
        self.replica_health = (0, 0, 2, 2)
        self.engine_override: str | None = None

    def query(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
        *,
        column_formats: Mapping[str, str] | None = None,
    ) -> FakeResult:
        if query.startswith("SELECT version()"):
            return FakeResult(("version", "timezone"), ((self.version, self.timezone),))
        if "FROM system.functions" in query:
            return FakeResult(("name",), tuple((name,) for name in self.function_names))
        if "FROM system.tables" in query:
            engine = self.engine_override or (
                "ReplicatedReplacingMergeTree"
                if self.layout.topology.value == "clickhouse-replicated"
                else "ReplacingMergeTree"
            )
            return FakeResult(("name", "engine"), ((self.layout.table, engine),))
        if "FROM system.columns" in query:
            hidden = (
                (HIDDEN_COLUMNS[0], "FixedString(64)"),
                (HIDDEN_COLUMNS[1], "String"),
                (HIDDEN_COLUMNS[2], "String"),
                (HIDDEN_COLUMNS[3], "FixedString(64)"),
                (HIDDEN_COLUMNS[4], "LowCardinality(String)"),
                (HIDDEN_COLUMNS[5], "String"),
                (HIDDEN_COLUMNS[6], "DateTime64(9, 'UTC')"),
            )
            rows = (
                *hidden,
                *((item.physical_name, item.clickhouse_type) for item in self.layout.columns),
            )
            return FakeResult(("name", "type"), rows)
        if "FROM system.replicas" in query:
            return FakeResult(
                (
                    "table",
                    "is_readonly",
                    "is_session_expired",
                    "total_replicas",
                    "active_replicas",
                ),
                ((self.layout.table, *self.replica_health),),
            )
        if "_meridian_resources" in query:
            metadata = self.metadata_override or (
                self.layout.resource_fingerprint,
                self.layout.schema_fingerprint,
                self.layout.record_profile.value,
                self.layout.table,
                self.layout.layout_fingerprint,
                self.layout.schema_version,
            )
            return FakeResult(
                (
                    "resource_ref",
                    "resource_fingerprint",
                    "schema_fingerprint",
                    "profile",
                    "table_name",
                    "layout_fingerprint",
                    "schema_version",
                ),
                ((self.layout.resource.canonical, *metadata),),
            )
        return self.ordinary_result

    def insert(
        self,
        table: str,
        data: Sequence[Sequence[Any]],
        column_names: Sequence[str],
        database: str,
        settings: Mapping[str, Any] | None = None,
    ) -> object:
        self.inserts.append(
            (table, tuple(data), tuple(column_names), database, dict(settings or {}))
        )
        return {}

    def command(
        self,
        command: str,
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> object:
        self.commands.append(command)
        return ""

    def close(self) -> None:
        self.closed = True
