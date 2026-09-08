# SPDX-License-Identifier: Apache-2.0
"""Explicit migration artifacts executed by Platform/Vangu IaC jobs, never startup."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ._canonical import quote_identifier
from .client import ClickHouseClient
from .configuration import ClickHouseSettings
from .probe.health import _verify_append_order
from .schema import METADATA_TABLE, ResourceLayout, SchemaCompilation


@dataclass(frozen=True, slots=True)
class MigrationBundle:
    migration_id: str
    statements: tuple[str, ...]
    layouts: tuple[ResourceLayout, ...]

    def __post_init__(self) -> None:
        if not self.migration_id or not self.statements or not self.layouts:
            raise ValueError("migration bundle identity, statements, and layouts are required")


def plan_initial_migration(
    migration_id: str,
    compilations: tuple[SchemaCompilation, ...],
) -> MigrationBundle:
    if not compilations:
        raise ValueError("initial ClickHouse migration requires at least one Schema compilation")
    ordered = tuple(sorted(compilations, key=lambda item: item.layout.resource))
    topology = ordered[0].layout.topology
    if any(item.layout.topology is not topology for item in ordered):
        raise ValueError("one ClickHouse migration cannot mix Engine topology profiles")
    resources = tuple(item.layout.resource for item in ordered)
    if len(set(resources)) != len(resources):
        raise ValueError("initial ClickHouse migration Resources must be unique")
    metadata = {item.metadata_table_sql for item in ordered}
    if len(metadata) != 1:
        raise ValueError("one ClickHouse migration must target exactly one physical namespace")
    statements = (next(iter(metadata)), *(item.create_table_sql for item in ordered))
    layouts = tuple(item.layout for item in ordered)
    return MigrationBundle(migration_id, statements, layouts)


class ClickHouseMigrator:
    """Apply a pre-rendered bundle only when called by an authorized deployment job."""

    def __init__(self, client: ClickHouseClient, settings: ClickHouseSettings) -> None:
        self._client = client
        self._settings = settings

    def apply(self, bundle: MigrationBundle, *, now: datetime | None = None) -> None:
        for layout in bundle.layouts:
            expected = self._settings.layouts.get(layout.resource.canonical)
            if expected is None or expected.layout_fingerprint != layout.layout_fingerprint:
                raise ValueError("migration layout differs from the closed ClickHouse Binding")
        for statement in bundle.statements:
            self._client.command(statement)
        for layout in bundle.layouts:
            if layout.append_only:
                # IF NOT EXISTS cannot upgrade an old table. Never relabel its metadata.
                _verify_append_order(self._client, self._settings.database, layout)
        timestamp = (now or datetime.now(UTC)).astimezone(UTC)
        metadata = f"{quote_identifier(self._settings.database)}.{quote_identifier(METADATA_TABLE)}"
        rows: list[tuple[Any, ...]] = [
            (
                layout.resource.canonical,
                layout.resource_fingerprint,
                layout.schema_fingerprint,
                layout.record_profile.value,
                layout.table,
                layout.layout_fingerprint,
                layout.schema_version,
                timestamp,
            )
            for layout in bundle.layouts
        ]
        self._client.insert(
            table=METADATA_TABLE,
            data=rows,
            column_names=(
                "resource_ref",
                "resource_fingerprint",
                "schema_fingerprint",
                "profile",
                "table_name",
                "layout_fingerprint",
                "schema_version",
                "updated_at",
            ),
            database=self._settings.database,
            settings={"insert_deduplication_token": bundle.migration_id},
        )
        self._client.command(f"OPTIMIZE TABLE {metadata} FINAL")


__all__ = ["ClickHouseMigrator", "MigrationBundle", "plan_initial_migration"]
