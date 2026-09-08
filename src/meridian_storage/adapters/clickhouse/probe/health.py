# SPDX-License-Identifier: Apache-2.0
"""Read-only probes that bind advertised capabilities to real Engine evidence."""

from __future__ import annotations

from collections.abc import Mapping
from typing import cast

from meridian_storage.errors import CompatibilityError, ErrorCode
from meridian_storage.semantics import JsonValue
from meridian_storage.spi import AdapterProbe, PhysicalResource, PhysicalVerification

from .._canonical import fingerprint, quote_identifier
from ..client import ClickHouseClient
from ..configuration import ClickHouseSettings
from ..descriptor import capability_manifest
from ..schema import HIDDEN_COLUMNS, METADATA_TABLE, ResourceLayout, Topology


def probe_adapter(
    client: ClickHouseClient,
    settings: ClickHouseSettings,
    *,
    selected_engine_version: str,
) -> AdapterProbe:
    identity = client.query("SELECT version() AS version, timezone() AS timezone")
    if len(identity.result_rows) != 1:
        _incompatible("ClickHouse identity probe did not return one row")
    actual_version, timezone = (str(item) for item in identity.result_rows[0])
    if not _version_matches(actual_version, selected_engine_version):
        _incompatible(
            "ClickHouse deployment release drift: observed server differs from Binding selection"
        )
    if timezone not in {"UTC", "Etc/UTC"}:
        _incompatible("ClickHouse server timezone must be UTC")
    required = settings.required_functions
    placeholders = ", ".join(f"{{f{index}:String}}" for index in range(len(required)))
    parameters = {f"f{index}": name for index, name in enumerate(required)}
    functions = client.query(
        f"SELECT name FROM system.functions WHERE name IN ({placeholders}) ORDER BY name",
        parameters,
    )
    found = tuple(str(row[0]) for row in functions.result_rows)
    if found != required:
        _incompatible("ClickHouse server is missing one or more required functions")
    engines = _table_engines(client, settings)
    expected_replicated = settings.topology is Topology.REPLICATED
    expected_engine = (
        "ReplicatedReplacingMergeTree" if expected_replicated else "ReplacingMergeTree"
    )
    if any(value != expected_engine for value in engines.values()):
        _incompatible("ClickHouse physical topology differs from the Binding Engine profile")
    replica_count = 1
    if expected_replicated:
        replica_count = _verify_replicas(client, settings)
    manifest = capability_manifest(settings, selected_engine_version)
    return AdapterProbe(
        manifest,
        {
            "actualEngineVersion": actual_version,
            "selectedEngineVersion": selected_engine_version,
            "releaseEvidence": "unverified; consult exact conformance reports",
            "functionCount": str(len(found)),
            "layoutCount": str(len(settings.layouts)),
            "migrationAuthority": "external-iac-job",
            "replicaCount": str(replica_count),
            "timezone": timezone,
            "topology": settings.topology.value,
        },
        observed_engine_version=actual_version,
    )


def verify_physical(
    client: ClickHouseClient,
    settings: ClickHouseSettings,
    resources: tuple[PhysicalResource, ...],
) -> PhysicalVerification:
    if not resources:
        raise ValueError("physical verification requires at least one Resource")
    declared = {item.resource_ref.canonical: item for item in resources}
    if len(declared) != len(resources):
        raise ValueError("physical verification Resources must be unique")
    metadata = _metadata(client, settings)
    mappings: dict[str, str] = {}
    evidence_rows: list[JsonValue] = []
    engines = _table_engines(client, settings)
    for canonical, expected in sorted(declared.items()):
        layout = settings.layouts.get(canonical)
        row = metadata.get(canonical)
        if layout is None or row is None:
            _incompatible("ClickHouse metadata is missing a required physical Resource")
        assert layout is not None and row is not None
        _verify_metadata(row, expected, layout)
        columns = _columns(client, settings.database, layout)
        expected_columns = _expected_columns(layout)
        if columns != expected_columns:
            _incompatible("ClickHouse physical columns differ from the compiled Schema layout")
        engine = engines.get(layout.table)
        if engine is None:
            _incompatible("ClickHouse physical table is missing")
        expected_engine = (
            "ReplicatedReplacingMergeTree"
            if layout.topology is Topology.REPLICATED
            else "ReplacingMergeTree"
        )
        if engine != expected_engine:
            _incompatible("ClickHouse physical table engine differs from the compiled layout")
        mapping = layout.qualified_table(settings.database)
        mappings[canonical] = mapping
        evidence_rows.append(
            {
                "columns": [[name, value] for name, value in columns],
                "engine": engine,
                "layoutFingerprint": layout.layout_fingerprint,
                "mapping": mapping,
                "resource": canonical,
                "resourceFingerprint": expected.resource_fingerprint,
                "schemaFingerprint": expected.schema_fingerprint,
            }
        )
    return PhysicalVerification(
        fingerprint(cast(JsonValue, evidence_rows)),
        mappings=mappings,
        evidence={
            "migrationAuthority": "external-iac-job",
            "resourceCount": str(len(resources)),
            "topology": settings.topology.value,
            "verification": "metadata+system.columns+system.tables",
        },
    )


def _metadata(
    client: ClickHouseClient,
    settings: ClickHouseSettings,
) -> Mapping[str, tuple[str, ...]]:
    table = f"{quote_identifier(settings.database)}.{quote_identifier(METADATA_TABLE)}"
    result = client.query(
        "SELECT resource_ref, resource_fingerprint, schema_fingerprint, profile, "
        f"table_name, layout_fingerprint, schema_version FROM {table} FINAL "
        "ORDER BY resource_ref"
    )
    return {_text(row[0]): tuple(_text(value) for value in row[1:]) for row in result.result_rows}


def _verify_metadata(
    row: tuple[str, ...],
    expected: PhysicalResource,
    layout: ResourceLayout,
) -> None:
    values = (
        expected.resource_fingerprint,
        expected.schema_fingerprint or layout.schema_fingerprint,
        expected.profile,
        layout.table,
        layout.layout_fingerprint,
        layout.schema_version,
    )
    if row != values:
        _incompatible("ClickHouse metadata fingerprints or profile differ from the Registry pins")


def _columns(
    client: ClickHouseClient,
    database: str,
    layout: ResourceLayout,
) -> tuple[tuple[str, str], ...]:
    result = client.query(
        "SELECT name, type FROM system.columns "
        "WHERE database = {database:String} AND table = {table:String} ORDER BY position",
        {"database": database, "table": layout.table},
    )
    return tuple((str(row[0]), str(row[1])) for row in result.result_rows)


def _expected_columns(layout: ResourceLayout) -> tuple[tuple[str, str], ...]:
    hidden = (
        (HIDDEN_COLUMNS[0], "FixedString(64)"),
        (HIDDEN_COLUMNS[1], "String"),
        (HIDDEN_COLUMNS[2], "String"),
        (HIDDEN_COLUMNS[3], "FixedString(64)"),
        (HIDDEN_COLUMNS[4], "LowCardinality(String)"),
        (HIDDEN_COLUMNS[5], "String"),
        (HIDDEN_COLUMNS[6], "DateTime64(9, 'UTC')"),
    )
    return (*hidden, *((item.physical_name, item.clickhouse_type) for item in layout.columns))


def _table_engines(
    client: ClickHouseClient,
    settings: ClickHouseSettings,
) -> Mapping[str, str]:
    names = tuple(item.table for item in settings.layouts.values())
    result = client.query(
        "SELECT name, engine FROM system.tables "
        "WHERE database = {database:String} AND name IN {tables:Array(String)} ORDER BY name",
        {"database": settings.database, "tables": list(names)},
    )
    engines = {str(row[0]): str(row[1]) for row in result.result_rows}
    if set(engines) != set(names):
        _incompatible("ClickHouse is missing one or more IaC-managed physical tables")
    return engines


def _verify_replicas(client: ClickHouseClient, settings: ClickHouseSettings) -> int:
    names = tuple(item.table for item in settings.layouts.values())
    result = client.query(
        "SELECT table, is_readonly, is_session_expired, total_replicas, active_replicas "
        "FROM system.replicas WHERE database = {database:String} "
        "AND table IN {tables:Array(String)} ORDER BY table",
        {"database": settings.database, "tables": list(names)},
    )
    rows = {str(row[0]): tuple(int(value) for value in row[1:]) for row in result.result_rows}
    if set(rows) != set(names):
        _incompatible("ClickHouse replicated tables are missing replica health metadata")
    replica_counts: set[int] = set()
    for is_readonly, is_expired, total_replicas, active_replicas in rows.values():
        if is_readonly or is_expired or total_replicas < 2 or active_replicas != total_replicas:
            _incompatible("ClickHouse replicated table does not have a healthy active replica set")
        replica_counts.add(total_replicas)
    if len(replica_counts) != 1:
        _incompatible("ClickHouse replicated tables disagree on replica count")
    return next(iter(replica_counts))


def _version_matches(actual: str, selected: str) -> bool:
    return actual == selected or actual.startswith(selected + ".")


def _text(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode("ascii")
    return str(value)


def _incompatible(message: str) -> None:
    raise CompatibilityError(ErrorCode.ADAPTER_CONTRACT, message)


__all__ = ["probe_adapter", "verify_physical"]
