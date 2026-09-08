# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace

import pytest
from meridian_storage.errors import CompatibilityError
from meridian_storage.semantics import CatalogName, SchemaReference
from meridian_storage.spi import PhysicalResource

from meridian_storage import ResourceRef
from meridian_storage.adapters.clickhouse import (
    ClickHouseMigrator,
    ClickHouseSchemaCompiler,
    ClickHouseSettings,
    Topology,
    plan_initial_migration,
)
from meridian_storage.adapters.clickhouse.probe import probe_adapter, verify_physical
from tests.conftest import RESOURCE_FINGERPRINT, build_binding, build_layout, build_schema
from tests.fakes import FakeClient, FakeResult


def _compilation(database="meridian_adapter_test", topology=Topology.STANDALONE):  # type: ignore[no-untyped-def]
    return ClickHouseSchemaCompiler().compile(
        database=database,
        resource=ResourceRef("evidence", "observability", "metric_points"),
        resource_fingerprint=RESOURCE_FINGERPRINT,
        schema=build_schema(),
        record_profile="metric",
        topology=topology,
    )


def test_migration_bundle_is_explicit_and_records_layout() -> None:
    compilation = _compilation()
    bundle = plan_initial_migration("migration-v1", (compilation,))
    fake = FakeClient(compilation.layout)
    settings = ClickHouseSettings.from_binding(build_binding(compilation.layout))
    ClickHouseMigrator(fake, settings).apply(bundle)

    assert len(fake.commands) == 3
    assert fake.commands[-1].startswith("OPTIMIZE TABLE")
    assert fake.inserts[0][0] == "_meridian_resources"
    assert fake.inserts[0][-1] == {"insert_deduplication_token": "migration-v1"}


def test_empty_and_mixed_migration_bundles_are_rejected() -> None:
    with pytest.raises(ValueError, match="at least one"):
        plan_initial_migration("migration-v1", ())
    with pytest.raises(ValueError, match="mix"):
        plan_initial_migration(
            "migration-v1",
            (_compilation(), _compilation(topology=Topology.REPLICATED)),
        )
    with pytest.raises(ValueError, match="unique"):
        plan_initial_migration("migration-v1", (_compilation(), _compilation()))


def test_migration_plan_order_is_canonical_and_binding_checked() -> None:
    first = _compilation()
    other_schema = replace(
        build_schema(),
        ref=SchemaReference(
            CatalogName("evidence"), "observability", "metric_points_other", "1.0.0"
        ),
    )
    second = ClickHouseSchemaCompiler().compile(
        database="meridian_adapter_test",
        resource=ResourceRef("evidence", "observability", "metric_points_other"),
        resource_fingerprint=RESOURCE_FINGERPRINT,
        schema=other_schema,
        record_profile="metric",
    )
    forward = plan_initial_migration("migration-v1", (first, second))
    reverse = plan_initial_migration("migration-v1", (second, first))
    assert forward == reverse

    settings_layout = build_layout()
    settings = ClickHouseSettings.from_binding(build_binding(settings_layout))
    with pytest.raises(ValueError, match="closed ClickHouse Binding"):
        ClickHouseMigrator(FakeClient(settings_layout), settings).apply(forward)


def test_probe_and_physical_verification_are_deterministic(layout) -> None:  # type: ignore[no-untyped-def]
    fake = FakeClient(layout)
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    first_probe = probe_adapter(fake, settings, selected_engine_version="25.3")
    second_probe = probe_adapter(fake, settings, selected_engine_version="25.3")
    resource = PhysicalResource(
        layout.resource,
        layout.resource_fingerprint,
        layout.schema_fingerprint,
        layout.record_profile.value,
    )
    first_physical = verify_physical(fake, settings, (resource,))
    second_physical = verify_physical(fake, settings, (resource,))

    assert first_probe == second_probe
    assert first_physical == second_physical
    assert first_physical.mappings[layout.resource.canonical].endswith(layout.table + "`")


def test_probe_rejects_version_functions_topology_and_metadata(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    wrong_version = FakeClient(layout, version="24.8.1.1")
    with pytest.raises(CompatibilityError, match="deployment release drift"):
        probe_adapter(wrong_version, settings, selected_engine_version="25.3")

    missing_function = FakeClient(layout)
    missing_function.function_names = ("count", "sum")
    with pytest.raises(CompatibilityError, match="functions"):
        probe_adapter(missing_function, settings, selected_engine_version="25.3")

    wrong_timezone = FakeClient(layout)
    wrong_timezone.timezone = "America/Los_Angeles"
    with pytest.raises(CompatibilityError, match="timezone"):
        probe_adapter(wrong_timezone, settings, selected_engine_version="25.3")

    wrong_engine = FakeClient(layout)
    wrong_engine.engine_override = "Memory"
    with pytest.raises(CompatibilityError, match="topology"):
        probe_adapter(wrong_engine, settings, selected_engine_version="25.3")

    replicated = build_layout(topology=Topology.REPLICATED)
    mismatched_settings = ClickHouseSettings.from_binding(build_binding(replicated))
    standalone_fake = FakeClient(layout)
    standalone_fake.layout = replicated
    object.__setattr__(standalone_fake.layout, "topology", Topology.STANDALONE)
    with pytest.raises(CompatibilityError, match="topology"):
        probe_adapter(standalone_fake, mismatched_settings, selected_engine_version="25.3")

    bad_metadata = FakeClient(layout)
    bad_metadata.metadata_override = ("sha256:" + "8" * 64,) * 6
    resource = PhysicalResource(
        layout.resource,
        layout.resource_fingerprint,
        layout.schema_fingerprint,
        layout.record_profile.value,
    )
    with pytest.raises(CompatibilityError, match="metadata"):
        verify_physical(bad_metadata, settings, (resource,))


def test_replicated_probe_requires_a_healthy_active_replica_set() -> None:
    replicated = build_layout(topology=Topology.REPLICATED)
    settings = ClickHouseSettings.from_binding(build_binding(replicated))
    healthy = FakeClient(replicated)
    probe = probe_adapter(healthy, settings, selected_engine_version="25.3")
    assert probe.evidence["replicaCount"] == "2"

    for health in ((1, 0, 2, 2), (0, 1, 2, 2), (0, 0, 1, 1), (0, 0, 2, 1)):
        unhealthy = FakeClient(replicated)
        unhealthy.replica_health = health
        with pytest.raises(CompatibilityError, match="healthy active replica set"):
            probe_adapter(unhealthy, settings, selected_engine_version="25.3")


def test_physical_verification_rejects_empty_duplicate_and_drift(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    resource = PhysicalResource(
        layout.resource,
        layout.resource_fingerprint,
        layout.schema_fingerprint,
        layout.record_profile.value,
    )
    with pytest.raises(ValueError, match="at least one"):
        verify_physical(FakeClient(layout), settings, ())
    with pytest.raises(ValueError, match="unique"):
        verify_physical(FakeClient(layout), settings, (resource, resource))

    class DriftedClient(FakeClient):
        drift: str

        def query(self, query, parameters=None, settings=None):  # type: ignore[no-untyped-def]
            if self.drift == "columns" and "FROM system.columns" in query:
                return FakeResult(("name", "type"), (("wrong", "String"),))
            if self.drift == "tables" and "FROM system.tables" in query:
                return FakeResult(("name", "engine"), ())
            if self.drift == "metadata" and "_meridian_resources" in query:
                return FakeResult(("resource_ref",), ())
            return super().query(query, parameters, settings)

    for drift, message in (
        ("columns", "physical columns"),
        ("tables", "missing"),
        ("metadata", "metadata is missing"),
    ):
        client = DriftedClient(layout)
        client.drift = drift
        with pytest.raises(CompatibilityError, match=message):
            verify_physical(client, settings, (resource,))


def test_fixed_string_metadata_bytes_are_normalized(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    client = FakeClient(layout)
    client.metadata_override = tuple(
        value.encode("ascii")
        for value in (
            layout.resource_fingerprint,
            layout.schema_fingerprint,
            layout.record_profile.value,
            layout.table,
            layout.layout_fingerprint,
            layout.schema_version,
        )
    )  # type: ignore[assignment]
    resource = PhysicalResource(
        layout.resource,
        layout.resource_fingerprint,
        layout.schema_fingerprint,
        layout.record_profile.value,
    )
    assert verify_physical(client, settings, (resource,)).mappings
