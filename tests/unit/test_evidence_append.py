# SPDX-License-Identifier: Apache-2.0
from dataclasses import replace

import pytest
from meridian_storage.errors import CompatibilityError
from meridian_storage.spi import PhysicalResource

from meridian_storage.adapters.clickhouse import ClickHouseSettings, ResourceLayout, Topology
from meridian_storage.adapters.clickhouse.descriptor import adapter_descriptor
from meridian_storage.adapters.clickhouse.probe import probe_adapter, verify_physical
from meridian_storage.adapters.clickhouse.schema import HIDDEN_ROW, create_table_ddl
from tests.conftest import build_binding, build_layout
from tests.fakes import FakeClient


def test_direct_spi_atomic_append_is_rejected_before_insert():
    from meridian_storage.adapters.clickhouse import ClickHouseAdapterFactory
    from meridian_storage.adapters.clickhouse.client import ClientLease
    from tests.conftest import build_create_context, build_request

    layout = build_layout()
    fake = FakeClient(layout)
    runtime = ClickHouseAdapterFactory(lambda _context, _settings: ClientLease(fake)).create(
        build_create_context(layout)
    )
    runtime.open()
    session = runtime.open_session(transactional=False)
    request = build_request(layout)
    request = replace(
        request,
        operation=replace(
            request.operation, input={**request.operation.input, "requireAtomic": True}
        ),
    )
    try:
        with pytest.raises(CompatibilityError, match="atomic-evidence"):
            session.execute(request)
        assert fake.inserts == []
    finally:
        session.close()
        runtime.close()


@pytest.mark.parametrize("topology", list(Topology))
def test_append_only_layout_is_explicit_and_legacy_locks_are_unchanged(topology):
    layout = build_layout(topology=topology)
    legacy = replace(layout, append_only=False)
    assert "appendOnly" not in legacy.to_dict()
    assert ResourceLayout.from_mapping(legacy.to_dict()) == legacy
    assert ResourceLayout.from_mapping(layout.to_dict()) == layout
    assert layout.layout_fingerprint != legacy.layout_fingerprint
    assert HIDDEN_ROW in create_table_ddl("test", layout).split("ORDER BY")[1]
    assert HIDDEN_ROW not in create_table_ddl("test", legacy).split("ORDER BY")[1]
    for selected, supported in [(layout, True), (legacy, False)]:
        settings = ClickHouseSettings.from_binding(build_binding(selected))
        descriptor = adapter_descriptor(settings, "25.8")
        assert (
            "append-only" in descriptor.capability_for("meridian.evidence.append").guarantees
        ) == supported
        assert "append-only" not in descriptor.capability_for("meridian.structured.put").guarantees
        assert "atomic-evidence" not in repr(descriptor.to_dict())


def test_structured_compiler_preserves_append_version():
    layout = build_layout(catalog="structured")
    assert not layout.append_only
    with pytest.raises(ValueError, match="evidence Catalog"):
        replace(layout, append_only=True)
    with pytest.raises(TypeError, match="appendOnly"):
        replace(layout, append_only="true")


@pytest.mark.parametrize("value", [False, "true", 1, None])
def test_append_only_wire_flag_is_canonical(value):
    raw = build_layout().to_dict()
    raw["appendOnly"] = value
    with pytest.raises(ValueError, match="appendOnly"):
        ResourceLayout.from_mapping(raw)


def test_mixed_layout_binding_does_not_overpromise():
    layout = build_layout()
    legacy = replace(layout, resource=replace(layout.resource, name="legacy"), append_only=False)
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    settings = replace(
        settings, layouts={layout.resource.canonical: layout, legacy.resource.canonical: legacy}
    )
    assert (
        "append-only"
        not in adapter_descriptor(settings, "25.8")
        .capability_for("meridian.evidence.append")
        .guarantees
    )


@pytest.mark.parametrize("physical", [False, True])
def test_relabelled_legacy_table_is_rejected_by_actual_sorting_key(physical):
    layout = build_layout()
    client = FakeClient(layout)
    client.sorting_key_override = ", ".join(client.layout.order_fields[:-1])
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    with pytest.raises(CompatibilityError, match="append-only sorting key"):
        if physical:
            verify_physical(
                client,
                settings,
                (
                    PhysicalResource(
                        layout.resource,
                        layout.resource_fingerprint,
                        layout.schema_fingerprint,
                        layout.record_profile.value,
                    ),
                ),
            )
        else:
            probe_adapter(client, settings, selected_engine_version="25.3")
