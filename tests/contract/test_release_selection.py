# SPDX-License-Identifier: Apache-2.0
"""Metadata acceptance is distinct from real-engine conformance evidence."""

from dataclasses import replace

import pytest
from meridian_storage.errors import CompatibilityError
from meridian_storage.runtime import BindingConfig

from meridian_storage.adapters.clickhouse import ClickHouseSettings, Topology, capability_manifest
from meridian_storage.adapters.clickhouse.descriptor import adapter_descriptor
from meridian_storage.adapters.clickhouse.probe import probe_adapter
from tests.conftest import binding_mapping, build_binding, build_layout
from tests.fakes import FakeClient


@pytest.mark.parametrize("topology", list(Topology))
@pytest.mark.parametrize("selected,observed", [("25.8", "25.8.23.1"), ("26.1.7.9", "26.1.7.9")])
def test_unlisted_selection_is_not_fabricated_conformance(topology, selected, observed):
    layout = build_layout(topology=topology)
    binding = build_binding(layout, engine_version=selected)
    settings = ClickHouseSettings.from_binding(binding)
    probe = probe_adapter(
        FakeClient(layout, version=observed), settings, selected_engine_version=selected
    )
    assert probe.manifest.engine_version == selected
    assert probe.observed_engine_version == observed
    assert probe.evidence["selectedEngineVersion"] == selected
    assert "unverified" in probe.evidence["releaseEvidence"]
    assert selected not in probe.manifest.descriptor.supported_engine_versions[topology.value]
    assert probe.manifest.fingerprint == binding.required_capability_fingerprint
    assert adapter_descriptor(settings, selected) == adapter_descriptor(settings, "25.3")
    assert (
        capability_manifest(settings, selected).fingerprint
        != capability_manifest(settings, "25.3").fingerprint
    )
    # Canonical layout and Binding round trips retain the independently selected release.
    raw = binding_mapping(layout, engine_version=selected)
    assert BindingConfig.from_mapping(raw, "binding").to_dict() == raw
    assert replace(settings, layouts=dict(reversed(list(settings.layouts.items())))) == settings


@pytest.mark.parametrize("observed", ["25.80.1", "25.3.1", "25.8-vendor"])
def test_selected_release_drift_is_still_rejected(observed):
    layout = build_layout()
    settings = ClickHouseSettings.from_binding(build_binding(layout, engine_version="25.8"))
    with pytest.raises(CompatibilityError, match="deployment release drift"):
        probe_adapter(
            FakeClient(layout, version=observed), settings, selected_engine_version="25.8"
        )


@pytest.mark.parametrize("failure", ["functions", "timezone", "topology"])
def test_unlisted_release_does_not_bypass_required_features(failure):
    layout = build_layout()
    fake = FakeClient(layout, version="25.8.23.1")
    if failure == "functions":
        fake.function_names = ("count",)
    elif failure == "timezone":
        fake.timezone = "America/Los_Angeles"
    else:
        fake.engine_override = "Memory"
    settings = ClickHouseSettings.from_binding(build_binding(layout, engine_version="25.8"))
    with pytest.raises(CompatibilityError, match=failure):
        probe_adapter(fake, settings, selected_engine_version="25.8")


def test_packaged_release_selection_golden_fingerprints():
    import json
    from importlib.resources import files

    document = json.loads(
        files("meridian_storage.adapters.clickhouse")
        .joinpath("contracts/release-selection.v1.json")
        .read_text()
    )
    assert document["evidenceClass"] == "canonical-metadata-only"
    for row in document["combinations"]:
        layout = build_layout(topology=Topology(row["profile"]))
        selected = row["selectedEngineVersion"]
        settings = ClickHouseSettings.from_binding(build_binding(layout, engine_version=selected))
        assert layout.layout_fingerprint == row["layoutFingerprint"]
        assert adapter_descriptor(settings, selected).fingerprint == row["descriptorFingerprint"]
        assert capability_manifest(settings, selected).fingerprint == row["manifestFingerprint"]
