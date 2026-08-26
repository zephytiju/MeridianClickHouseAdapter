# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from copy import deepcopy

import pytest
from meridian_storage.errors import ConfigurationError
from meridian_storage.runtime import BindingConfig

from meridian_storage import ResourceRef
from meridian_storage.adapters.clickhouse import ClickHouseSettings, Topology
from meridian_storage.adapters.clickhouse.configuration import parse_endpoint
from tests.conftest import binding_mapping, build_binding, build_layout


def test_endpoint_parsing_accepts_only_matching_origins() -> None:
    assert parse_endpoint("http://clickhouse.internal", tls_mode="disabled").port == 8123
    secure = parse_endpoint("https://clickhouse.internal", tls_mode="server")
    assert secure.port == 8443
    assert secure.secure is True

    for endpoint in (
        "http://clickhouse.internal/path",
        "http://clickhouse.internal?query=yes",
        "http://clickhouse.internal#fragment",
        "http://user@clickhouse.internal",
        "http://:8123",
    ):
        with pytest.raises(ConfigurationError, match="origin"):
            parse_endpoint(endpoint, tls_mode="disabled")
    with pytest.raises(ConfigurationError, match="port"):
        parse_endpoint("http://clickhouse.internal:99999", tls_mode="disabled")


@pytest.mark.parametrize(
    ("attribute", "value", "message"),
    [
        ("adapter_id", "meridian.storage.other", "does not select"),
        ("adapter_contract", "2.0.0", "exactly 1.0.0"),
        ("engine_version", "26.1", "not supported"),
        ("engine_profile", "clickhouse-future", "standalone or replicated"),
        ("endpoint", None, "resolve serviceRef"),
        ("physical_namespace", "unsafe; DROP DATABASE", "physical identifier"),
    ],
)
def test_binding_identity_and_engine_selection_fail_closed(
    layout, attribute: str, value: object, message: str
) -> None:  # type: ignore[no-untyped-def]
    binding = build_binding(layout)
    object.__setattr__(binding, attribute, value)
    with pytest.raises((ConfigurationError, ValueError), match=message):
        ClickHouseSettings.from_binding(binding)


def test_layouts_are_required_unique_and_topology_pinned(layout) -> None:  # type: ignore[no-untyped-def]
    binding = build_binding(layout)
    settings = dict(binding.settings)
    settings["layouts"] = []
    object.__setattr__(binding, "settings", settings)
    with pytest.raises(ConfigurationError, match="cannot be empty"):
        ClickHouseSettings.from_binding(binding)

    binding = build_binding(layout)
    settings = dict(binding.settings)
    settings["layouts"] = [layout.to_dict(), layout.to_dict()]
    object.__setattr__(binding, "settings", settings)
    with pytest.raises(ConfigurationError, match="duplicate Resource"):
        ClickHouseSettings.from_binding(binding)

    replicated = build_layout(topology=Topology.REPLICATED)
    binding = build_binding(layout)
    settings = dict(binding.settings)
    settings["layouts"] = [replicated.to_dict()]
    object.__setattr__(binding, "settings", settings)
    with pytest.raises(ConfigurationError, match="topology"):
        ClickHouseSettings.from_binding(binding)


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("maxBatchRows", True, "between"),
        ("maxBatchRows", 0, "between"),
        ("maxBatchBytes", 2_147_483_648, "between"),
        ("maxTimeRangeSeconds", 0, "between"),
        ("retryWindowSeconds", 604_801, "between"),
        ("cursorTtlSeconds", 86_401, "between"),
        ("insertQuorum", 65, "between"),
        ("requiredFunctions", [], "one or more"),
        ("requiredFunctions", ["count", "count"], "unique"),
        ("requiredFunctions", "count", "array"),
    ],
)
def test_closed_setting_limits_are_enforced(layout, name: str, value: object, message: str) -> None:  # type: ignore[no-untyped-def]
    mapping = binding_mapping(layout)
    configured = mapping["settings"]
    assert isinstance(configured, dict)
    configured[name] = value
    binding = BindingConfig.from_mapping(mapping, "bindings[0]")
    with pytest.raises(ConfigurationError, match=message):
        ClickHouseSettings.from_binding(binding)


def test_defaults_and_resource_lookup_are_immutable(layout) -> None:  # type: ignore[no-untyped-def]
    mapping = binding_mapping(layout)
    configured = mapping["settings"]
    assert isinstance(configured, dict)
    for key in tuple(configured):
        if key != "layouts":
            configured.pop(key)
    settings = ClickHouseSettings.from_binding(
        BindingConfig.from_mapping(deepcopy(mapping), "bindings[0]")
    )
    assert settings.max_batch_rows == 10_000
    assert settings.required_functions == ("count", "quantile", "sum")
    assert settings.layout_for(layout.resource) == layout
    with pytest.raises(ConfigurationError, match="no pinned"):
        settings.layout_for(ResourceRef("evidence", "other", "missing"))
    with pytest.raises(TypeError):
        settings.layouts[layout.resource.canonical] = layout  # type: ignore[index]
