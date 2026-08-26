# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import pytest
from meridian_storage.errors import ConfigurationError
from meridian_storage.runtime import BindingConfig

from meridian_storage.adapters.clickhouse import ClickHouseSettings
from meridian_storage.adapters.clickhouse.configuration import parse_endpoint
from tests.conftest import binding_mapping


def test_binding_settings_are_closed_and_layout_is_pinned(layout) -> None:  # type: ignore[no-untyped-def]
    value = binding_mapping(layout)
    settings = value["settings"]
    assert isinstance(settings, dict)
    settings["unknown"] = True
    binding = BindingConfig.from_mapping(value, "bindings[0]")
    with pytest.raises(ConfigurationError, match="unknown fields"):
        ClickHouseSettings.from_binding(binding)


def test_endpoint_cannot_embed_credentials_or_paths() -> None:
    with pytest.raises(ConfigurationError):
        parse_endpoint("http://user:password@localhost:8123/private", tls_mode="disabled")


def test_endpoint_scheme_must_match_tls_policy() -> None:
    with pytest.raises(ConfigurationError, match="must match"):
        parse_endpoint("http://localhost:8123", tls_mode="server")
