# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import stat
from pathlib import Path
from typing import Any

import pytest
from meridian_storage.errors import ConfigurationError
from meridian_storage.runtime import BindingConfig
from meridian_storage.spi import AdapterCreateContext, SecretValue

from meridian_storage.adapters.clickhouse import ClickHouseSettings
from meridian_storage.adapters.clickhouse import client as client_module
from meridian_storage.adapters.clickhouse.client import ClientLease
from tests.conftest import binding_mapping


class _DriverClient:
    def __init__(self, *, fail_close: bool = False) -> None:
        self.closed = False
        self.fail_close = fail_close

    def close(self) -> None:
        self.closed = True
        if self.fail_close:
            raise RuntimeError("close failed")


def _context(layout, *, tls: bool) -> tuple[AdapterCreateContext, ClickHouseSettings]:  # type: ignore[no-untyped-def]
    value = binding_mapping(
        layout,
        endpoint="https://clickhouse.internal:8443" if tls else "http://127.0.0.1:8123",
    )
    if tls:
        value["tls"] = {
            "mode": "mutual",
            "serverName": "clickhouse.internal",
            "caRef": {"provider": "test", "reference": "ca"},
            "clientCertificateRef": {"provider": "test", "reference": "client"},
        }
    binding = BindingConfig.from_mapping(value, "bindings[0]")
    context = AdapterCreateContext(
        binding,
        SecretValue(b"meridian"),
        SecretValue(b"password"),
        SecretValue(b"test-ca") if tls else None,
        SecretValue(b"test-client") if tls else None,
    )
    return context, ClickHouseSettings.from_binding(binding)


def test_connect_constructs_closed_driver_options_and_cleans_tls_files(
    layout, monkeypatch: pytest.MonkeyPatch
) -> None:  # type: ignore[no-untyped-def]
    captured: dict[str, Any] = {}
    driver = _DriverClient()

    def get_client(**kwargs: Any) -> _DriverClient:
        captured.update(kwargs)
        return driver

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", get_client)
    context, settings = _context(layout, tls=True)
    lease = client_module.connect(context, settings)
    ca_path = Path(captured["ca_cert"])
    certificate_path = Path(captured["client_cert"])

    assert captured["host"] == "clickhouse.internal"
    assert captured["secure"] is True
    assert captured["verify"] is True
    assert captured["query_retries"] == 0
    assert stat.S_IMODE(ca_path.stat().st_mode) == 0o600
    assert ca_path.read_bytes() == b"test-ca"
    assert certificate_path.read_bytes() == b"test-client"

    lease.close()
    assert driver.closed
    assert not ca_path.exists()
    assert not certificate_path.exists()


def test_connect_cleans_tls_files_when_driver_creation_fails(
    layout, monkeypatch: pytest.MonkeyPatch
) -> None:  # type: ignore[no-untyped-def]
    paths: list[Path] = []

    def fail(**kwargs: Any) -> None:
        paths.extend((Path(kwargs["ca_cert"]), Path(kwargs["client_cert"])))
        raise RuntimeError("driver failed")

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", fail)
    context, settings = _context(layout, tls=True)
    with pytest.raises(RuntimeError, match="driver failed"):
        client_module.connect(context, settings)
    assert paths
    assert all(not path.exists() for path in paths)


def test_connect_without_tls_does_not_materialize_files(
    layout, monkeypatch: pytest.MonkeyPatch
) -> None:  # type: ignore[no-untyped-def]
    captured: dict[str, Any] = {}

    def get_client(**kwargs: Any) -> _DriverClient:
        captured.update(kwargs)
        return _DriverClient()

    monkeypatch.setattr(client_module.clickhouse_connect, "get_client", get_client)
    context, settings = _context(layout, tls=False)
    lease = client_module.connect(context, settings)
    try:
        assert captured["secure"] is False
        assert "ca_cert" not in captured
        assert "client_cert" not in captured
    finally:
        lease.close()


@pytest.mark.parametrize("value", (b"", b"contains\x00nul", b"\xff"))
def test_invalid_secrets_are_rejected(value: bytes) -> None:
    with pytest.raises(ConfigurationError):
        client_module._decode_secret(value, "credential")


def test_client_lease_removes_material_even_when_driver_close_fails(tmp_path: Path) -> None:
    material = tmp_path / "credential.pem"
    material.write_bytes(b"secret")
    driver = _DriverClient(fail_close=True)
    lease = ClientLease(driver, (material,))  # type: ignore[arg-type]

    with pytest.raises(RuntimeError, match="close failed"):
        lease.close()
    assert driver.closed
    assert not material.exists()
