# SPDX-License-Identifier: Apache-2.0
"""Credential-safe ClickHouse HTTP client construction and narrow execution protocol."""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import clickhouse_connect  # type: ignore[import-untyped]
from meridian_storage.errors import ConfigurationError, ErrorCode
from meridian_storage.spi import AdapterCreateContext

from .configuration import ClickHouseSettings


@runtime_checkable
class QueryResult(Protocol):
    column_names: tuple[str, ...]
    result_rows: Sequence[Sequence[Any]]
    summary: Mapping[str, str]


@runtime_checkable
class ClickHouseClient(Protocol):
    def query(
        self,
        query: str,
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
        *,
        column_formats: Mapping[str, str] | None = None,
    ) -> QueryResult: ...

    def insert(
        self,
        table: str,
        data: Sequence[Sequence[Any]],
        column_names: Sequence[str],
        database: str,
        settings: Mapping[str, Any] | None = None,
    ) -> object: ...

    def command(
        self,
        command: str,
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
    ) -> object: ...

    def close(self) -> None: ...


class ClientLease:
    """Own a driver client and any short-lived TLS material backing it."""

    def __init__(self, client: ClickHouseClient, temporary_paths: Sequence[Path] = ()) -> None:
        self.client = client
        self._temporary_paths = tuple(temporary_paths)

    def close(self) -> None:
        try:
            self.client.close()
        finally:
            for path in self._temporary_paths:
                with contextlib.suppress(OSError):
                    path.unlink(missing_ok=True)


def connect(context: AdapterCreateContext, settings: ClickHouseSettings) -> ClientLease:
    username = _decode_secret(context.identity.reveal(), "identity")
    password = _decode_secret(context.credential.reveal(), "credential")
    temporary_paths: list[Path] = []
    kwargs: dict[str, Any] = {
        "host": settings.endpoint.host,
        "port": settings.endpoint.port,
        "username": username,
        "password": password,
        "database": settings.database,
        "secure": settings.endpoint.secure,
        "connect_timeout": max(1, context.binding.client.acquire_timeout_ms // 1000),
        "send_receive_timeout": max(1, context.binding.client.operation_timeout_ms // 1000),
        "query_retries": 0,
        "tz_mode": "aware",
    }
    if context.binding.tls.mode != "disabled":
        kwargs["verify"] = True
        kwargs["server_host_name"] = context.binding.tls.server_name
        if context.tls_ca is not None:
            ca_path = _secret_file(context.tls_ca.reveal(), suffix="-ca.pem")
            temporary_paths.append(ca_path)
            kwargs["ca_cert"] = str(ca_path)
        if context.tls_client_certificate is not None:
            cert_path = _secret_file(
                context.tls_client_certificate.reveal(),
                suffix="-client.pem",
            )
            temporary_paths.append(cert_path)
            kwargs["client_cert"] = str(cert_path)
    try:
        client = clickhouse_connect.get_client(**kwargs)
    except Exception:
        for path in temporary_paths:
            path.unlink(missing_ok=True)
        raise
    return ClientLease(client, temporary_paths)


def _decode_secret(value: bytes, name: str) -> str:
    try:
        decoded = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigurationError(
            ErrorCode.CONFIG_SECRET_REFERENCE,
            f"ClickHouse {name} must resolve to UTF-8 bytes",
        ) from exc
    if not decoded or "\x00" in decoded:
        raise ConfigurationError(
            ErrorCode.CONFIG_SECRET_REFERENCE,
            f"ClickHouse {name} is invalid",
        )
    return decoded


def _secret_file(value: bytes, *, suffix: str) -> Path:
    descriptor, raw_path = tempfile.mkstemp(prefix="meridian-clickhouse-", suffix=suffix)
    path = Path(raw_path)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(value)
    except Exception:
        with contextlib.suppress(OSError):
            os.close(descriptor)
        path.unlink(missing_ok=True)
        raise
    return path


__all__ = ["ClickHouseClient", "ClientLease", "QueryResult", "connect"]
