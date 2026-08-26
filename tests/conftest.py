# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Mapping

import pytest
from meridian_storage.runtime import BindingConfig
from meridian_storage.semantics import (
    PROFILE_EXTENSION_KEY,
    CatalogName,
    FieldDefinition,
    JsonValue,
    LogicalKind,
    LogicalType,
    SchemaDocument,
    SchemaReference,
    SemanticKind,
    TimeSeriesProfile,
)
from meridian_storage.spi import AdapterCreateContext, ExecutionRequest, SecretValue

from meridian_storage import Operation, OperationContext, ResourceRef
from meridian_storage.adapters.clickhouse import (
    ClickHouseSchemaCompiler,
    ClickHouseSettings,
    RecordProfile,
    ResourceLayout,
    Topology,
    capability_manifest,
)

RESOURCE_FINGERPRINT = "sha256:" + "1" * 64
REGISTRY_FINGERPRINT = "sha256:" + "2" * 64


def build_schema(*, catalog: str = "evidence") -> SchemaDocument:
    profile = TimeSeriesProfile(
        timestamp_field="observed_at",
        series_identity=("series_id",),
        dimensions=("service",),
        measurements=("value",),
    )
    return SchemaDocument(
        ref=SchemaReference(CatalogName(catalog), "observability", "metric_points", "1.0.0"),
        semantic_kind=SemanticKind.TIME_SERIES,
        fields=(
            FieldDefinition("observed_at", LogicalType(LogicalKind.UTC_TIMESTAMP)),
            FieldDefinition("series_id", LogicalType(LogicalKind.STRING)),
            FieldDefinition("service", LogicalType(LogicalKind.STRING)),
            FieldDefinition("value", LogicalType(LogicalKind.FLOAT64)),
        ),
        identity=("series_id",),
        consistency="eventual",
        extensions={PROFILE_EXTENSION_KEY: profile.to_dict()},
    )


def build_layout(
    *,
    database: str = "meridian_adapter_test",
    catalog: str = "evidence",
    topology: Topology = Topology.STANDALONE,
) -> ResourceLayout:
    resource = ResourceRef(catalog, "observability", "metric_points")
    return (
        ClickHouseSchemaCompiler()
        .compile(
            database=database,
            resource=resource,
            resource_fingerprint=RESOURCE_FINGERPRINT,
            schema=build_schema(catalog=catalog),
            record_profile=RecordProfile.METRIC,
            topology=topology,
            retention_seconds=30 * 24 * 60 * 60,
            administrative_profiles=("backup.daily", "partition-export.parquet"),
        )
        .layout
    )


def binding_mapping(
    layout: ResourceLayout,
    *,
    endpoint: str = "http://127.0.0.1:8123",
    engine_version: str = "25.3",
    required_capability_fingerprint: str = "sha256:" + "0" * 64,
    required_physical_fingerprint: str | None = None,
) -> dict[str, JsonValue]:
    return {
        "id": "clickhouse-test",
        "adapterId": "meridian.storage.clickhouse",
        "adapterContract": "1.0.0",
        "engineProfile": layout.topology.value,
        "engineVersion": engine_version,
        "endpoint": endpoint,
        "serviceRef": None,
        "physicalNamespace": "meridian_adapter_test",
        "tls": {
            "mode": "disabled",
            "serverName": None,
            "caRef": None,
            "clientCertificateRef": None,
        },
        "identityRef": {"provider": "test", "reference": "username"},
        "secretRef": {"provider": "test", "reference": "password"},
        "client": {
            "minSize": 0,
            "maxSize": 4,
            "acquireTimeoutMs": 5_000,
            "idleTimeoutMs": 60_000,
            "operationTimeoutMs": 30_000,
            "maxResultBytes": 4 * 1024 * 1024,
            "iteratorLifetimeMs": 30_000,
        },
        "requiredCapabilityFingerprint": required_capability_fingerprint,
        "requiredPhysicalFingerprint": required_physical_fingerprint,
        "compatibilityPins": {},
        "settings": {
            "layouts": [layout.to_dict()],
            "maxBatchRows": 100,
            "maxBatchBytes": 1024 * 1024,
            "maxTimeRangeSeconds": 86_400,
            "retryWindowSeconds": 86_400,
            "cursorTtlSeconds": 900,
            "insertQuorum": 1,
            "requiredFunctions": ["count", "quantile", "sum"],
        },
        "extensions": {},
    }


def build_binding(
    layout: ResourceLayout,
    *,
    endpoint: str = "http://127.0.0.1:8123",
    engine_version: str = "25.3",
    physical_fingerprint: str | None = None,
) -> BindingConfig:
    initial = BindingConfig.from_mapping(
        binding_mapping(layout, endpoint=endpoint, engine_version=engine_version),
        "bindings[0]",
    )
    settings = ClickHouseSettings.from_binding(initial)
    capability = capability_manifest(settings, engine_version).fingerprint
    return BindingConfig.from_mapping(
        binding_mapping(
            layout,
            endpoint=endpoint,
            engine_version=engine_version,
            required_capability_fingerprint=capability,
            required_physical_fingerprint=physical_fingerprint,
        ),
        "bindings[0]",
    )


def build_create_context(
    layout: ResourceLayout,
    *,
    endpoint: str = "http://127.0.0.1:8123",
    username: str = "default",
    password: str = "password",
) -> AdapterCreateContext:
    return AdapterCreateContext(
        build_binding(layout, endpoint=endpoint),
        SecretValue(username.encode()),
        SecretValue(password.encode()),
    )


def build_request(
    layout: ResourceLayout,
    *,
    records: object | None = None,
    scope: Mapping[str, str] | None = None,
    request_id: str = "request-1",
) -> ExecutionRequest:
    operation = Operation(
        catalog=layout.resource.catalog,
        operation_contract=f"meridian.{layout.resource.catalog}.append",
        operation_version="1.0.0",
        resources=(layout.resource,),
        input={"records": records or [sample_record()]},
        read_only=False,
        idempotent=True,
    )
    context = OperationContext(
        "principal:test",
        request_id=request_id,
        tenant="tenant-a",
        scope=scope or {"region": "us-west"},
        idempotency_key="batch-1",
    )
    return ExecutionRequest(
        operation=operation,
        context=context,
        request_id=request_id,
        execution_id="execution-1",
        binding_id="clickhouse-test",
        registry_revision=1,
        registry_fingerprint=REGISTRY_FINGERPRINT,
        attempt=1,
    )


def sample_record(
    *,
    series_id: str = "series-1",
    observed_at: str = "2026-08-25T12:00:00Z",
    value: float = 42.5,
) -> dict[str, JsonValue]:
    return {
        "observed_at": observed_at,
        "series_id": series_id,
        "service": "checkout",
        "value": value,
    }


@pytest.fixture
def layout() -> ResourceLayout:
    return build_layout()
