# SPDX-License-Identifier: Apache-2.0
"""Versioned ClickHouse Adapter capability declarations."""

from __future__ import annotations

from meridian_storage.query import QueryCapabilities
from meridian_storage.semantics import JsonValue
from meridian_storage.spi import AdapterDescriptor, CapabilityManifest, OperationCapability

from .._canonical import fingerprint
from ..configuration import (
    ADAPTER_CONTRACT_VERSION,
    ADAPTER_ID,
    TESTED_ENGINE_VERSIONS,
    ClickHouseSettings,
)
from ..schema import Topology

DRIVER = "clickhouse-connect/0.15"


def query_capabilities(settings: ClickHouseSettings) -> QueryCapabilities:
    return QueryCapabilities(
        adapter_id=ADAPTER_ID,
        operations=("aggregate", "get", "scan"),
        native_semantics=(
            "query.operation.aggregate",
            "query.operation.get",
            "query.operation.scan",
            "query.operator.aggregate.avg",
            "query.operator.aggregate.count",
            "query.operator.aggregate.max",
            "query.operator.aggregate.min",
            "query.operator.aggregate.percentile",
            "query.operator.aggregate.sum",
            "query.operator.and",
            "query.operator.eq",
            "query.operator.field",
            "query.operator.gt",
            "query.operator.gte",
            "query.operator.in",
            "query.operator.isNull",
            "query.operator.literal",
            "query.operator.lt",
            "query.operator.lte",
            "query.operator.ne",
            "query.operator.not",
            "query.operator.notIn",
            "query.operator.or",
            "query.operator.timestampRange",
            "query.pagination.keyset",
        ),
        operators=(
            "aggregate",
            "aggregate.avg",
            "aggregate.count",
            "aggregate.max",
            "aggregate.min",
            "aggregate.percentile",
            "aggregate.sum",
            "and",
            "eq",
            "field",
            "get",
            "gt",
            "gte",
            "in",
            "isNull",
            "literal",
            "lt",
            "lte",
            "ne",
            "not",
            "notIn",
            "or",
            "order",
            "scan",
            "timestampRange",
        ),
        logical_types=(
            "boolean",
            "bytes",
            "date",
            "decimal",
            "duration",
            "enum",
            "float64",
            "int16",
            "int32",
            "int64",
            "int8",
            "json",
            "objectRef",
            "recordRef",
            "string",
            "utcTimestamp",
            "uuid",
            "wgs84Point",
        ),
        consistency_classes=("eventual",),
        guarantees=(
            "bounded-time-range",
            "deterministic-order",
            "parameterized-commands",
            "scope-isolation",
            "single-binding",
        ),
        features=(
            "grouping",
            "live-keyset",
            "live-keyset-cursor",
            "projection",
            "quantile-profile",
        ),
        limits={
            "deadlineMs": settings.operation_timeout_ms,
            "maxTimeRangeSeconds": settings.max_time_range_seconds,
            "membershipNames": 10_000,
            "pageSize": 500,
            "resultBytes": settings.max_result_bytes,
            "resultValues": 500,
        },
    )


def adapter_descriptor(settings: ClickHouseSettings, engine_version: str) -> AdapterDescriptor:
    # engine_version is retained for API compatibility; selection is manifest provenance.
    limits = {
        "maxBatchBytes": settings.max_batch_bytes,
        "maxBatchRows": settings.max_batch_rows,
        "maxTimeRangeSeconds": settings.max_time_range_seconds,
        "pageSize": 500,
        "retryWindowSeconds": settings.retry_window_seconds,
    }
    query_extension = query_capabilities(settings).to_dict()
    common = {
        "cursor_behavior": "signed-live-keyset",
        "migration_behavior": "external-iac-job",
        "health_probes": ("authenticated", "functions", "schema", "timezone", "topology"),
    }
    evidence_layouts = tuple(
        layout for layout in settings.layouts.values() if layout.resource.catalog == "evidence"
    )
    append_guarantees: tuple[str, ...] = (
        "eventual-visibility",
        "retry-window-dedup",
        "scope-isolation",
    )
    # A Binding-wide promise must hold for every eligible Resource, including mixed layouts.
    if evidence_layouts and all(layout.append_only for layout in evidence_layouts):
        append_guarantees = ("append-only", *append_guarantees)
    capabilities = (
        OperationCapability(
            "meridian.evidence.append",
            ("1.0.0",),
            guarantees=append_guarantees,
            limits=limits,
            extensions={
                "recordProfiles": [
                    "analytical",
                    "cost",
                    "log",
                    "metric",
                    "span",
                    "time-series",
                    "usage",
                ],
            },
            **common,
        ),
        OperationCapability(
            "meridian.evidence.query",
            ("1.0.0",),
            guarantees=("bounded-time-range", "scope-isolation", "single-binding"),
            limits=limits,
            extensions={"queryCapabilities": query_extension},
            **common,
        ),
        OperationCapability(
            "meridian.structured.aggregate",
            ("1.0.0",),
            guarantees=("bounded-time-range", "scope-isolation", "single-binding"),
            limits=limits,
            extensions={"queryCapabilities": query_extension},
            **common,
        ),
        OperationCapability(
            "meridian.structured.get",
            ("1.0.0",),
            guarantees=("eventual-visibility", "scope-isolation", "single-binding"),
            limits=limits,
            extensions={"queryCapabilities": query_extension},
            **common,
        ),
        OperationCapability(
            "meridian.structured.put",
            ("1.0.0",),
            guarantees=("eventual-visibility", "retry-window-dedup", "scope-isolation"),
            limits=limits,
            extensions={"writeModel": "append-version"},
            **common,
        ),
        OperationCapability(
            "meridian.structured.query",
            ("1.0.0",),
            guarantees=("bounded-time-range", "scope-isolation", "single-binding"),
            limits=limits,
            extensions={"queryCapabilities": query_extension},
            **common,
        ),
    )
    return AdapterDescriptor(
        adapter_id=ADAPTER_ID,
        adapter_contract_version=ADAPTER_CONTRACT_VERSION,
        driver=DRIVER,
        supported_engine_versions={settings.topology.value: TESTED_ENGINE_VERSIONS},
        capabilities=capabilities,
    )


def capability_manifest(
    settings: ClickHouseSettings,
    engine_version: str,
) -> CapabilityManifest:
    """Return the deterministic authenticated manifest pinned by a Binding."""

    layout_set: JsonValue = [
        [canonical, layout.layout_fingerprint] for canonical, layout in settings.layouts.items()
    ]
    return CapabilityManifest(
        descriptor=adapter_descriptor(settings, engine_version),
        engine_profile=settings.topology.value,
        engine_version=engine_version,
        extensions={
            "layoutSetFingerprint": fingerprint(layout_set),
            "migrationAuthority": "external-iac-job",
            "queryCapabilityFingerprint": query_capabilities(settings).fingerprint,
        },
    )


def expected_engine_profile(replicated: bool) -> str:
    return Topology.REPLICATED.value if replicated else Topology.STANDALONE.value


__all__ = [
    "DRIVER",
    "adapter_descriptor",
    "capability_manifest",
    "expected_engine_profile",
    "query_capabilities",
]
