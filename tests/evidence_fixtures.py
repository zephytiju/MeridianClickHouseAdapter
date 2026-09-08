# SPDX-License-Identifier: Apache-2.0
"""Public Catalog + registered Schema + real Core startup; no capability overrides."""

from dataclasses import replace

import pytest
from meridian_storage.errors import CompatibilityError, MeridianError
from meridian_storage.evidence import EvidenceCatalogProvider
from meridian_storage.registry import (
    CapabilityRequirement,
    NamespaceDefinition,
    ResourceBundle,
    ResourceDefinition,
    SchemaDefinition,
)
from meridian_storage.runtime.config import RuntimeConfig
from meridian_storage.spi import PhysicalResource, SecretValue

from meridian_storage import Meridian, OperationContext, ResourceRef
from meridian_storage.adapters.clickhouse import (
    ClickHouseAdapterFactory,
    ClickHouseMigrator,
    ClickHouseSchemaCompiler,
    ClickHouseSettings,
    plan_initial_migration,
)
from tests.conftest import build_create_context, build_schema, sample_record


def core_fixture(clients, endpoints, topology):
    logical = build_schema()
    logical = replace(logical, ref=replace(logical.ref, name="public_append"))
    registered = SchemaDefinition(logical.ref.to_core(), logical.to_dict())
    resource = ResourceDefinition(
        ResourceRef("evidence", "observability", "public_append"),
        "metric",
        registered.ref,
        requirements=tuple(
            CapabilityRequirement(f"meridian.evidence.{method}", "1.0.0")
            for method in ("append", "query")
        ),
    )
    bundle = ResourceBundle(
        "clickhouse.conformance",
        "1.0.0",
        "1.0.0",
        namespaces=(NamespaceDefinition("evidence", "observability"),),
        schemas=(registered,),
        resources=(resource,),
    )

    class Provider:
        provider_id = bundle.provider_id
        provider_contract_version = bundle.provider_contract_version

        def load(self):
            return bundle

    class Secrets:
        def resolve(self, reference):
            return SecretValue(
                b"meridian" if reference.reference == "username" else b"meridian-test"
            )

    compilation = ClickHouseSchemaCompiler().compile(
        database="meridian_adapter_test",
        resource=resource.ref,
        resource_fingerprint=resource.fingerprint,
        schema=logical,
        record_profile="metric",
        topology=topology,
    )
    compilation = replace(
        compilation, layout=replace(compilation.layout, schema_fingerprint=registered.fingerprint)
    )
    layout = compilation.layout
    configs = []
    for client, endpoint in zip(clients, endpoints, strict=True):
        context = build_create_context(
            layout, endpoint=endpoint, username="meridian", password="meridian-test"
        )
        settings = ClickHouseSettings.from_binding(context.binding)
        ClickHouseMigrator(client, settings).apply(
            plan_initial_migration("public-append", (compilation,))
        )
    for endpoint in endpoints:
        context = build_create_context(
            layout, endpoint=endpoint, username="meridian", password="meridian-test"
        )
        adapter = ClickHouseAdapterFactory().create(context)
        adapter.open()
        try:
            physical = adapter.verify_physical(
                (
                    PhysicalResource(
                        resource.ref, resource.fingerprint, registered.fingerprint, "metric"
                    ),
                )
            )
        finally:
            adapter.close()
        binding = replace(context.binding, required_physical_fingerprint=physical.fingerprint)
        catalog = EvidenceCatalogProvider().manifest()
        configs.append(
            RuntimeConfig.from_mapping(
                {
                    "formatVersion": "meridian-config.v1",
                    "profile": "public-append",
                    "catalogs": {
                        "providers": [
                            {
                                "name": "evidence",
                                "package": catalog.package_name,
                                "contract": catalog.catalog_contract_version,
                                "requiredFingerprint": catalog.fingerprint,
                            }
                        ],
                        "extensions": {},
                    },
                    "schemas": {
                        "providers": [
                            {
                                "id": bundle.provider_id,
                                "package": "clickhouse-conformance-schemas",
                                "contract": "1.0.0",
                                "requiredFingerprint": bundle.fingerprint,
                            }
                        ],
                        "live": {"enabled": False, "required": False, "providerId": None},
                        "extensions": {},
                    },
                    "resources": {
                        "pins": [
                            {
                                "providerId": bundle.provider_id,
                                "ref": resource.ref.to_dict(),
                                "requiredFingerprint": resource.fingerprint,
                            }
                        ],
                        "extensions": {},
                    },
                    "bindings": [binding.to_dict()],
                    "placements": [
                        {
                            "id": "public-append",
                            "bindingId": binding.id,
                            "selector": {
                                "resources": [resource.ref.to_dict()],
                                "catalog": None,
                                "labels": {},
                            },
                            "extensions": {},
                        }
                    ],
                    "validation": {
                        "strict": True,
                        "requirePhysicalFingerprints": True,
                        "defaultOperationTimeoutMs": 30000,
                        "idempotencyCacheEntries": 64,
                        "retry": {
                            "maxAttempts": 1,
                            "baseDelayMs": 1,
                            "maxDelayMs": 1,
                            "jitterRatio": 0,
                        },
                    },
                    "telemetry": {
                        "enabled": False,
                        "serviceName": None,
                        "attributes": {},
                        "suppressExporterRecursion": True,
                        "extensions": {},
                    },
                    "extensions": {},
                }
            )
        )

    def runtime(index=0):
        result = Meridian.from_config(
            configs[index], schema_providers=[Provider()], secret_resolver=Secrets()
        )
        result.start()
        return result

    return runtime, layout


def append_and_assert(make_runtime, layout, *, seed):
    runtime = make_runtime()
    context = OperationContext(
        tenant="tenant-public", principal_ref="conformance", scope={"suite": "public-append"}
    )
    records = [
        {**sample_record(series_id="same-identity", value=float(i)), "observed_at": stamp}
        for i, stamp in enumerate(
            (
                "2026-08-25T00:00:00.000000001Z",
                "2026-08-25T00:00:00.000000001Z",
                "2026-08-25T00:00:00.000000002Z",
            )
        )
    ]
    try:
        with runtime.context(context):
            surface = runtime.catalog("evidence")
            if seed:
                for index, record in enumerate(records):
                    with runtime.context(replace(context, idempotency_key=f"append-{index}")):
                        result = runtime.execute(
                            surface.append(resource=layout.resource, data=record)
                        )
                        assert result.data["acceptedRows"] == 1
                # A fresh Core runtime below ensures this is an engine retry, not Core's cache.
                with pytest.raises(CompatibilityError):
                    runtime.execute(
                        surface.append(
                            resource=layout.resource, data=records[0], require_atomic=True
                        )
                    )
                with pytest.raises(MeridianError):
                    runtime.execute(
                        surface.append(resource=layout.resource, data=[records[0]] * 101)
                    )
                with pytest.raises(MeridianError):
                    runtime.execute(surface.query(resource=layout.resource))
            else:
                with runtime.context(replace(context, idempotency_key="append-0")):
                    assert (
                        runtime.execute(
                            surface.append(resource=layout.resource, data=records[0])
                        ).data["acceptedRows"]
                        == 1
                    )
            query = {
                "resource": layout.resource,
                "where": {
                    "observed_at": {"gte": "2026-08-25T00:00:00Z", "lt": "2026-08-26T00:00:00Z"}
                },
                "limit": 1,
            }
            cursor = None
            found = []
            for _ in range(5):
                page = runtime.execute(surface.query(**query, cursor=cursor)).data
                found.extend(page["items"])
                cursor = page["cursor"]
                if cursor is None:
                    break
            assert cursor is None
            assert sorted((row["value"], row["observed_at"]) for row in found) == sorted(
                (row["value"], row["observed_at"]) for row in records
            )
            with runtime.context(replace(context, tenant="other")):
                assert runtime.execute(surface.query(**query)).data["items"] == ()
    finally:
        runtime.close()
