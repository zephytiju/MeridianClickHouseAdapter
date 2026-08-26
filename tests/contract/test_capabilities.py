# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from importlib import resources

import jsonschema
from meridian_storage.query import query_capability_contract
from meridian_storage.spi import CapabilityManifest, adapter_capability_contract

from meridian_storage.adapters.clickhouse import ClickHouseSettings, adapter_descriptor
from meridian_storage.adapters.clickhouse.ingestion import BatchReceipt
from tests.conftest import build_binding


def test_adapter_and_query_capability_documents_validate(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    descriptor = adapter_descriptor(settings, "25.3")
    manifest = CapabilityManifest(descriptor, layout.topology.value, "25.3")
    manifest_document = manifest.to_dict()
    jsonschema.validate(manifest_document, adapter_capability_contract())

    query_document = next(
        item
        for item in manifest_document["descriptor"]["capabilities"]
        if item["operationContract"] == "meridian.evidence.query"
    )
    query_capabilities = query_document["extensions"]["queryCapabilities"]
    jsonschema.validate(query_capabilities, query_capability_contract())


def test_descriptor_has_no_catalog_or_engine_boundary_leak(layout) -> None:  # type: ignore[no-untyped-def]
    settings = ClickHouseSettings.from_binding(build_binding(layout))
    document = adapter_descriptor(settings, "25.3").to_dict()
    encoded = json.dumps(document, sort_keys=True)
    contracts = {
        item["operationContract"] for item in document["capabilities"] if isinstance(item, dict)
    }

    assert all(
        value.startswith(("meridian.evidence.", "meridian.structured.")) for value in contracts
    )
    assert not any(
        token in encoded
        for token in (
            "meridian.telemetry.",
            "meridian.audit.",
            "meridian.lineage.",
            "meridian.usage.",
            "nativeQuery",
        )
    )


def test_packaged_conformance_vectors_are_json() -> None:
    root = resources.files("meridian_storage.adapters.clickhouse").joinpath(
        "contracts", "conformance"
    )
    documents = [json.loads(item.read_text(encoding="utf-8")) for item in root.iterdir()]
    assert documents
    assert all(
        document["formatVersion"] == "meridian.clickhouse.conformance.v1" for document in documents
    )


def test_packaged_adapter_schemas_validate_runtime_documents(layout) -> None:  # type: ignore[no-untyped-def]
    root = resources.files("meridian_storage.adapters.clickhouse").joinpath("contracts")
    layout_contract = json.loads(
        root.joinpath("meridian-clickhouse-layout.v1.schema.json").read_text(encoding="utf-8")
    )
    receipt_contract = json.loads(
        root.joinpath("meridian-clickhouse-batch-result.v1.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.Draft202012Validator.check_schema(layout_contract)
    jsonschema.Draft202012Validator.check_schema(receipt_contract)
    jsonschema.validate(layout.to_dict(), layout_contract)
    jsonschema.validate(BatchReceipt("mb1_" + "a" * 64, 2).to_dict(), receipt_contract)
