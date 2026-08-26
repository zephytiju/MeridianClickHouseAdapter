# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import clickhouse_connect
import pytest
from meridian_storage.spi import ExecutionRequest, PhysicalResource
from meridian_storage.testing import AdapterConformanceTarget, run_adapter_conformance

from meridian_storage import Operation, OperationContext
from meridian_storage.adapters.clickhouse import (
    ClickHouseAdapterFactory,
    ClickHouseMigrator,
    ClickHouseSchemaCompiler,
    ClickHouseSettings,
    plan_initial_migration,
)
from tests.conftest import (
    REGISTRY_FINGERPRINT,
    RESOURCE_FINGERPRINT,
    build_create_context,
    build_layout,
    build_request,
    build_schema,
    sample_record,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def real_engine():  # type: ignore[no-untyped-def]
    if os.environ.get("CLICKHOUSE_INTEGRATION") != "1":
        pytest.skip("set CLICKHOUSE_INTEGRATION=1 for the real standalone Engine suite")
    endpoint = os.environ.get("CLICKHOUSE_ENDPOINT", "http://127.0.0.1:18123")
    port = int(endpoint.rsplit(":", 1)[1])
    client = _wait_client(port)
    client.command("DROP DATABASE IF EXISTS meridian_adapter_test SYNC")
    client.command("CREATE DATABASE meridian_adapter_test")
    layout = build_layout()
    settings = ClickHouseSettings.from_binding(
        build_create_context(
            layout,
            endpoint=endpoint,
            username="meridian",
            password="meridian-test",
        ).binding
    )
    compilation = ClickHouseSchemaCompiler().compile(
        database=settings.database,
        resource=layout.resource,
        resource_fingerprint=RESOURCE_FINGERPRINT,
        schema=build_schema(),
        record_profile="metric",
        retention_seconds=30 * 24 * 60 * 60,
        administrative_profiles=("backup.daily", "partition-export.parquet"),
    )
    ClickHouseMigrator(client, settings).apply(
        plan_initial_migration("standalone-v1", (compilation,))
    )
    try:
        yield endpoint, client, compilation.layout
    finally:
        client.command("DROP DATABASE IF EXISTS meridian_adapter_test SYNC")
        client.close()


def test_real_engine_core_conformance(real_engine) -> None:  # type: ignore[no-untyped-def]
    endpoint, _client, layout = real_engine
    context = build_create_context(
        layout,
        endpoint=endpoint,
        username="meridian",
        password="meridian-test",
    )

    def assert_result(result):  # type: ignore[no-untyped-def]
        assert result.data["acceptedRows"] == 1
        assert result.data["visibility"] == "eventual"

    report = run_adapter_conformance(
        AdapterConformanceTarget(
            factory=ClickHouseAdapterFactory(),
            create_context=context,
            resources=(
                PhysicalResource(
                    layout.resource,
                    layout.resource_fingerprint,
                    layout.schema_fingerprint,
                    layout.record_profile.value,
                ),
            ),
            operation=build_request(layout, request_id="real-conformance").operation,
            context=OperationContext(
                "principal:conformance",
                request_id="real-conformance",
                tenant="tenant-conformance",
                scope={"suite": "standalone"},
                idempotency_key="real-conformance",
            ),
            assert_result=assert_result,
        )
    )
    assert report.engine_profile == "clickhouse-standalone"
    assert report.checks[-1] == "normalized-execution"
    _write_evidence("standalone", report.to_dict())


def test_real_engine_retry_scope_query_and_quantile(real_engine) -> None:  # type: ignore[no-untyped-def]
    endpoint, _client, layout = real_engine
    context = build_create_context(
        layout,
        endpoint=endpoint,
        username="meridian",
        password="meridian-test",
    )
    runtime = ClickHouseAdapterFactory().create(context)
    runtime.open()
    session = runtime.open_session(transactional=False)
    try:
        records = [
            sample_record(series_id=f"checkout-{index}", value=float(value))
            for index, value in enumerate((10, 20, 30, 40), start=1)
        ]
        request = build_request(
            layout,
            records=records,
            scope={"suite": "bounded-query"},
            request_id="real-batch",
        )
        first = session.execute(request)
        second = session.execute(request)
        assert first.data["batchId"] == second.data["batchId"]

        query = _query_request(layout, aggregate=False)
        queried = session.execute(query)
        assert len(queried.data["items"]) == 4

        quantile = session.execute(_query_request(layout, aggregate=True))
        assert quantile.data["items"][0]["p99"] == 40

        isolated = _query_request(layout, aggregate=False)
        object.__setattr__(
            isolated,
            "context",
            OperationContext(
                "principal:test",
                request_id="other-scope",
                tenant="tenant-a",
                scope={"suite": "another-scope"},
            ),
        )
        assert session.execute(isolated).data["items"] == ()
    finally:
        session.close()
        runtime.close()


def _query_request(layout, *, aggregate: bool) -> ExecutionRequest:  # type: ignore[no-untyped-def]
    common = {
        "resource": layout.resource.to_dict(),
        "where": {
            "observed_at": {
                "gte": "2026-08-25T00:00:00Z",
                "lt": "2026-08-26T00:00:00Z",
            }
        },
    }
    if aggregate:
        contract = "meridian.evidence.query"
        input_value = {
            **common,
            "groupBy": [],
            "metrics": [
                {
                    "name": "p99",
                    "function": "quantile",
                    "field": "value",
                    "quantile": 0.99,
                }
            ],
        }
    else:
        contract = "meridian.evidence.query"
        input_value = {**common, "select": [], "orderBy": [], "limit": 50}
    operation = Operation(
        "evidence",
        contract,
        "1.0.0",
        (layout.resource,),
        input_value,
        read_only=True,
        idempotent=True,
    )
    return ExecutionRequest(
        operation,
        OperationContext(
            "principal:test",
            request_id="query-aggregate" if aggregate else "query-records",
            tenant="tenant-a",
            scope={"suite": "bounded-query"},
        ),
        "query-aggregate" if aggregate else "query-records",
        "execution-query",
        "clickhouse-test",
        1,
        REGISTRY_FINGERPRINT,
        1,
    )


def _wait_client(port: int):  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + 120
    while True:
        try:
            return clickhouse_connect.get_client(
                host="127.0.0.1",
                port=port,
                username="meridian",
                password="meridian-test",
                database="default",
            )
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)


def _write_evidence(profile: str, document: object) -> None:
    target = os.environ.get("MERIDIAN_EVIDENCE_PATH")
    if target is None:
        return
    path = Path(target)
    path.mkdir(parents=True, exist_ok=True)
    (path / f"{profile}.json").write_text(
        json.dumps(document, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
