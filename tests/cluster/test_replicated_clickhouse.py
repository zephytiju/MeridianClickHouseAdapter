# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import clickhouse_connect
import pytest
from meridian_storage.spi import PhysicalResource
from meridian_storage.testing import AdapterConformanceTarget, run_adapter_conformance

from meridian_storage import OperationContext
from meridian_storage.adapters.clickhouse import (
    ClickHouseAdapterFactory,
    ClickHouseMigrator,
    ClickHouseSchemaCompiler,
    ClickHouseSettings,
    Topology,
    plan_initial_migration,
)
from tests.conftest import (
    RESOURCE_FINGERPRINT,
    build_create_context,
    build_layout,
    build_request,
    build_schema,
    sample_record,
)

pytestmark = pytest.mark.cluster


@pytest.fixture(scope="module")
def replicated_engine():  # type: ignore[no-untyped-def]
    if os.environ.get("CLICKHOUSE_CLUSTER") != "1":
        pytest.skip("set CLICKHOUSE_CLUSTER=1 for the real replicated Engine suite")
    clients = (_wait_client(18123), _wait_client(18124))
    for client in clients:
        client.command("DROP DATABASE IF EXISTS meridian_adapter_test SYNC")
        client.command("CREATE DATABASE meridian_adapter_test")
    layout = build_layout(topology=Topology.REPLICATED)
    compilation = ClickHouseSchemaCompiler().compile(
        database="meridian_adapter_test",
        resource=layout.resource,
        resource_fingerprint=RESOURCE_FINGERPRINT,
        schema=build_schema(),
        record_profile="metric",
        topology=Topology.REPLICATED,
    )
    for port, client in zip((18123, 18124), clients, strict=True):
        endpoint = f"http://127.0.0.1:{port}"
        settings = ClickHouseSettings.from_binding(
            build_create_context(
                compilation.layout,
                endpoint=endpoint,
                username="meridian",
                password="meridian-test",
            ).binding
        )
        ClickHouseMigrator(client, settings).apply(
            plan_initial_migration("replicated-v1", (compilation,))
        )
    try:
        yield clients, compilation.layout
    finally:
        for client in clients:
            client.command("DROP DATABASE IF EXISTS meridian_adapter_test SYNC")
            client.close()


def test_real_replicated_core_conformance(replicated_engine) -> None:  # type: ignore[no-untyped-def]
    _clients, layout = replicated_engine
    context = build_create_context(
        layout,
        endpoint="http://127.0.0.1:18123",
        username="meridian",
        password="meridian-test",
    )

    def assert_result(result):  # type: ignore[no-untyped-def]
        assert result.data["acceptedRows"] == 1

    report = run_adapter_conformance(
        AdapterConformanceTarget(
            ClickHouseAdapterFactory(),
            context,
            (
                PhysicalResource(
                    layout.resource,
                    layout.resource_fingerprint,
                    layout.schema_fingerprint,
                    layout.record_profile.value,
                ),
            ),
            build_request(
                layout,
                records=[sample_record(series_id="replicated-conformance")],
                request_id="replicated-conformance",
            ).operation,
            OperationContext(
                "principal:conformance",
                request_id="replicated-conformance",
                tenant="tenant-cluster",
                scope={"suite": "replicated"},
                idempotency_key="replicated-conformance",
            ),
            assert_result,
        )
    )
    assert report.engine_profile == "clickhouse-replicated"
    _write_evidence(report.to_dict())


def test_record_replicates_to_second_server(replicated_engine) -> None:  # type: ignore[no-untyped-def]
    clients, layout = replicated_engine
    context = build_create_context(
        layout,
        endpoint="http://127.0.0.1:18123",
        username="meridian",
        password="meridian-test",
    )
    runtime = ClickHouseAdapterFactory().create(context)
    runtime.open()
    session = runtime.open_session(transactional=False)
    try:
        session.execute(
            build_request(
                layout,
                records=[sample_record(series_id="cross-replica")],
                scope={"suite": "cross-replica"},
                request_id="cross-replica",
            )
        )
    finally:
        session.close()
        runtime.close()
    deadline = time.monotonic() + 30
    count = 0
    table = layout.qualified_table("meridian_adapter_test")
    series_column = layout.physical_column("series_id")
    while time.monotonic() < deadline:
        result = clients[1].query(
            f"SELECT count() FROM {table} FINAL WHERE `{series_column}` = {{series:String}}",
            {"series": "cross-replica"},
        )
        count = int(result.result_rows[0][0])
        if count:
            break
        time.sleep(0.5)
    assert count == 1


def _wait_client(port: int):  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + 180
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


def _write_evidence(document: object) -> None:
    target = os.environ.get("MERIDIAN_EVIDENCE_PATH")
    if target is None:
        return
    path = Path(target)
    path.mkdir(parents=True, exist_ok=True)
    (path / "replicated.json").write_text(
        json.dumps(document, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
