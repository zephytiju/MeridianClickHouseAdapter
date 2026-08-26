# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from meridian_storage.errors import (
    CompatibilityError,
    TransactionError,
    UnavailableError,
    ValidationError,
)
from meridian_storage.query import Field, Literal, QueryOperation, QueryTarget, TimestampRange
from meridian_storage.spi import (
    AdapterCreateContext,
    ExecutionRequest,
    PhysicalResource,
    SecretValue,
)
from meridian_storage.testing import AdapterConformanceTarget, run_adapter_conformance

from meridian_storage import Operation, OperationContext
from meridian_storage.adapters.clickhouse import ClickHouseAdapterFactory
from meridian_storage.adapters.clickhouse.client import ClientLease
from tests.conftest import (
    REGISTRY_FINGERPRINT,
    build_binding,
    build_layout,
    build_request,
    sample_record,
)
from tests.fakes import FakeClient, FakeResult


def _factory_context(layout, fake):  # type: ignore[no-untyped-def]
    context = AdapterCreateContext(
        build_binding(layout),
        SecretValue(b"default"),
        SecretValue(b"test-password"),
    )
    factory = ClickHouseAdapterFactory(lambda _context, _settings: ClientLease(fake))
    return factory, context


def test_core_adapter_conformance_runner(layout) -> None:  # type: ignore[no-untyped-def]
    fake = FakeClient(layout)
    factory, context = _factory_context(layout, fake)
    operation = build_request(layout).operation
    target = AdapterConformanceTarget(
        factory=factory,
        create_context=context,
        resources=(
            PhysicalResource(
                layout.resource,
                layout.resource_fingerprint,
                layout.schema_fingerprint,
                layout.record_profile.value,
            ),
        ),
        operation=operation,
        context=build_request(layout).context,
        assert_result=lambda result: result.data["acceptedRows"] == 1,
    )
    report = run_adapter_conformance(target)

    assert report.checks == (
        "authenticated-open",
        "deterministic-capability-manifest",
        "deterministic-physical-verification",
        "normalized-execution",
    )
    assert fake.closed
    assert len(fake.inserts) == 1


def test_runtime_rejects_capability_pin_mismatch(layout) -> None:  # type: ignore[no-untyped-def]
    fake = FakeClient(layout)
    binding = build_binding(layout)
    object.__setattr__(binding, "required_capability_fingerprint", "sha256:" + "9" * 64)
    context = AdapterCreateContext(binding, SecretValue(b"default"), SecretValue(b"password"))
    runtime = ClickHouseAdapterFactory(lambda _context, _settings: ClientLease(fake)).create(
        context
    )
    with pytest.raises(CompatibilityError):
        runtime.open()
    assert fake.closed


def test_nontransactional_session_lifecycle(layout) -> None:  # type: ignore[no-untyped-def]
    fake = FakeClient(layout)
    factory, context = _factory_context(layout, fake)
    runtime = factory.create(context)
    with pytest.raises(UnavailableError):
        runtime.probe()
    runtime.open()
    runtime.open()
    object.__setattr__(runtime, "_probe", None)
    assert runtime.probe().manifest.adapter_id == "meridian.storage.clickhouse"
    with pytest.raises(TransactionError):
        runtime.open_session(transactional=True)
    session = runtime.open_session(transactional=False)
    with pytest.raises(TransactionError):
        session.begin()
    with pytest.raises(TransactionError):
        session.commit()
    with pytest.raises(TransactionError):
        session.rollback()
    session.close()
    with pytest.raises(UnavailableError):
        session.execute(build_request(layout))
    runtime.close()
    runtime.close()


def test_runtime_normalizes_connector_and_physical_probe_failures(layout) -> None:  # type: ignore[no-untyped-def]
    context = AdapterCreateContext(
        build_binding(layout), SecretValue(b"default"), SecretValue(b"password")
    )

    def fail_connector(_context, _settings):  # type: ignore[no-untyped-def]
        raise RuntimeError("connection refused")

    runtime = ClickHouseAdapterFactory(fail_connector).create(context)
    with pytest.raises(UnavailableError, match="startup probe"):
        runtime.open()

    fake = FakeClient(layout)
    pinned = build_binding(layout, physical_fingerprint="sha256:" + "9" * 64)
    runtime = ClickHouseAdapterFactory(lambda _c, _s: ClientLease(fake)).create(
        AdapterCreateContext(pinned, SecretValue(b"default"), SecretValue(b"password"))
    )
    runtime.open()
    resource = PhysicalResource(
        layout.resource,
        layout.resource_fingerprint,
        layout.schema_fingerprint,
        layout.record_profile.value,
    )
    with pytest.raises(CompatibilityError, match="physical fingerprint"):
        runtime.verify_physical((resource,))
    runtime.close()


def test_mapping_first_query_executes_and_normalizes(layout) -> None:  # type: ignore[no-untyped-def]
    fake = FakeClient(layout)
    fake.ordinary_result = FakeResult(
        (
            "observed_at",
            "series_id",
            "service",
            "value",
            "__meridian_sort_0",
            "__meridian_sort_1",
            "__meridian_sort_2",
        ),
        (
            (
                datetime(2026, 8, 25, 12, tzinfo=UTC),
                "series-1",
                "checkout",
                42.5,
                datetime(2026, 8, 25, 12, tzinfo=UTC),
                "series-1",
                "a" * 64,
            ),
        ),
    )
    factory, context = _factory_context(layout, fake)
    runtime = factory.create(context)
    runtime.open()
    session = runtime.open_session(transactional=False)
    operation = Operation(
        catalog="evidence",
        operation_contract="meridian.evidence.query",
        operation_version="1.0.0",
        resources=(layout.resource,),
        input={
            "resource": layout.resource.to_dict(),
            "where": {
                "observed_at": {
                    "gte": "2026-08-25T00:00:00Z",
                    "lt": "2026-08-26T00:00:00Z",
                }
            },
            "select": ["observed_at", "series_id", "service", "value"],
            "orderBy": [],
            "limit": 10,
        },
        read_only=True,
        idempotent=True,
    )
    request = ExecutionRequest(
        operation,
        OperationContext(
            "principal:test",
            request_id="query-1",
            tenant="tenant-a",
            scope={"region": "us-west"},
        ),
        "query-1",
        "execution-query-1",
        "clickhouse-test",
        1,
        REGISTRY_FINGERPRINT,
        1,
    )
    result = session.execute(request)

    assert result.data["items"][0]["series_id"] == "series-1"
    assert result.data["cursor"] is None
    assert result.provenance["scopePredicate"] == "first"


def test_invalid_query_and_wrong_binding_are_typed(layout) -> None:  # type: ignore[no-untyped-def]
    fake = FakeClient(layout)
    factory, context = _factory_context(layout, fake)
    runtime = factory.create(context)
    runtime.open()
    session = runtime.open_session(transactional=False)
    append = build_request(layout)
    object.__setattr__(append, "binding_id", "another-binding")
    with pytest.raises(ValidationError):
        session.execute(append)

    query_operation = Operation(
        catalog="evidence",
        operation_contract="meridian.evidence.query",
        operation_version="1.0.0",
        resources=(layout.resource,),
        input={"resource": layout.resource.to_dict(), "where": {}, "limit": 50},
        read_only=True,
        idempotent=True,
    )
    invalid = ExecutionRequest(
        query_operation,
        build_request(layout).context,
        "invalid-query",
        "invalid-execution",
        "clickhouse-test",
        1,
        REGISTRY_FINGERPRINT,
        1,
    )
    with pytest.raises(ValidationError) as caught:
        session.execute(invalid)
    assert caught.value.code == "MERIDIAN_OPERATION_INVALID"

    expired = replace(
        build_request(layout),
        context=OperationContext(
            "principal:test",
            request_id="expired",
            tenant="tenant-a",
            scope={"region": "us-west"},
            deadline=datetime.now(UTC) - timedelta(seconds=1),
        ),
    )
    with pytest.raises(ValidationError) as deadline:
        session.execute(expired)
    assert deadline.value.code == "MERIDIAN_DEADLINE_EXCEEDED"


def test_released_wire_query_plan_executes_through_runtime(layout) -> None:  # type: ignore[no-untyped-def]
    fake = FakeClient(layout)
    factory, context = _factory_context(layout, fake)
    runtime = factory.create(context)
    runtime.open()
    session = runtime.open_session(transactional=False)
    logical = QueryOperation(
        catalog="evidence",
        targets=(QueryTarget(layout.resource),),
        operation="scan",
        filter=TimestampRange(
            Field("observed_at"),
            Literal("2026-08-25T00:00:00Z", "utcTimestamp"),
            Literal("2026-08-26T00:00:00Z", "utcTimestamp"),
        ),
        consistency="eventual",
    )
    request = ExecutionRequest(
        logical.to_core_operation(),
        build_request(layout).context,
        "wire-query",
        "wire-query-execution",
        "clickhouse-test",
        1,
        REGISTRY_FINGERPRINT,
        1,
    )
    result = session.execute(request)
    assert result.data == {"items": (), "cursor": None}
    session.close()
    runtime.close()


def test_operation_version_and_missing_write_data_are_rejected(layout) -> None:  # type: ignore[no-untyped-def]
    fake = FakeClient(layout)
    factory, context = _factory_context(layout, fake)
    runtime = factory.create(context)
    runtime.open()
    session = runtime.open_session(transactional=False)

    wrong_version = build_request(layout)
    object.__setattr__(wrong_version.operation, "operation_version", "2.0.0")
    with pytest.raises(CompatibilityError, match="Operation version"):
        session.execute(wrong_version)

    missing = Operation(
        "evidence",
        "meridian.evidence.append",
        "1.0.0",
        (layout.resource,),
        {},
        read_only=False,
        idempotent=True,
    )
    request = ExecutionRequest(
        missing,
        build_request(layout).context,
        "missing",
        "missing-execution",
        "clickhouse-test",
        1,
        REGISTRY_FINGERPRINT,
        1,
    )
    with pytest.raises(ValidationError, match="invalid mapping-first"):
        session.execute(request)
    session.close()
    runtime.close()


def test_structured_put_rejects_conditional_version(layout) -> None:  # type: ignore[no-untyped-def]
    structured = build_layout(catalog="structured")
    fake = FakeClient(structured)
    factory, context = _factory_context(structured, fake)
    runtime = factory.create(context)
    runtime.open()
    session = runtime.open_session(transactional=False)
    operation = Operation(
        "structured",
        "meridian.structured.put",
        "1.0.0",
        (structured.resource,),
        {"data": sample_record(), "expectedVersion": "v1"},
        read_only=False,
        idempotent=True,
    )
    request = ExecutionRequest(
        operation,
        build_request(structured).context,
        "put-1",
        "put-execution-1",
        "clickhouse-test",
        1,
        REGISTRY_FINGERPRINT,
        1,
    )
    with pytest.raises(ValidationError):
        session.execute(request)
