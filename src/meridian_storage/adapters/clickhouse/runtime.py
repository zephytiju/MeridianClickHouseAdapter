# SPDX-License-Identifier: Apache-2.0
"""Meridian Core Adapter SPI implementation for ClickHouse."""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import cast

from meridian_storage.errors import (
    CompatibilityError,
    ErrorCode,
    MeridianError,
    SafeCause,
    TransactionError,
    TransientError,
    UnavailableError,
    ValidationError,
)
from meridian_storage.query import (
    CursorSigner,
    QueryOperation,
    TranslationContext,
    enforce_result_budget,
)
from meridian_storage.semantics import JsonValue, canonical_json_bytes
from meridian_storage.spi import (
    AdapterCreateContext,
    AdapterProbe,
    ExecutionRequest,
    ExecutionResult,
    PhysicalResource,
    PhysicalVerification,
)

from ._canonical import scope_fingerprint
from .client import ClickHouseClient, ClientLease, connect
from .configuration import ADAPTER_ID, ClickHouseSettings
from .ingestion import BatchExecutor, prepare_batch
from .probe import probe_adapter, verify_physical
from .query import ClickHouseQueryTranslator, compile_simple_query

ClientConnector = Callable[[AdapterCreateContext, ClickHouseSettings], ClientLease]


class ClickHouseAdapterFactory:
    """Discoverable Core 1.0.0 Adapter factory."""

    def __init__(self, client_connector: ClientConnector = connect) -> None:
        self._client_connector = client_connector

    @property
    def adapter_id(self) -> str:
        return ADAPTER_ID

    def create(self, context: AdapterCreateContext) -> ClickHouseAdapterRuntime:
        return ClickHouseAdapterRuntime(context, self._client_connector)


class ClickHouseAdapterRuntime:
    def __init__(self, context: AdapterCreateContext, connector: ClientConnector) -> None:
        self._context = context
        self._settings = ClickHouseSettings.from_binding(context.binding)
        self._connector = connector
        self._lease: ClientLease | None = None
        self._probe: AdapterProbe | None = None
        self._lock = threading.RLock()
        key = hashlib.sha256(
            b"meridian-storage-clickhouse/cursor/v1\x00" + context.credential.reveal()
        ).digest()
        key_id = hashlib.sha256(key).hexdigest()[:16]
        self._cursor_signer = CursorSigner(
            {key_id: key},
            active_key_id=key_id,
            ttl_seconds=self._settings.cursor_ttl_seconds,
        )

    def open(self) -> None:
        with self._lock:
            if self._lease is not None:
                return
            try:
                self._lease = self._connector(self._context, self._settings)
                probe = self._probe_now()
                required = self._context.binding.required_capability_fingerprint
                if probe.manifest.fingerprint != required:
                    raise CompatibilityError(
                        ErrorCode.CAPABILITY_FINGERPRINT,
                        "authenticated ClickHouse capability fingerprint differs "
                        "from the Binding pin",
                    )
                self._probe = probe
            except MeridianError:
                self._close_lease()
                raise
            except Exception as exc:
                self._close_lease()
                raise UnavailableError(
                    ErrorCode.ADAPTER_FAILURE,
                    "ClickHouse authenticated startup probe failed",
                    retryable=True,
                    cause=SafeCause.from_exception(exc),
                ) from exc

    def probe(self) -> AdapterProbe:
        with self._lock:
            self._require_open()
            if self._probe is None:
                self._probe = self._probe_now()
            return self._probe

    def verify_physical(
        self,
        resources: tuple[PhysicalResource, ...],
    ) -> PhysicalVerification:
        with self._lock:
            client = self._client()
            try:
                verification = verify_physical(client, self._settings, resources)
                required = self._context.binding.required_physical_fingerprint
                if required is not None and verification.fingerprint != required:
                    raise CompatibilityError(
                        ErrorCode.PHYSICAL_FINGERPRINT,
                        "ClickHouse physical fingerprint differs from the Binding pin",
                    )
                return verification
            except MeridianError:
                raise
            except Exception as exc:
                raise UnavailableError(
                    ErrorCode.ADAPTER_FAILURE,
                    "ClickHouse physical verification failed",
                    retryable=True,
                    cause=SafeCause.from_exception(exc),
                ) from exc

    def open_session(self, *, transactional: bool) -> ClickHouseAdapterSession:
        self._require_open()
        if transactional:
            raise TransactionError(
                ErrorCode.TRANSACTION_SCOPE,
                "ClickHouse Adapter does not advertise Meridian transaction Operations",
            )
        return ClickHouseAdapterSession(
            self._client(),
            self._settings,
            self._cursor_signer,
            self._context.binding.id,
        )

    def close(self) -> None:
        with self._lock:
            self._probe = None
            self._close_lease()

    def _probe_now(self) -> AdapterProbe:
        return probe_adapter(
            self._client(),
            self._settings,
            selected_engine_version=self._context.binding.engine_version,
        )

    def _client(self) -> ClickHouseClient:
        self._require_open()
        assert self._lease is not None
        return self._lease.client

    def _require_open(self) -> None:
        if self._lease is None:
            raise UnavailableError(ErrorCode.RUNTIME_STATE, "ClickHouse Adapter is not open")

    def _close_lease(self) -> None:
        lease, self._lease = self._lease, None
        if lease is not None:
            lease.close()


class ClickHouseAdapterSession:
    def __init__(
        self,
        client: ClickHouseClient,
        settings: ClickHouseSettings,
        cursor_signer: CursorSigner,
        binding_id: str,
    ) -> None:
        self._client = client
        self._settings = settings
        self._cursor_signer = cursor_signer
        self._binding_id = binding_id
        self._translator = ClickHouseQueryTranslator(settings, cursor_signer)
        self._batch_executor = BatchExecutor(client, settings)
        self._closed = False

    def begin(self) -> None:
        self._reject_transaction()

    def execute(self, request: ExecutionRequest) -> ExecutionResult:
        self._require_open()
        try:
            return self._execute(request)
        except MeridianError:
            raise
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                ErrorCode.OPERATION_INVALID,
                "ClickHouse rejected an invalid mapping-first Operation",
                operation_contract=request.operation.operation_contract,
                resource_ref=str(request.operation.resources[0]),
                request_id=request.request_id,
                execution_id=request.execution_id,
                cause=SafeCause.from_exception(exc),
            ) from exc
        except Exception as exc:
            raise TransientError(
                ErrorCode.ADAPTER_FAILURE,
                "ClickHouse Operation execution failed",
                operation_contract=request.operation.operation_contract,
                resource_ref=str(request.operation.resources[0]),
                request_id=request.request_id,
                execution_id=request.execution_id,
                cause=SafeCause.from_exception(exc),
            ) from exc

    def commit(self) -> None:
        self._reject_transaction()

    def rollback(self) -> None:
        self._reject_transaction()

    def close(self) -> None:
        self._closed = True

    def _execute(self, request: ExecutionRequest) -> ExecutionResult:
        if request.binding_id != self._binding_id:
            raise ValidationError(
                ErrorCode.OPERATION_SCOPE,
                "Execution request Binding identity differs from the open Adapter session",
            )
        operation = request.operation
        if len(operation.resources) != 1:
            raise ValueError("ClickHouse V1 Operations target exactly one Resource")
        _timeout_seconds(request, self._settings)
        layout = self._settings.layout_for(operation.resources[0])
        contract = operation.operation_contract
        capability = self._translator_capability(contract)
        if operation.operation_version not in capability.operation_versions:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "ClickHouse does not advertise the requested Operation version",
            )
        if contract in {"meridian.evidence.append", "meridian.structured.put"}:
            records = _write_records(contract, operation.input)
            batch = prepare_batch(request, layout, records, self._settings)
            receipt = self._batch_executor.execute(batch)
            data = cast(JsonValue, receipt.to_dict())
            return ExecutionResult(
                data,
                len(canonical_json_bytes(data)),
                {
                    "deduplication": "retry-window",
                    "layoutFingerprint": layout.layout_fingerprint,
                    "visibility": "eventual",
                },
            )
        if contract not in {
            "meridian.evidence.query",
            "meridian.structured.aggregate",
            "meridian.structured.get",
            "meridian.structured.query",
        }:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "ClickHouse does not advertise the requested Operation contract",
            )
        translation = self._translation_context(request, layout.schema_fingerprint)
        query_plan = operation.input.get("queryPlan")
        if query_plan is not None:
            if not isinstance(query_plan, Mapping):
                raise TypeError("queryPlan must be the Query 1.0.0 mapping")
            logical = QueryOperation.from_mapping(cast(Mapping[str, object], query_plan))
            if logical.catalog != operation.catalog or logical.resources != operation.resources:
                raise ValueError("queryPlan Catalog or Resources differ from the Core Operation")
            compiled = self._translator.compile_wire(logical, translation)
            budget = logical.budget
        else:
            method = contract.rsplit(".", 1)[-1]
            if "metrics" in operation.input:
                method = "aggregate"
            compiled = compile_simple_query(
                method,
                operation.input,
                layout,
                translation,
                self._settings,
                self._cursor_signer,
            )
            budget = None
        command = cast(Mapping[str, JsonValue], compiled.command)
        sql = command.get("sql")
        if not isinstance(sql, str):
            raise TypeError("compiled ClickHouse command is invalid")
        timeout = _timeout_seconds(request, self._settings)
        result = self._client.query(
            sql,
            dict(compiled.parameters),
            {
                "max_execution_time": timeout,
                "max_result_rows": 501,
                "result_overflow_mode": "throw",
            },
        )
        normalized = self._translator.normalize_result(compiled, result)
        if budget is not None:
            normalized = enforce_result_budget(normalized, budget)
        data = normalized.operation_data()
        encoded = len(canonical_json_bytes(data))
        if encoded > self._settings.max_result_bytes:
            raise ValidationError(
                ErrorCode.OPERATION_RESULT_LIMIT,
                "ClickHouse normalized result exceeds the Binding result limit",
            )
        return ExecutionResult(
            data,
            encoded,
            {
                **dict(normalized.provenance),
                "layoutFingerprint": layout.layout_fingerprint,
                "visibility": "eventual",
            },
        )

    def _translation_context(
        self,
        request: ExecutionRequest,
        schema_fingerprint: str,
    ) -> TranslationContext:
        return TranslationContext(
            binding_id=request.binding_id,
            plan_fingerprint=request.operation.request_fingerprint,
            registry_fingerprint=request.registry_fingerprint,
            schema_fingerprints={
                request.operation.resources[0].canonical: schema_fingerprint,
            },
            scope_fingerprint=scope_fingerprint(request.context),
            deadline_ms=max(1, int(_timeout_seconds(request, self._settings) * 1000)),
        )

    def _translator_capability(self, contract: str):  # type: ignore[no-untyped-def]
        from .descriptor import adapter_descriptor

        descriptor = adapter_descriptor(self._settings, "runtime")
        capability = descriptor.capability_for(contract)
        if capability is None:
            raise CompatibilityError(
                ErrorCode.CAPABILITY_UNSUPPORTED,
                "ClickHouse does not advertise the requested Operation contract",
            )
        return capability

    def _reject_transaction(self) -> None:
        self._require_open()
        raise TransactionError(
            ErrorCode.TRANSACTION_STATE,
            "ClickHouse Adapter sessions are non-transactional",
        )

    def _require_open(self) -> None:
        if self._closed:
            raise UnavailableError(ErrorCode.RUNTIME_CLOSED, "ClickHouse Adapter session is closed")


def _write_records(
    contract: str,
    input_value: Mapping[str, JsonValue],
) -> object:
    if contract == "meridian.structured.put":
        if input_value.get("expectedVersion") is not None:
            raise ValueError("ClickHouse V1 put does not advertise conditional versions")
        if "data" not in input_value:
            raise ValueError("structured.put requires data")
        return input_value["data"]
    for name in ("records", "data"):
        if name in input_value:
            return input_value[name]
    raise ValueError("evidence.append requires records")


def _timeout_seconds(request: ExecutionRequest, settings: ClickHouseSettings) -> float:
    configured = settings.operation_timeout_ms / 1000
    remaining = request.context.remaining_seconds(now=datetime.now(UTC))
    if remaining is not None:
        if remaining <= 0:
            raise ValidationError(ErrorCode.DEADLINE_EXCEEDED, "Operation deadline has elapsed")
        configured = min(configured, remaining)
    return max(0.001, configured)


__all__ = [
    "ClickHouseAdapterFactory",
    "ClickHouseAdapterRuntime",
    "ClickHouseAdapterSession",
]
